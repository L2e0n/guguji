# -*- coding: utf-8 -*-
"""A-share sector real-time fund flow service (Eastmoney board money-flow).

Provides industry / concept / region board rankings with main-force net inflow,
order-size breakdown, leading stock, and simple capital-flow signals.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

import requests

log = logging.getLogger("guguji-sector-flow")

TZ_SH = timezone(timedelta(hours=8))

# Eastmoney clist board money-flow
EM_CLIST = "https://push2.eastmoney.com/api/qt/clist/get"
EM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://data.eastmoney.com/bkzj/",
    "Accept": "*/*",
}

BOARD_FS = {
    "industry": "m:90+t:2+f:!50",
    "concept": "m:90+t:3+f:!50",
    "region": "m:90+t:1+f:!50",
}

# period -> (sort field for main net, main field, main ratio field if any)
PERIOD_MAIN = {
    "1": ("f62", "f62", "f184"),
    "5": ("f164", "f164", None),  # 5-day main net; ratio not always stable
    "10": ("f174", "f174", "f175"),
}

LIST_FIELDS = (
    "f12,f14,f2,f3,f20,f62,f184,"
    "f66,f69,f72,f75,f78,f81,f84,f87,"
    "f104,f105,f106,"
    "f164,f166,f168,f170,f172,f174,f175,f176,f177,f178,f179,"
    "f128,f136,f140,"
    "f204,f205,f206,f207,f208,f222,f124"
)

MEMBER_FIELDS = (
    "f12,f14,f2,f3,f62,f184,f66,f72,f78,f84,f6,f8,f9,f20,f104"
)

# simple in-memory cache: key -> (expires_ts, payload)
_cache: dict[str, tuple[float, Any]] = {}
_CACHE_TTL = 15.0  # seconds; boards refresh slowly enough for UI polling


def _now() -> datetime:
    return datetime.now(TZ_SH)


def _now_iso() -> str:
    return _now().isoformat(timespec="seconds")


def _num(v: Any) -> Optional[float]:
    if v is None or v == "-" or v == "":
        return None
    if isinstance(v, (int, float)):
        if v != v:  # NaN
            return None
        return float(v)
    try:
        return float(str(v).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None


def _yi(v: Optional[float]) -> Optional[float]:
    """Yuan -> 亿."""
    if v is None:
        return None
    return round(v / 1e8, 4)


def _signal(chg: Optional[float], main: Optional[float]) -> dict[str, str]:
    """Tag capital behavior from change% + main net inflow."""
    if chg is None or main is None:
        return {"code": "unknown", "label": "数据不足", "tone": "muted"}
    thr = 0.3e8  # 0.3 亿 as weak noise floor
    if main >= thr and chg >= 0.5:
        return {"code": "attack", "label": "资金进攻", "tone": "up"}
    if main >= thr and chg <= -0.5:
        return {"code": "absorb", "label": "低位吸筹", "tone": "mix"}
    if main <= -thr and chg >= 0.5:
        return {"code": "distribute", "label": "高位派发", "tone": "warn"}
    if main <= -thr and chg <= -0.5:
        return {"code": "flee", "label": "资金撤离", "tone": "down"}
    if main >= thr:
        return {"code": "inflow", "label": "净流入", "tone": "up"}
    if main <= -thr:
        return {"code": "outflow", "label": "净流出", "tone": "down"}
    return {"code": "flat", "label": "中性", "tone": "muted"}


def _cache_get(key: str) -> Any:
    hit = _cache.get(key)
    if not hit:
        return None
    exp, val = hit
    if time.time() > exp:
        _cache.pop(key, None)
        return None
    return val


def _cache_set(key: str, val: Any, ttl: float = _CACHE_TTL) -> None:
    _cache[key] = (time.time() + ttl, val)


def _em_get(params: dict[str, Any], timeout: float = 12.0) -> dict:
    """Fetch Eastmoney clist; bypass env proxy and try backup host."""
    hosts = [
        EM_CLIST,
        "https://push2delay.eastmoney.com/api/qt/clist/get",
        "https://82.push2.eastmoney.com/api/qt/clist/get",
    ]
    last_err: Exception | None = None
    session = requests.Session()
    session.trust_env = False  # ignore HTTP(S)_PROXY that often breaks EM locally
    for url in hosts:
        try:
            r = session.get(url, params=params, headers=EM_HEADERS, timeout=timeout)
            r.raise_for_status()
            r.encoding = "utf-8"
            data = r.json()
            if not isinstance(data, dict):
                raise RuntimeError("eastmoney_invalid_json")
            # empty data may still be valid off-hours; return as-is
            return data
        except Exception as e:
            last_err = e
            log.warning("eastmoney fetch failed via %s: %s", url, e)
            continue
    raise RuntimeError(f"eastmoney_unreachable: {last_err}")


def _normalize_board(row: dict, board_type: str, period: str) -> dict:
    _, main_f, ratio_f = PERIOD_MAIN[period]
    chg = _num(row.get("f3"))
    main = _num(row.get(main_f))
    main_ratio = _num(row.get(ratio_f)) if ratio_f else None
    # always expose today breakdown
    super_net = _num(row.get("f66"))
    large_net = _num(row.get("f72"))
    mid_net = _num(row.get("f78"))
    small_net = _num(row.get("f84"))
    sig = _signal(chg, main if period == "1" else _num(row.get("f62")))

    return {
        "code": row.get("f12") or "",
        "name": row.get("f14") or "",
        "type": board_type,
        "price": _num(row.get("f2")),
        "change_pct": chg,
        "market_cap": _num(row.get("f20")),
        "main_net": main,
        "main_net_yi": _yi(main),
        "main_net_ratio": main_ratio,
        "today_main_net": _num(row.get("f62")),
        "today_main_net_yi": _yi(_num(row.get("f62"))),
        "today_main_ratio": _num(row.get("f184")),
        "day5_main_net": _num(row.get("f164")),
        "day5_main_net_yi": _yi(_num(row.get("f164"))),
        "day10_main_net": _num(row.get("f174")),
        "day10_main_net_yi": _yi(_num(row.get("f174"))),
        "day10_main_ratio": _num(row.get("f175")),
        "super_net": super_net,
        "super_net_yi": _yi(super_net),
        "super_ratio": _num(row.get("f69")),
        "large_net": large_net,
        "large_net_yi": _yi(large_net),
        "large_ratio": _num(row.get("f75")),
        "mid_net": mid_net,
        "mid_net_yi": _yi(mid_net),
        "mid_ratio": _num(row.get("f81")),
        "small_net": small_net,
        "small_net_yi": _yi(small_net),
        "small_ratio": _num(row.get("f87")),
        "up_count": int(_num(row.get("f104")) or 0),
        "down_count": int(_num(row.get("f105")) or 0),
        "flat_count": int(_num(row.get("f106")) or 0),
        # 领涨: f204/f205 + f136(涨跌幅更可靠); 领跌: f207/f208 + f222
        "leader_name": row.get("f204") or row.get("f128") or "",
        "leader_code": row.get("f205") or row.get("f140") or "",
        "leader_change_pct": (
            _num(row.get("f136"))
            if _num(row.get("f136")) is not None
            else _num(row.get("f206"))
        ),
        "laggard_name": row.get("f207") or "",
        "laggard_code": row.get("f208") or "",
        "laggard_change_pct": _num(row.get("f222")),
        "signal": sig,
        "updated_ts": int(_num(row.get("f124")) or 0),
    }


def _is_secondary_industry(name: str) -> bool:
    if not name:
        return False
    # 东财行业多级：银行Ⅱ / 国有大型银行Ⅲ
    return name.endswith("Ⅱ") or name.endswith("Ⅲ") or name.endswith("II") or name.endswith("III")


def fetch_board_flow(
    board_type: str = "industry",
    period: str = "1",
    sort: str = "in",
    limit: int = 50,
    page: int = 1,
    primary_only: bool = True,
    refresh: bool = False,
) -> dict[str, Any]:
    """Fetch ranked board fund-flow list.

    sort: in | out | change | name
    period: 1 | 5 | 10  (trading days)
    """
    board_type = (board_type or "industry").lower().strip()
    if board_type not in BOARD_FS:
        raise ValueError(f"unsupported board type: {board_type}")
    period = str(period or "1").strip()
    if period not in PERIOD_MAIN:
        raise ValueError("period must be 1, 5 or 10")
    sort = (sort or "in").lower().strip()
    limit = max(1, min(int(limit), 200))
    page = max(1, int(page))

    sort_fid, _, _ = PERIOD_MAIN[period]
    # po=1 desc, po=0 asc
    if sort == "out":
        fid, po = sort_fid, 0
    elif sort == "change":
        fid, po = "f3", 1
    elif sort == "name":
        fid, po = "f14", 1
    else:  # in
        fid, po = sort_fid, 1

    # Pull a wider page then filter secondary industries so top-N is clean.
    fetch_pz = min(500, max(limit * 3, limit + 20)) if primary_only and board_type == "industry" else limit
    cache_key = f"flow:{board_type}:{period}:{sort}:{page}:{limit}:{primary_only}:{fetch_pz}"
    if not refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

    params = {
        "pn": page,
        "pz": fetch_pz,
        "po": po,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": fid,
        "fs": BOARD_FS[board_type],
        "fields": LIST_FIELDS,
        "_": int(time.time() * 1000),
    }
    raw = _em_get(params)
    data = raw.get("data") or {}
    rows = data.get("diff") or []
    if not isinstance(rows, list):
        rows = []

    items = [_normalize_board(r, board_type, period) for r in rows if isinstance(r, dict)]
    total_raw = int(data.get("total") or len(items))

    if primary_only and board_type == "industry":
        items = [x for x in items if not _is_secondary_industry(x["name"])]

    # re-sort after filter (eastmoney already sorted, filter keeps relative order)
    if sort == "in":
        items.sort(key=lambda x: (x.get("main_net") is None, -(x.get("main_net") or 0)))
    elif sort == "out":
        items.sort(key=lambda x: (x.get("main_net") is None, (x.get("main_net") or 0)))
    elif sort == "change":
        items.sort(key=lambda x: (x.get("change_pct") is None, -(x.get("change_pct") or 0)))

    items = items[:limit]

    # summary stats on returned page
    mains = [x["main_net"] for x in items if x.get("main_net") is not None]
    inflow_n = sum(1 for v in mains if v > 0)
    outflow_n = sum(1 for v in mains if v < 0)
    net_sum = sum(mains) if mains else None

    result = {
        "ok": True,
        "source": "eastmoney",
        "board_type": board_type,
        "period": period,
        "sort": sort,
        "primary_only": primary_only,
        "asof": _now_iso(),
        "total_raw": total_raw,
        "count": len(items),
        "summary": {
            "inflow_count": inflow_n,
            "outflow_count": outflow_n,
            "page_main_net_yi": _yi(net_sum) if net_sum is not None else None,
            "top_in": items[0]["name"] if items and sort != "out" else (items[-1]["name"] if items else None),
        },
        "items": items,
    }
    _cache_set(cache_key, result)
    return result


def fetch_board_members(
    board_code: str,
    limit: int = 30,
    sort: str = "in",
    refresh: bool = False,
) -> dict[str, Any]:
    """Fetch constituent stocks of a board with fund-flow fields."""
    code = (board_code or "").strip().upper()
    if not code.startswith("BK"):
        # allow bare numbers
        if code.isdigit():
            code = f"BK{code}"
        else:
            raise ValueError("board code must look like BK0475")
    limit = max(1, min(int(limit), 100))
    sort = (sort or "in").lower()
    po = 0 if sort == "out" else 1
    fid = "f62" if sort in ("in", "out") else "f3"

    cache_key = f"members:{code}:{limit}:{sort}"
    if not refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

    params = {
        "pn": 1,
        "pz": limit,
        "po": po,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": fid,
        "fs": f"b:{code}+f:!50",
        "fields": MEMBER_FIELDS,
        "_": int(time.time() * 1000),
    }
    raw = _em_get(params)
    data = raw.get("data") or {}
    rows = data.get("diff") or []
    items = []
    for r in rows if isinstance(rows, list) else []:
        if not isinstance(r, dict):
            continue
        chg = _num(r.get("f3"))
        main = _num(r.get("f62"))
        items.append(
            {
                "code": r.get("f12") or "",
                "name": r.get("f14") or "",
                "price": _num(r.get("f2")),
                "change_pct": chg,
                "main_net": main,
                "main_net_yi": _yi(main),
                "main_net_ratio": _num(r.get("f184")),
                "super_net_yi": _yi(_num(r.get("f66"))),
                "large_net_yi": _yi(_num(r.get("f72"))),
                "amount": _num(r.get("f6")),
                "turnover": _num(r.get("f8")),
                "pe": _num(r.get("f9")),
                "signal": _signal(chg, main),
            }
        )

    result = {
        "ok": True,
        "source": "eastmoney",
        "board_code": code,
        "asof": _now_iso(),
        "count": len(items),
        "items": items,
    }
    _cache_set(cache_key, result)
    return result


def dual_rank(
    board_type: str = "industry",
    period: str = "1",
    top: int = 20,
    primary_only: bool = True,
    refresh: bool = False,
) -> dict[str, Any]:
    """Convenience: top inflow + top outflow in one response."""
    top = max(1, min(int(top), 50))
    inflow = fetch_board_flow(
        board_type=board_type,
        period=period,
        sort="in",
        limit=top,
        primary_only=primary_only,
        refresh=refresh,
    )
    outflow = fetch_board_flow(
        board_type=board_type,
        period=period,
        sort="out",
        limit=top,
        primary_only=primary_only,
        refresh=False,  # same underlying pull likely cached
    )
    return {
        "ok": True,
        "source": "eastmoney",
        "board_type": board_type,
        "period": period,
        "asof": _now_iso(),
        "inflow": inflow.get("items") or [],
        "outflow": outflow.get("items") or [],
        "summary": {
            "inflow": inflow.get("summary"),
            "outflow": outflow.get("summary"),
        },
    }


def health() -> dict[str, Any]:
    try:
        sample = fetch_board_flow(board_type="industry", period="1", sort="in", limit=3, primary_only=True)
        ok = bool(sample.get("items"))
        return {
            "ok": ok,
            "service": "sector-flow",
            "source": "eastmoney",
            "asof": _now_iso(),
            "sample_count": sample.get("count", 0),
            "sample_top": (sample.get("items") or [{}])[0].get("name"),
        }
    except Exception as e:
        log.warning("sector flow health failed: %s", e)
        return {"ok": False, "service": "sector-flow", "error": str(e), "asof": _now_iso()}


# ── Intraday multi-board main-force path (Eastmoney fflow kline + local snaps) ──
# push2 clist often accepts the first ut; fflow/kline needs the data.eastmoney ut.
EM_UT = "b2884a393a59ad64002292a3e90d46a5"
EM_FFLOW_UTS = [
    "fa5fd1943c7b386f172d6893dbfba10b",  # data.eastmoney board money-flow
    "b2884a393a59ad64002292a3e90d46a5",
    "7eea3edcaed734bea9cbfc24409ed989",
]
EM_FFLOW_KLINE_HOSTS = [
    "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get",
    "https://push2delay.eastmoney.com/api/qt/stock/fflow/kline/get",
]

# day|board_type -> {"names": {code:name}, "points": [{t, ts, vals:{code:main_yi}}]}
_intraday_snaps: dict[str, dict[str, Any]] = {}
_snap_lock = threading.Lock()
_INTRADAY_TTL = 12.0


def _hhmm_from_any(s: str) -> str:
    """Normalize time to HH:MM. Accepts HH:MM, HH:MM:SS, YYYY-MM-DD HH:MM, compact 0931."""
    s = (s or "").strip().replace("：", ":")
    if not s:
        return "09:30"
    # date-time -> take clock part
    if " " in s:
        s = s.split(" ")[-1]
    # HH:MM or HH:MM:SS
    if ":" in s:
        parts = s.split(":")
        try:
            h = int(parts[0])
            m = int(parts[1]) if len(parts) > 1 else 0
            return f"{h:02d}:{m:02d}"
        except Exception:
            pass
    if len(s) >= 5 and s[2] == ":":
        return s[:5]
    # compact 0931 / 202607290931
    digits = re.sub(r"\D", "", s)
    if len(digits) >= 4:
        digits = digits[-4:]
        return f"{digits[:2]}:{digits[2:]}"
    return s


def _parse_fflow_klines(klines: list) -> list[dict[str, Any]]:
    pts: list[dict[str, Any]] = []
    for line in klines or []:
        if not isinstance(line, str):
            continue
        parts = line.split(",")
        if len(parts) < 2:
            continue
        t = _hhmm_from_any(parts[0])
        main = _num(parts[1])
        if main is None:
            continue
        pts.append(
            {
                "t": t,
                "main_net": main,
                "main_net_yi": _yi(main),
                "super_net_yi": _yi(_num(parts[2])) if len(parts) > 2 else None,
                "large_net_yi": _yi(_num(parts[3])) if len(parts) > 3 else None,
            }
        )
    return pts


def _fetch_one_fflow_minute(code: str, klt: int = 1) -> dict[str, Any]:
    code = (code or "").strip().upper()
    if not code.startswith("BK"):
        raise ValueError("bad board code")
    session = requests.Session()
    session.trust_env = False
    last_err: Exception | None = None
    hosts = list(EM_FFLOW_KLINE_HOSTS) + [
        "https://82.push2.eastmoney.com/api/qt/stock/fflow/kline/get",
    ]
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": f"https://data.eastmoney.com/bkzj/{code}.html",
        "Accept": "*/*",
    }
    for ut in EM_FFLOW_UTS:
        for url in hosts:
            params = {
                "lmt": 0,
                "klt": int(klt),
                "fields1": "f1,f2,f3,f7",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
                "ut": ut,
                "secid": f"90.{code}",
                "_": int(time.time() * 1000),
            }
            try:
                r = session.get(url, params=params, headers=headers, timeout=8)
                r.raise_for_status()
                data = r.json()
                d = data.get("data") or {}
                pts = _parse_fflow_klines(d.get("klines") or [])
                if not pts:
                    last_err = RuntimeError(f"empty_klines rc={data.get('rc')} ut={ut[:8]}")
                    continue
                # chart continuity: start at open zero if EM starts at 09:31
                if pts and (pts[0].get("t") or "") > "09:30":
                    pts = [
                        {
                            "t": "09:30",
                            "main_net": 0.0,
                            "main_net_yi": 0.0,
                            "super_net_yi": 0.0,
                            "large_net_yi": 0.0,
                        }
                    ] + pts
                return {
                    "ok": True,
                    "code": code,
                    "name": d.get("name") or "",
                    "points": pts,
                    "source": "eastmoney_fflow_kline",
                    "ut": ut,
                }
            except Exception as e:
                last_err = e
                continue
    return {"ok": False, "code": code, "points": [], "error": str(last_err)}

def _snap_key(board_type: str) -> str:
    day = _now().strftime("%Y-%m-%d")
    return f"{day}|{board_type}"


def _record_snapshot(board_type: str, items: list[dict[str, Any]]) -> None:
    """Append a full-board main_net snapshot for local intraday path."""
    if not items:
        return
    now = _now()
    # only keep trading-session-ish points (extended a bit for late quotes)
    hm = now.hour * 100 + now.minute
    if hm < 925 or hm > 1510:
        # still record one open/close edge for continuity off-hours? skip flood
        # allow after close single samples until 16:00
        if hm > 1600 or hm < 900:
            return
    t = now.strftime("%H:%M")
    vals: dict[str, float] = {}
    names: dict[str, str] = {}
    for it in items:
        code = (it.get("code") or "").strip().upper()
        if not code:
            continue
        # prefer today main
        main_yi = it.get("today_main_net_yi")
        if main_yi is None:
            main_yi = it.get("main_net_yi")
        if main_yi is None:
            continue
        try:
            vals[code] = float(main_yi)
        except (TypeError, ValueError):
            continue
        names[code] = it.get("name") or code
    if not vals:
        return
    key = _snap_key(board_type)
    with _snap_lock:
        bucket = _intraday_snaps.get(key)
        if not bucket:
            bucket = {"names": {}, "points": []}
            _intraday_snaps[key] = bucket
        bucket["names"].update(names)
        pts = bucket["points"]
        # de-dup same minute: replace last if same t
        if pts and pts[-1].get("t") == t:
            pts[-1] = {"t": t, "ts": now.isoformat(timespec="seconds"), "vals": vals}
        else:
            pts.append({"t": t, "ts": now.isoformat(timespec="seconds"), "vals": vals})
        # cap length (~1 day 1-min * 4h = 240; allow denser 15s ~ 1000)
        if len(pts) > 1200:
            bucket["points"] = pts[-1200:]


def _series_from_snaps(board_type: str, codes: list[str]):
    key = _snap_key(board_type)
    with _snap_lock:
        bucket = _intraday_snaps.get(key) or {"points": []}
        points = list(bucket.get("points") or [])
        names = dict(bucket.get("names") or {})
    out: dict[str, list[dict[str, Any]]] = {c: [] for c in codes}
    for p in points:
        vals = p.get("vals") or {}
        t = p.get("t")
        for c in codes:
            if c in vals:
                out[c].append({"t": t, "main_net_yi": vals[c]})
    return out, names


def _pull_wide_boards(
    board_type: str,
    period: str = "1",
    primary_only: bool = True,
    refresh: bool = False,
) -> list[dict[str, Any]]:
    """Pull a wide page of boards (unsorted filter), for ranking by abs flow."""
    # reuse fetch_board_flow with large limit; for industry filter secondary
    # fetch both in and out isn't needed if we get full sorted list twice —
    # instead request sort=in with high limit then sort=out and merge unique.
    inflow = fetch_board_flow(
        board_type=board_type,
        period=period,
        sort="in",
        limit=120,
        primary_only=primary_only,
        refresh=refresh,
    )
    outflow = fetch_board_flow(
        board_type=board_type,
        period=period,
        sort="out",
        limit=120,
        primary_only=primary_only,
        refresh=False,
    )
    by_code: dict[str, dict] = {}
    for it in (inflow.get("items") or []) + (outflow.get("items") or []):
        code = (it.get("code") or "").strip().upper()
        if code:
            by_code[code] = it
    items = list(by_code.values())
    _record_snapshot(board_type, items)
    return items


def _pick_trend_boards(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    def score(it: dict) -> float:
        v = it.get("today_main_net_yi")
        if v is None:
            v = it.get("main_net_yi")
        try:
            return abs(float(v))
        except (TypeError, ValueError):
            return -1.0

    ranked = sorted(items, key=score, reverse=True)
    # drop null main
    ranked = [x for x in ranked if score(x) >= 0]
    return ranked[: max(1, min(limit, 80))]


def intraday_trend(
    board_type: str = "industry",
    limit: int = 40,
    primary_only: bool = True,
    klt: int = 1,
    refresh: bool = False,
) -> dict[str, Any]:
    """Multi-board intraday main-force cumulative path for charting.

    Prefer Eastmoney minute fflow kline; merge/fallback to local snapshots so
    the tip stays real-time even when EM klines lag.
    """
    board_type = (board_type or "industry").lower().strip()
    if board_type not in BOARD_FS:
        raise ValueError(f"unsupported board type: {board_type}")
    limit = max(4, min(int(limit), 80))
    klt = 1 if int(klt) not in (1, 5) else int(klt)

    cache_key = f"intraday:{board_type}:{limit}:{primary_only}:{klt}"
    if not refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

    items = _pull_wide_boards(
        board_type=board_type,
        period="1",
        primary_only=primary_only,
        refresh=refresh,
    )
    picked = _pick_trend_boards(items, limit)
    codes = [ (x.get("code") or "").upper() for x in picked ]
    meta = {
        c: {
            "name": x.get("name") or c,
            "change_pct": x.get("change_pct"),
            "main_net_yi": x.get("today_main_net_yi") if x.get("today_main_net_yi") is not None else x.get("main_net_yi"),
            "signal": x.get("signal"),
        }
        for c, x in (( (x.get("code") or "").upper(), x) for x in picked)
        if c
    }

    # parallel EM klines
    em_map: dict[str, list[dict[str, Any]]] = {}
    em_names: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=min(16, max(4, len(codes)))) as pool:
        futs = {pool.submit(_fetch_one_fflow_minute, c, klt): c for c in codes}
        for fut in as_completed(futs):
            c = futs[fut]
            try:
                res = fut.result()
            except Exception as e:
                log.warning("fflow kline fail %s: %s", c, e)
                continue
            pts = res.get("points") or []
            if pts:
                em_map[c] = pts
            if res.get("name"):
                em_names[c] = res["name"]

    snap_map, snap_names = _series_from_snaps(board_type, codes)

    series: list[dict[str, Any]] = []
    for c in codes:
        m = meta.get(c) or {}
        name = em_names.get(c) or snap_names.get(c) or m.get("name") or c
        em_pts = em_map.get(c) or []
        snap_pts = snap_map.get(c) or []

        # prefer EM path; ensure latest snap tip is applied for real-time
        if len(em_pts) >= 1:
            path = [{"t": p["t"], "main_net_yi": p.get("main_net_yi")} for p in em_pts]
            source = "eastmoney"
        else:
            path = list(snap_pts)
            source = "snapshot" if path else "none"

        # merge latest snapshot tip if newer / different
        if snap_pts:
            tip = snap_pts[-1]
            if not path:
                path = [tip]
                source = "snapshot"
            else:
                last_t = path[-1].get("t")
                if tip.get("t") != last_t:
                    # if tip time sorts after or equal session progress, append/replace
                    path = path + [tip]
                    if source == "eastmoney":
                        source = "eastmoney+snapshot"
                else:
                    path[-1] = {"t": tip.get("t"), "main_net_yi": tip.get("main_net_yi")}
                    if source == "eastmoney":
                        source = "eastmoney+snapshot"

        # if still empty, single point from live meta
        if not path and m.get("main_net_yi") is not None:
            path = [{"t": _now().strftime("%H:%M"), "main_net_yi": m.get("main_net_yi")}]
            source = "live"

        last = path[-1]["main_net_yi"] if path else m.get("main_net_yi")
        series.append(
            {
                "code": c,
                "name": name,
                "change_pct": m.get("change_pct"),
                "main_net_yi": last,
                "signal": m.get("signal"),
                "source": source,
                "points": path,
                "point_count": len(path),
            }
        )

    # sort series: outflow most negative first on bottom labels? keep abs rank order
    series.sort(key=lambda x: abs(x.get("main_net_yi") or 0), reverse=True)

    # session timeline scaffold
    session = {
        "day": _now().strftime("%Y-%m-%d"),
        "segments": [
            {"start": "09:30", "end": "11:30"},
            {"start": "13:00", "end": "15:00"},
        ],
        "unit": "yi",
        "field": "main_net_yi",
        "label": "主力净流入(累计)",
    }

    em_n = sum(1 for s in series if str(s.get("source", "")).startswith("eastmoney"))
    snap_n = sum(1 for s in series if s.get("source") in ("snapshot", "eastmoney+snapshot", "live"))

    result = {
        "ok": True,
        "source": "eastmoney+local_snapshot",
        "board_type": board_type,
        "period": "1",
        "klt": klt,
        "asof": _now_iso(),
        "limit": limit,
        "count": len(series),
        "em_series": em_n,
        "snapshot_series": snap_n,
        "session": session,
        "series": series,
        "note": "东财分时主力资金累计；盘初无K线时用服务端快照累积；单位亿元",
    }
    _cache_set(cache_key, result, ttl=_INTRADAY_TTL)
    return result


# ?? Market volume + limit-up/down overview (????? / ????ST) ??
EM_ULIST = "https://push2.eastmoney.com/api/qt/ulist.np/get"
EM_ZT_HOSTS = [
    "https://push2ex.eastmoney.com/getTopicZTPool",
    "https://push2exdelay.eastmoney.com/getTopicZTPool",
]
EM_DT_HOSTS = [
    "https://push2ex.eastmoney.com/getTopicDTPool",
    "https://push2exdelay.eastmoney.com/getTopicDTPool",
]
EM_ZB_HOSTS = [
    "https://push2ex.eastmoney.com/getTopicZBPool",
    "https://push2exdelay.eastmoney.com/getTopicZBPool",
]
_MARKET_TTL = 12.0


def _is_st_name(name: str) -> bool:
    n = (name or "").upper().replace(" ", "")
    if not n:
        return False
    return "ST" in n  # covers ST / *ST / S*ST / SST


def _session_progress(now: Optional[datetime] = None) -> dict[str, Any]:
    """A-share continuous auction progress over 240 trading minutes."""
    now = now or _now()
    mins = now.hour * 60 + now.minute + now.second / 60.0
    m0930, m1130, m1300, m1500 = 9 * 60 + 30, 11 * 60 + 30, 13 * 60, 15 * 60
    if mins < m0930:
        elapsed, progress, phase = 0.0, 0.0, "pre"
    elif mins <= m1130:
        elapsed = mins - m0930
        progress = elapsed / 240.0
        phase = "morning"
    elif mins < m1300:
        elapsed, progress, phase = 120.0, 120.0 / 240.0, "lunch"
    elif mins < m1500:
        elapsed = 120.0 + (mins - m1300)
        progress = elapsed / 240.0
        phase = "afternoon"
    else:
        elapsed, progress, phase = 240.0, 1.0, "closed"
    progress = max(0.0, min(1.0, progress))
    return {
        "phase": phase,
        "elapsed_minutes": round(elapsed, 2),
        "total_minutes": 240,
        "progress": round(progress, 4),
        "progress_pct": round(progress * 100, 2),
        "remaining_minutes": round(max(0.0, 240.0 - elapsed), 2),
        "asof_time": now.strftime("%H:%M:%S"),
        "day": now.strftime("%Y-%m-%d"),
    }


def _predict_full_day_linear(amount: Optional[float], progress: float) -> Optional[float]:
    if amount is None:
        return None
    if progress <= 0.01:
        return None
    if progress >= 0.995:
        return float(amount)
    p = max(float(progress), 0.03)
    return float(amount) / p


# backward-compatible alias
def _predict_full_day(amount: Optional[float], progress: float) -> Optional[float]:
    return _predict_full_day_linear(amount, progress)


def _predict_confidence(progress: float, method: str = "linear") -> str:
    """Profile method is more trustworthy earlier in the session."""
    if method == "profile":
        if progress >= 0.55:
            return "high"
        if progress >= 0.20:
            return "medium"
        if progress >= 0.08:
            return "low"
        return "very_low"
    if progress >= 0.75:
        return "high"
    if progress >= 0.35:
        return "medium"
    if progress >= 0.12:
        return "low"
    return "very_low"


def _hhmm_to_session_idx(hhmm: str) -> Optional[int]:
    """Map HH:MM to continuous-auction minute index 0..239."""
    try:
        hh, mm = _hhmm_from_any(hhmm).split(":")[:2]
        h, m = int(hh), int(mm)
    except Exception:
        return None
    mins = h * 60 + m
    m0930, m1130, m1300, m1500 = 9 * 60 + 30, 11 * 60 + 30, 13 * 60, 15 * 60
    if mins < m0930:
        return 0
    if mins <= m1130:
        return mins - m0930
    if mins < m1300:
        return 120
    if mins <= m1500:
        return 120 + (mins - m1300)
    return 239


def _parse_trends_amounts(trends: list) -> dict[str, list[tuple[str, float]]]:
    """day -> [(HH:MM, minute_amount_yuan), ...]"""
    by_day: dict[str, list[tuple[str, float]]] = {}
    for line in trends or []:
        if not isinstance(line, str):
            continue
        parts = line.split(",")
        if len(parts) < 7:
            continue
        head = parts[0].strip()
        if " " not in head:
            continue
        day, tm = head.split(" ", 1)
        tm = _hhmm_from_any(tm)
        amt = _num(parts[6])
        if amt is None:
            continue
        by_day.setdefault(day, []).append((tm, float(amt)))
    for day in by_day:
        by_day[day].sort(key=lambda x: _hhmm_to_session_idx(x[0]) or 0)
    return by_day


def _fetch_index_trends(secid: str, ndays: int = 5) -> list:
    """Multi-day 1-min trends from Eastmoney his (includes amount)."""
    session = requests.Session()
    session.trust_env = False
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://quote.eastmoney.com/",
        "Accept": "*/*",
    }
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "ndays": int(ndays),
        "iscr": 0,
        "secid": secid,
        "_": int(time.time() * 1000),
    }
    hosts = [
        "https://push2his.eastmoney.com/api/qt/stock/trends2/get",
        "https://push2.eastmoney.com/api/qt/stock/trends2/get",
    ]
    last_err: Exception | None = None
    for url in hosts:
        try:
            r = session.get(url, params=params, headers=headers, timeout=12)
            r.raise_for_status()
            data = r.json()
            trends = ((data.get("data") or {}).get("trends")) or []
            if trends:
                return trends
        except Exception as e:
            last_err = e
            continue
    if last_err:
        log.warning("index trends %s failed: %s", secid, last_err)
    return []


def _avg_cum_ratio_curve(
    by_day: dict[str, list[tuple[str, float]]], exclude_day: str
) -> dict[int, float]:
    """Average cumulative volume share by session minute index (completed hist days)."""
    buckets: dict[int, list[float]] = {}
    for day, pts in by_day.items():
        if day == exclude_day:
            continue
        if len(pts) < 200:  # incomplete day, skip
            continue
        total = sum(a for _, a in pts)
        if total <= 0:
            continue
        cum = 0.0
        day_ratio: dict[int, float] = {}
        for tm, amt in pts:
            idx = _hhmm_to_session_idx(tm)
            if idx is None:
                continue
            cum += amt
            day_ratio[idx] = cum / total
        if not day_ratio:
            continue
        for idx in range(0, 240):
            if idx in day_ratio:
                r = day_ratio[idx]
            else:
                prev = [i for i in day_ratio if i <= idx]
                r = day_ratio[max(prev)] if prev else 0.0
            buckets.setdefault(idx, []).append(r)
    curve: dict[int, float] = {}
    for idx, vals in buckets.items():
        if vals:
            curve[idx] = sum(vals) / len(vals)
    return curve


def _ratio_at(curve: dict[int, float], hhmm: str) -> Optional[float]:
    idx = _hhmm_to_session_idx(hhmm)
    if idx is None or not curve:
        return None
    if idx in curve:
        return curve[idx]
    prev = [i for i in curve if i <= idx]
    if prev:
        return curve[max(prev)]
    nxt = [i for i in curve if i >= idx]
    if nxt:
        return curve[min(nxt)]
    return None


def _predict_by_profile(
    amount: Optional[float],
    hhmm: str,
    curve: dict[int, float],
    progress: float,
) -> tuple[Optional[float], str, Optional[float]]:
    """Return (predict_amount, method, ratio_used)."""
    if amount is None:
        return None, "none", None
    if progress >= 0.995:
        return float(amount), "closed", 1.0
    ratio = _ratio_at(curve, hhmm)
    if ratio is not None and ratio >= 0.06:
        ratio = max(0.06, min(0.995, float(ratio)))
        return float(amount) / ratio, "profile", ratio
    pred = _predict_full_day_linear(amount, progress)
    return pred, "linear", progress if progress > 0 else None


def _blend_curves(*curves: dict[int, float]) -> dict[int, float]:
    out: dict[int, float] = {}
    for idx in range(0, 240):
        vals = [c[idx] for c in curves if idx in c]
        if vals:
            out[idx] = sum(vals) / len(vals)
    return out


def _complete_day_totals(
    by_day: dict[str, list[tuple[str, float]]], exclude_day: str
) -> list[tuple[str, float]]:
    """Return [(day, total_yuan), ...] for complete hist days, newest first."""
    rows: list[tuple[str, float]] = []
    for day, pts in by_day.items():
        if day == exclude_day:
            continue
        if len(pts) < 200:
            continue
        total = sum(a for _, a in pts)
        if total > 0:
            rows.append((day, float(total)))
    rows.sort(key=lambda x: x[0], reverse=True)
    return rows


def _get_volume_profiles(today: str) -> dict[str, Any]:
    """Cached SH/SZ/HS ratio curves + previous complete-day totals."""
    cache_key = f"vol_profile:{today}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    sh_curve: dict[int, float] = {}
    sz_curve: dict[int, float] = {}
    sh_by: dict[str, list[tuple[str, float]]] = {}
    sz_by: dict[str, list[tuple[str, float]]] = {}
    try:
        sh_by = _parse_trends_amounts(_fetch_index_trends("1.000001", ndays=5))
        sz_by = _parse_trends_amounts(_fetch_index_trends("0.399001", ndays=5))
        sh_curve = _avg_cum_ratio_curve(sh_by, exclude_day=today)
        sz_curve = _avg_cum_ratio_curve(sz_by, exclude_day=today)
    except Exception as e:
        log.warning("volume profile build failed: %s", e)
    hs_curve = _blend_curves(sh_curve, sz_curve)

    sh_days = _complete_day_totals(sh_by, exclude_day=today)
    sz_days = _complete_day_totals(sz_by, exclude_day=today)
    # align previous trading day: prefer intersection of days present on both
    sh_map = {d: a for d, a in sh_days}
    sz_map = {d: a for d, a in sz_days}
    common = sorted(set(sh_map) & set(sz_map), reverse=True)
    prev_day = common[0] if common else (sh_days[0][0] if sh_days else (sz_days[0][0] if sz_days else None))
    prev_sh = sh_map.get(prev_day) if prev_day else None
    prev_sz = sz_map.get(prev_day) if prev_day else None
    prev_hs = None
    if prev_sh is not None or prev_sz is not None:
        prev_hs = float(prev_sh or 0.0) + float(prev_sz or 0.0)

    payload: dict[str, Any] = {
        "sh": sh_curve,
        "sz": sz_curve,
        "hs": hs_curve,
        "prev_day": prev_day,
        "prev_sh": prev_sh,
        "prev_sz": prev_sz,
        "prev_hs": prev_hs,
    }
    # historical profiles are stable intraday
    _cache_set(cache_key, payload, ttl=1800.0)
    return payload


def _fetch_index_quotes() -> list[dict[str, Any]]:
    session = requests.Session()
    session.trust_env = False
    params = {
        "fltt": "2",
        "secids": "1.000001,0.399001,0.399006,1.000688,0.899050",
        "fields": "f12,f14,f2,f3,f6,f8,f104,f105,f106",
        "ut": EM_UT,
        "_": int(time.time() * 1000),
    }
    hosts = [
        EM_ULIST,
        "https://push2delay.eastmoney.com/api/qt/ulist.np/get",
    ]
    last_err: Exception | None = None
    for url in hosts:
        try:
            r = session.get(url, params=params, headers=EM_HEADERS, timeout=10)
            r.raise_for_status()
            data = r.json()
            diff = ((data.get("data") or {}).get("diff")) or []
            if diff:
                return diff
        except Exception as e:
            last_err = e
            continue
    if last_err:
        log.warning("index quotes failed: %s", last_err)
    return []


def _bj_market_amount() -> Optional[float]:
    """Sum BJ A-share amount (yuan)."""
    session = requests.Session()
    session.trust_env = False
    total_amt = 0.0
    got = 0
    page = 1
    page_size = 200
    total_n = None
    while page <= 5:
        params = {
            "pn": page,
            "pz": page_size,
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f6",
            "fs": "m:0+t:81+s:2048",
            "fields": "f6,f12",
            "ut": EM_UT,
            "_": int(time.time() * 1000),
        }
        try:
            data = _em_get(params, timeout=12.0)
        except Exception as e:
            log.warning("BJ amount page %s failed: %s", page, e)
            break
        d = data.get("data") or {}
        if total_n is None:
            total_n = int(d.get("total") or 0)
        diff = d.get("diff") or []
        if not diff:
            break
        for row in diff:
            v = _num(row.get("f6"))
            if v is not None:
                total_amt += v
                got += 1
        if got >= (total_n or 0) or len(diff) < page_size:
            break
        page += 1
    if got <= 0:
        return None
    return total_amt


def _fetch_topic_pool(hosts: list[str], date_str: str, pagesize: int = 200, sort: str = "fund:desc") -> list[dict[str, Any]]:
    session = requests.Session()
    session.trust_env = False
    params = {
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "dpt": "wz.ztzt",
        "Pageindex": 0,
        "pagesize": int(pagesize),
        "sort": sort or "fund:desc",
        "date": date_str,
        "_": int(time.time() * 1000),
    }
    last_err: Exception | None = None
    for url in hosts:
        try:
            r = session.get(url, params=params, headers=EM_HEADERS, timeout=10)
            r.raise_for_status()
            data = r.json()
            d = data.get("data")
            if not isinstance(d, dict):
                continue
            pool = d.get("pool") or []
            if isinstance(pool, list):
                return pool
        except Exception as e:
            last_err = e
            continue
    if last_err:
        log.warning("topic pool failed %s: %s", hosts[0], last_err)
    return []


def _pool_stats(pool: list[dict[str, Any]]) -> dict[str, Any]:
    raw = list(pool or [])
    non_st = [x for x in raw if not _is_st_name(x.get("n") or "")]
    st = [x for x in raw if _is_st_name(x.get("n") or "")]

    def _top(items: list[dict[str, Any]], n: int = 8) -> list[dict[str, Any]]:
        out = []
        for x in items[:n]:
            out.append(
                {
                    "code": x.get("c"),
                    "name": x.get("n"),
                    "change_pct": _num(x.get("zdp")),
                    "amount_yi": _yi(_num(x.get("amount"))),
                    "board_count": x.get("lbc") or x.get("days"),
                    "industry": x.get("hybk"),
                }
            )
        return out

    # ??? heuristic: first seal near 09:25 and never opened (zbc==0)
    yizi = []
    for x in non_st:
        fbt = x.get("fbt")
        zbc = x.get("zbc")
        try:
            fbt_i = int(fbt) if fbt is not None else -1
        except (TypeError, ValueError):
            fbt_i = -1
        try:
            zbc_i = int(zbc) if zbc is not None else 0
        except (TypeError, ValueError):
            zbc_i = 0
        if zbc_i == 0 and 0 <= fbt_i <= 93000:
            yizi.append(x)

    # ?? >=2
    lb2 = []
    for x in non_st:
        lbc = x.get("lbc") if x.get("lbc") is not None else x.get("days")
        try:
            lbc_i = int(lbc) if lbc is not None else 1
        except (TypeError, ValueError):
            lbc_i = 1
        if lbc_i >= 2:
            lb2.append(x)

    return {
        "raw_count": len(raw),
        "count": len(non_st),
        "st_count": len(st),
        "yizi_count": len(yizi),
        "lb2_count": len(lb2),
        "top": _top(non_st, 8),
        "yizi_top": _top(yizi, 6),
        "lb2_top": _top(sorted(lb2, key=lambda z: int(z.get("lbc") or z.get("days") or 0), reverse=True), 6),
    }


def market_overview(refresh: bool = False) -> dict[str, Any]:
    """??????? + ?????? + ?????(??ST)?"""
    cache_key = "market_overview"
    if not refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            out = dict(cached)
            out["cached"] = True
            return out

    progress = _session_progress()
    p = float(progress["progress"] or 0)

    quotes = _fetch_index_quotes()
    by_code = {str(x.get("f12")): x for x in quotes}
    now_hhmm = (progress.get("asof_time") or "09:30")[:5]
    today = progress.get("day") or _now().strftime("%Y-%m-%d")
    prof = _get_volume_profiles(today)
    sh_curve = prof.get("sh") or {}
    sz_curve = prof.get("sz") or {}
    hs_curve = prof.get("hs") or {}
    prev_day = prof.get("prev_day")
    prev_hs = prof.get("prev_hs")

    def _idx(code: str, name_fallback: str, curve: Optional[dict[int, float]] = None) -> dict[str, Any]:
        row = by_code.get(code) or {}
        amt = _num(row.get("f6"))
        use_curve = curve if curve is not None else hs_curve
        pred, method, ratio = _predict_by_profile(amt, now_hhmm, use_curve, p)
        return {
            "code": code,
            "name": row.get("f14") or name_fallback,
            "price": _num(row.get("f2")),
            "change_pct": _num(row.get("f3")),
            "amount": amt,
            "amount_yi": _yi(amt),
            "predict_amount": pred,
            "predict_amount_yi": _yi(pred) if pred is not None else None,
            "predict_method": method,
            "profile_ratio": round(ratio, 4) if ratio is not None else None,
            "up_count": int(row["f104"]) if row.get("f104") is not None else None,
            "down_count": int(row["f105"]) if row.get("f105") is not None else None,
            "flat_count": int(row["f106"]) if row.get("f106") is not None else None,
        }

    sh = _idx("000001", "上证指数", sh_curve)
    sz = _idx("399001", "深证成指", sz_curve)
    cyb = _idx("399006", "创业板指", sz_curve)
    kc = _idx("000688", "科创50", sh_curve)
    bj50 = _idx("899050", "北证50", hs_curve)

    bj_amt = _bj_market_amount()
    if bj_amt is None:
        bj_amt = bj50.get("amount")
    bj_pred, bj_method, bj_ratio = _predict_by_profile(bj_amt, now_hhmm, hs_curve, p)
    bj = {
        "code": "BJ",
        "name": "北交所",
        "amount": bj_amt,
        "amount_yi": _yi(bj_amt) if bj_amt is not None else None,
        "predict_amount": bj_pred,
        "predict_amount_yi": _yi(bj_pred) if bj_pred is not None else None,
        "predict_method": bj_method,
        "profile_ratio": round(bj_ratio, 4) if bj_ratio is not None else None,
        "note": "全市场成交额汇总" if bj_amt is not None else "回退北证50",
        "index": bj50,
    }

    sh_amt = sh.get("amount") or 0.0
    sz_amt = sz.get("amount") or 0.0
    bj_a = bj_amt or 0.0
    hs_amt = sh_amt + sz_amt
    total_amt = hs_amt + bj_a
    hs_pred, hs_method, hs_ratio = _predict_by_profile(hs_amt, now_hhmm, hs_curve, p)
    total_pred, total_method, total_ratio = _predict_by_profile(total_amt, now_hhmm, hs_curve, p)

    # 较昨日放量/缩量：盘中用预测全天 vs 上一完整交易日沪深成交额；收盘后用实际
    vs_prev: dict[str, Any] = {
        "prev_day": prev_day,
        "prev_hs_amount": prev_hs,
        "prev_hs_amount_yi": _yi(prev_hs) if prev_hs is not None else None,
        "basis": None,
        "today_ref": None,
        "today_ref_yi": None,
        "delta": None,
        "delta_yi": None,
        "direction": None,  # expand / shrink / flat
        "label": None,
    }
    if prev_hs is not None and prev_hs > 0:
        if p >= 0.995:
            today_ref = float(hs_amt)  # 收盘用沪深实际；京所体量很小
            basis = "actual_hs"
        elif hs_pred is not None:
            today_ref = float(hs_pred)
            basis = "predict_hs"
        elif total_pred is not None:
            today_ref = float(total_pred)
            basis = "predict_total"
        else:
            today_ref = None
            basis = None
        if today_ref is not None:
            delta = today_ref - float(prev_hs)
            delta_yi = _yi(delta)
            if delta_yi is None:
                direction, label = None, None
            elif abs(delta_yi) < 1:  # <1亿视为持平
                direction, label = "flat", "持平"
            elif delta_yi > 0:
                direction, label = "expand", "放量"
            else:
                direction, label = "shrink", "缩量"
            vs_prev.update(
                {
                    "basis": basis,
                    "today_ref": today_ref,
                    "today_ref_yi": _yi(today_ref),
                    "delta": delta,
                    "delta_yi": delta_yi,
                    "direction": direction,
                    "label": label,
                }
            )

    day_key = _now().strftime("%Y%m%d")
    zt_pool, dt_pool, zb_pool = [], [], []
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_zt = pool.submit(_fetch_topic_pool, EM_ZT_HOSTS, day_key, 500, "amount:desc")
        f_dt = pool.submit(_fetch_topic_pool, EM_DT_HOSTS, day_key, 200, "amount:desc")
        f_zb = pool.submit(_fetch_topic_pool, EM_ZB_HOSTS, day_key, 200, "amount:desc")
        try:
            zt_pool = f_zt.result()
        except Exception as e:
            log.warning("zt pool: %s", e)
        try:
            dt_pool = f_dt.result()
        except Exception as e:
            log.warning("dt pool: %s", e)
        try:
            zb_pool = f_zb.result()
        except Exception as e:
            log.warning("zb pool: %s", e)

    zt = _pool_stats(zt_pool)
    dt = _pool_stats(dt_pool)
    zb = _pool_stats(zb_pool)

    result = {
        "ok": True,
        "source": "eastmoney",
        "asof": _now_iso(),
        "cached": False,
        "session": progress,
        "volume": {
            "unit": "yi",
            "sh": sh,
            "sz": sz,
            "bj": bj,
            "cyb": cyb,
            "kc": kc,
            "hs": {
                "name": "沪深两市",
                "amount": hs_amt,
                "amount_yi": _yi(hs_amt),
                "predict_amount": hs_pred,
                "predict_amount_yi": _yi(hs_pred) if hs_pred is not None else None,
                "predict_method": hs_method,
                "profile_ratio": round(hs_ratio, 4) if hs_ratio is not None else None,
            },
            "total": {
                "name": "沪深京合计",
                "amount": total_amt,
                "amount_yi": _yi(total_amt),
                "predict_amount": total_pred,
                "predict_amount_yi": _yi(total_pred) if total_pred is not None else None,
                "predict_method": total_method,
                "profile_ratio": round(total_ratio, 4) if total_ratio is not None else None,
            },
            "method": "实际=东财指数成交额；预测=当前额/近几日同时刻累计成交占比(量能曲线)，缺历史时回退线性外推",
            "predict_confidence": _predict_confidence(p, method=total_method if total_method != "none" else "linear"),
            "profile_minutes": len(hs_curve),
            "asof_hhmm": now_hhmm,
            "vs_prev": vs_prev,
        },
        "limit": {
            "date": day_key,
            "exclude_st": True,
            "limit_up": zt,
            "limit_down": dt,
            "broken": zb,
            "summary": {
                "limit_up": zt.get("count", 0),
                "limit_down": dt.get("count", 0),
                "broken": zb.get("count", 0),
                "yizi": zt.get("yizi_count", 0),
                "lb2": zt.get("lb2_count", 0),
                "st_limit_up": zt.get("st_count", 0),
                "st_limit_down": dt.get("st_count", 0),
            },
            "note": "涨停/跌停/炸板来自东财主题池，统计已剔除 ST 名称",
        },
    }
    _cache_set(cache_key, result, ttl=_MARKET_TTL)
    return result

