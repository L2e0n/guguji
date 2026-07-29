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
    "f204,f205,f206,f124"
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
        "leader_name": row.get("f204") or "",
        "leader_code": row.get("f205") or "",
        "leader_change_pct": _num(row.get("f206")),
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
EM_UT = "b2884a393a59ad64002292a3e90d46a5"
EM_FFLOW_KLINE_HOSTS = [
    "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get",
    "https://push2delay.eastmoney.com/api/qt/stock/fflow/kline/get",
]

# day|board_type -> {"names": {code:name}, "points": [{t, ts, vals:{code:main_yi}}]}
_intraday_snaps: dict[str, dict[str, Any]] = {}
_snap_lock = threading.Lock()
_INTRADAY_TTL = 12.0


def _hhmm_from_any(s: str) -> str:
    s = (s or "").strip()
    if " " in s:
        s = s.split(" ")[-1]
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
    params = {
        "lmt": 0,
        "klt": int(klt),
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": EM_UT,
        "secid": f"90.{code}",
        "secid2": "",
        "_": int(time.time() * 1000),
    }
    last_err: Exception | None = None
    for url in EM_FFLOW_KLINE_HOSTS:
        try:
            r = session.get(url, params=params, headers=EM_HEADERS, timeout=8)
            r.raise_for_status()
            data = r.json()
            d = data.get("data") or {}
            pts = _parse_fflow_klines(d.get("klines") or [])
            return {
                "ok": True,
                "code": code,
                "name": d.get("name") or "",
                "points": pts,
                "source": "eastmoney_fflow_kline",
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

