# -*- coding: utf-8 -*-
"""A-share sector real-time fund flow service (Eastmoney board money-flow).

Provides industry / concept / region board rankings with main-force net inflow,
order-size breakdown, leading stock, and simple capital-flow signals.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

import requests

import fund_structure

log = logging.getLogger("guguji-sector-flow")

TZ_SH = timezone(timedelta(hours=8))
YI = 1e8
VOL_PROFILE_TTL_OK = 1800.0  # successful curve cache
VOL_PROFILE_TTL_EMPTY = 45.0  # failed/empty curve short cache
VOL_PROFILE_DISK = Path(__file__).resolve().parent / "data" / "vol_profile_last.json"
IWENCAI_API_URL = os.environ.get("IWENCAI_BASE_URL", "https://openapi.iwencai.com").rstrip("/") + "/v1/query2data"
IWENCAI_SKILL_ID = "hithink-market-query"
IWENCAI_SKILL_VERSION = "1.0.0"

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

# simple in-memory cache: key -> (expires_ts, stale_until_ts, payload)
_cache: dict[str, tuple[float, float, Any]] = {}
_CACHE_TTL = 15.0  # seconds; boards refresh slowly enough for UI polling
_CACHE_STALE = 300.0  # serve last-good up to 5 min on upstream failure


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


def _cache_get(key: str, allow_stale: bool = False) -> Any:
    """Return fresh cache; if allow_stale, return last-good within hard stale window."""
    hit = _cache.get(key)
    if not hit:
        return None
    if len(hit) == 3:
        exp, stale_until, val = hit
    else:
        exp, val = hit  # type: ignore[misc]
        stale_until = exp + _CACHE_STALE
    now = time.time()
    if now <= exp:
        return val
    if allow_stale and now <= stale_until:
        return val
    if now > stale_until:
        _cache.pop(key, None)
    return None


def _cache_set(key: str, val: Any, ttl: float = _CACHE_TTL, stale: float | None = None) -> None:
    now = time.time()
    stale_span = _CACHE_STALE if stale is None else stale
    _cache[key] = (now + ttl, now + max(ttl, stale_span), val)


def _cache_stale_payload(key: str) -> Any:
    return _cache_get(key, allow_stale=True)


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
    # 结构资金（今日分档代理；5日/10日榜仍附带今日结构）
    if super_net is None and large_net is None:
        force_net = _num(row.get("f62"))
    else:
        force_net = (super_net or 0.0) + (large_net or 0.0)
    retail_net = small_net
    scissors = None
    if force_net is not None and retail_net is not None:
        scissors = force_net - retail_net
    abs_sum = abs(super_net or 0) + abs(large_net or 0) + abs(mid_net or 0) + abs(small_net or 0)
    size_main_share = round((abs(super_net or 0) + abs(large_net or 0)) / abs_sum * 100, 2) if abs_sum > 0 else None
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
        "force_net": force_net,
        "force_net_yi": _yi(force_net),
        "retail_net": retail_net,
        "retail_net_yi": _yi(retail_net),
        "scissors": scissors,
        "scissors_yi": _yi(scissors),
        "size_main_share_pct": size_main_share,
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
    dual_key = f"dual:{board_type}:{period}:{top}:{primary_only}"
    try:
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
        result = {
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
            "stale": False,
            "cached": False,
        }
        _cache_set(dual_key, result, ttl=_CACHE_TTL, stale=_CACHE_STALE)
        return result
    except Exception as e:
        stale = _cache_stale_payload(dual_key)
        if stale and isinstance(stale, dict) and stale.get("inflow") is not None:
            out = dict(stale)
            out["ok"] = True
            out["stale"] = True
            out["cached"] = True
            out["error"] = str(e)
            out["asof"] = out.get("asof") or _now_iso()
            log.warning("dual_rank fallback to stale: %s", e)
            return out
        raise


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
    def net_val(it: dict):
        v = it.get("today_main_net_yi")
        if v is None:
            v = it.get("main_net_yi")
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    # 10板：流入 TOP5 + 流出 TOP5（其余 limit 仍按绝对值最大）
    if int(limit) == 10:
        scored = [(it, net_val(it)) for it in items]
        scored = [(it, v) for it, v in scored if v is not None]
        inflows = sorted([x for x in scored if x[1] > 0], key=lambda x: x[1], reverse=True)[:5]
        outflows = sorted([x for x in scored if x[1] < 0], key=lambda x: x[1])[:5]
        picked = [x[0] for x in inflows] + [x[0] for x in outflows]
        if len(picked) < 10:
            used = {id(x) for x in picked}
            for it, _ in sorted(scored, key=lambda x: abs(x[1]), reverse=True):
                if id(it) in used:
                    continue
                picked.append(it)
                used.add(id(it))
                if len(picked) >= 10:
                    break
        return picked

    def score(it: dict) -> float:
        v = net_val(it)
        return abs(v) if v is not None else -1.0

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
            if isinstance(cached, dict):
                out = dict(cached)
                out["cached"] = True
                out["stale"] = False
                return out
            return cached

    try:
        return _intraday_trend_fresh(
            board_type=board_type,
            limit=limit,
            primary_only=primary_only,
            klt=klt,
            refresh=refresh,
            cache_key=cache_key,
        )
    except Exception as e:
        stale = _cache_stale_payload(cache_key)
        if stale and isinstance(stale, dict):
            out = dict(stale)
            out["ok"] = True
            out["stale"] = True
            out["cached"] = True
            out["error"] = str(e)
            log.warning("intraday_trend fallback to stale: %s", e)
            return out
        raise


def _intraday_trend_fresh(
    board_type: str,
    limit: int,
    primary_only: bool,
    klt: int,
    refresh: bool,
    cache_key: str,
) -> dict[str, Any]:
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
    """Deprecated: kept only for reference/tests; market overview no longer uses linear fallback."""
    if amount is None:
        return None
    if progress <= 0.01:
        return None
    if progress >= 0.995:
        return float(amount)
    p = max(float(progress), 0.03)
    return float(amount) / p


# backward-compatible alias (no longer used by market overview)
def _predict_full_day(amount: Optional[float], progress: float) -> Optional[float]:
    return _predict_full_day_linear(amount, progress)


def _predict_confidence(progress: float, method: str = "profile") -> str:
    """Profile method confidence by session progress. unavailable always very_low."""
    m = (method or "").lower()
    if m in ("none", "unavailable", ""):
        return "very_low"
    if m == "closed":
        return "high"
    # profile / profile_cache / hithink-assisted
    if progress >= 0.55:
        return "high"
    if progress >= 0.20:
        return "medium"
    if progress >= 0.08:
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


def _fetch_index_trends(secid: str, ndays: int = 5, retries: int = 1) -> list:
    """Multi-day 1-min trends from Eastmoney his (includes amount). Retries + dual hosts."""
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
        "Connection": "close",
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
        "https://push2delay.eastmoney.com/api/qt/stock/trends2/get",
    ]
    last_err: Exception | None = None
    for attempt in range(max(1, int(retries))):
        params["_"] = int(time.time() * 1000)
        for url in hosts:
            try:
                r = session.get(url, params=params, headers=headers, timeout=6)
                r.raise_for_status()
                data = r.json()
                trends = ((data.get("data") or {}).get("trends")) or []
                if trends:
                    if attempt > 0:
                        log.info("index trends %s recovered via %s attempt=%s", secid, url, attempt + 1)
                    return trends
            except Exception as e:
                last_err = e
                continue
        time.sleep(0.35 * (attempt + 1))
    if last_err:
        log.warning("index trends %s failed after retries: %s", secid, last_err)
    return []


def _avg_cum_ratio_curve(
    by_day: dict[str, list[tuple[str, float]]], exclude_day: str
) -> dict[int, float]:
    """Average cumulative volume share by session minute index (completed hist days)."""
    buckets: dict[int, list[float]] = {}
    for day, pts in by_day.items():
        if day == exclude_day:
            continue
        if len(pts) < 200:
            continue
        total = sum(a for _, a in pts)
        if total <= 0:
            continue
        cum = 0.0
        day_ratio: dict[int, float] = {}
        for tm, a in pts:
            idx = _hhmm_to_session_idx(tm)
            if idx is None:
                continue
            cum += a
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
    method_label: str = "profile",
) -> tuple[Optional[float], str, Optional[float]]:
    """Return (predict_amount, method, ratio_used). No linear fallback."""
    if amount is None:
        return None, "none", None
    if progress >= 0.995:
        return float(amount), "closed", 1.0
    ratio = _ratio_at(curve, hhmm)
    if ratio is not None and ratio >= 0.06:
        ratio = max(0.06, min(0.995, float(ratio)))
        label = method_label if method_label else "profile"
        return float(amount) / ratio, label, ratio
    return None, "unavailable", None


def _builtin_session_curve() -> dict[int, float]:
    """Typical A-share cumulative volume share by session minute (0..239).

    Not a linear amount/progress model: denser open + lunch reopen + close auction
    ramp. Used when Eastmoney minute curves are unavailable and no disk last-good.
    """
    dens: list[float] = []
    for i in range(240):
        if i < 30:  # 09:30-10:00 open rush
            d = 2.35 - i * 0.028
        elif i < 120:  # morning body
            d = 1.25 - (i - 30) * 0.0055
        elif i < 150:  # 13:00-13:30 soft reopen
            d = 0.95 + (i - 120) * 0.008
        elif i < 210:  # afternoon body
            d = 1.15 + (i - 150) * 0.004
        else:  # last 30m ramp
            d = 1.4 + (i - 210) * 0.03
        dens.append(max(0.35, float(d)))
    total = sum(dens) or 1.0
    cum = 0.0
    curve: dict[int, float] = {}
    for i, d in enumerate(dens):
        cum += d
        curve[i] = cum / total
    curve[239] = 1.0
    return curve


_BUILTIN_SESSION_CURVE = _builtin_session_curve()


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


def _curve_to_str_keys(curve: dict[int, float]) -> dict[str, float]:
    return {str(k): float(v) for k, v in curve.items()}


def _curve_from_str_keys(raw: Any) -> dict[int, float]:
    out: dict[int, float] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        try:
            out[int(k)] = float(v)
        except (TypeError, ValueError):
            continue
    return out


def _save_disk_profile(payload: dict[str, Any]) -> None:
    try:
        VOL_PROFILE_DISK.parent.mkdir(parents=True, exist_ok=True)
        disk = {
            "saved_at": _now_iso(),
            "prev_day": payload.get("prev_day"),
            "prev_sh": payload.get("prev_sh"),
            "prev_sz": payload.get("prev_sz"),
            "prev_hs": payload.get("prev_hs"),
            "sh": _curve_to_str_keys(payload.get("sh") or {}),
            "sz": _curve_to_str_keys(payload.get("sz") or {}),
            "hs": _curve_to_str_keys(payload.get("hs") or {}),
        }
        VOL_PROFILE_DISK.write_text(json.dumps(disk, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        log.warning("save vol profile disk failed: %s", e)


def _load_disk_profile() -> Optional[dict[str, Any]]:
    try:
        if not VOL_PROFILE_DISK.exists():
            return None
        raw = json.loads(VOL_PROFILE_DISK.read_text(encoding="utf-8"))
        sh = _curve_from_str_keys(raw.get("sh"))
        sz = _curve_from_str_keys(raw.get("sz"))
        hs = _curve_from_str_keys(raw.get("hs")) or _blend_curves(sh, sz)
        if len(hs) < 30:
            return None
        return {
            "sh": sh,
            "sz": sz,
            "hs": hs,
            "prev_day": raw.get("prev_day"),
            "prev_sh": raw.get("prev_sh"),
            "prev_sz": raw.get("prev_sz"),
            "prev_hs": raw.get("prev_hs"),
            "profile_source": "disk_cache",
            "saved_at": raw.get("saved_at"),
        }
    except Exception as e:
        log.warning("load vol profile disk failed: %s", e)
        return None


def _hithink_api_key() -> str:
    return (os.environ.get("IWENCAI_API_KEY") or "").strip()


def _hithink_query(query: str, limit: int = 10, timeout: int = 30) -> dict[str, Any]:
    key = _hithink_api_key()
    if not key:
        raise RuntimeError("IWENCAI_API_KEY 未配置")
    payload = {
        "query": query,
        "page": "1",
        "limit": str(limit),
        "is_cache": "1",
        "expand_index": "true",
    }
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "X-Claw-Call-Type": "normal",
        "X-Claw-Skill-Id": IWENCAI_SKILL_ID,
        "X-Claw-Skill-Version": IWENCAI_SKILL_VERSION,
        "X-Claw-Plugin-Id": "none",
        "X-Claw-Plugin-Version": "none",
        "X-Claw-Trace-Id": secrets.token_hex(32),
    }
    r = requests.post(
        IWENCAI_API_URL,
        headers=headers,
        json=payload,
        timeout=timeout,
    )
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError("hithink response not dict")
    return data


def _hithink_pick(row: dict, *names: str) -> Any:
    if not row:
        return None
    for n in names:
        if n in row and row[n] not in (None, ""):
            return row[n]
    for n in names:
        for k, v in row.items():
            ks = str(k)
            if ks == n or ks.startswith(n + "[") or ks.startswith(n + ":"):
                if v not in (None, ""):
                    return v
    low = [n.lower() for n in names]
    for k, v in row.items():
        kl = str(k).lower()
        for n in low:
            if n in kl and v not in (None, ""):
                return v
    return None


def _hithink_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace("亿", "")
    if not s or s.lower() in ("nan", "none", "--", "-"):
        return None
    try:
        # if original had 亿 unit text and was stripped, caller must not double-scale;
        # iwencai amount fields are usually in yuan as plain numbers.
        return float(s)
    except ValueError:
        return None


def _hithink_index_amounts() -> dict[str, Any]:
    """Backup index amounts via 问财/hithink. Returns yuan amounts."""
    cache_key = "hithink_index_amounts"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    if not _hithink_api_key():
        return {"ok": False, "error": "no_api_key"}
    try:
        data = _hithink_query(
            "上证指数,深证成指,北证50,创业板指,科创50 成交额 涨跌幅 最新价",
            limit=20,
            timeout=28,
        )
        rows = data.get("datas") or []
        out: dict[str, Any] = {
            "ok": True,
            "source": "hithink",
            "sh": None,
            "sz": None,
            "bj": None,
            "cyb": None,
            "kc50": None,
            "raw_count": len(rows),
        }
        for row in rows:
            if not isinstance(row, dict):
                continue
            code = str(_hithink_pick(row, "指数代码", "code") or "")
            name = str(_hithink_pick(row, "指数简称", "name") or "")
            # 成交额[YYYYMMDD] preferred for today snapshot
            amt = _hithink_float(_hithink_pick(row, "成交额", "amount"))
            if amt is None:
                for k, v in row.items():
                    ks = str(k)
                    if ks.startswith("成交额") or "成交额[" in ks:
                        amt = _hithink_float(v)
                        if amt is not None:
                            break
            chg = _hithink_float(_hithink_pick(row, "涨跌幅", "最新涨跌幅", "change"))
            price = _hithink_float(_hithink_pick(row, "最新价", "price"))
            item = {"amount": amt, "change_pct": chg, "price": price, "name": name, "code": code}
            code_u = code.upper()
            if "000001" in code_u or name == "上证指数" or ("上证" in name and "综" not in name and "50" not in name):
                out["sh"] = item
            elif "399001" in code_u or "深证成" in name:
                out["sz"] = item
            elif "899050" in code_u or "北证50" in name or name == "北证50":
                out["bj"] = item
            elif "399006" in code_u or "创业板" in name:
                out["cyb"] = item
            elif "000688" in code_u or "科创50" in name:
                out["kc50"] = item
        _cache_set(cache_key, out, ttl=40.0, stale=300.0)
        return out
    except Exception as e:
        log.warning("hithink index amounts failed: %s", e)
        stale = _cache_stale_payload(cache_key)
        if stale:
            return stale
        return {"ok": False, "error": str(e)}


def _hithink_row_amount(row: dict[str, Any], day_compact: Optional[str] = None) -> Optional[float]:
    """Parse amount fields including date-tagged amount[YYYYMMDD]."""
    if not isinstance(row, dict):
        return None
    amt_key = "成交额"
    preferred: list[str] = []
    if day_compact:
        preferred.append(f"{amt_key}[{day_compact}]")
    preferred.extend([amt_key, "amount"])
    amt = _hithink_float(_hithink_pick(row, *preferred))
    if amt is not None:
        return amt
    tagged: list[tuple[str, float]] = []
    for k, v in row.items():
        ks = str(k)
        if ks == amt_key or ks.startswith(amt_key + "[") or (amt_key + "[") in ks:
            fv = _hithink_float(v)
            if fv is not None:
                tagged.append((ks, fv))
    if not tagged:
        return None
    if day_compact:
        for ks, fv in tagged:
            if day_compact in ks:
                return fv
    tagged.sort(key=lambda x: x[0], reverse=True)
    return tagged[0][1]


def _hithink_prev_day_amounts(prev_day: Optional[str]) -> dict[str, Any]:
    """Fetch previous complete-day SH+SZ index amounts from hithink."""
    if not prev_day or not _hithink_api_key():
        return {"ok": False}
    cache_key = f"hithink_prev:{prev_day}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    dcompact = prev_day.replace("-", "")
    sh_name = "上证指数"
    sz_name = "深证成指"
    sh_token = "上证"
    sz_token = "深证成"
    code_key = "指数代码"
    name_key = "指数简称"
    amt_key = "成交额"
    try:
        q = f"{sh_name},{sz_name} {amt_key}[{dcompact}]"
        data = _hithink_query(q, limit=10, timeout=28)
        rows = data.get("datas") or []
        sh = sz = None
        for row in rows:
            if not isinstance(row, dict):
                continue
            code = str(_hithink_pick(row, code_key, "code") or "").upper()
            name = str(_hithink_pick(row, name_key, "name") or "")
            amt = _hithink_row_amount(row, dcompact)
            if amt is None:
                continue
            if "000001" in code or name == sh_name or (sh_token in name and "深" not in name and "50" not in name and "综" not in name):
                sh = float(amt)
            elif "399001" in code or sz_token in name:
                sz = float(amt)
        ok = sh is not None and sz is not None and sh > 0 and sz > 0
        out = {"ok": ok, "source": "hithink", "prev_day": prev_day, "prev_sh": sh, "prev_sz": sz, "prev_hs": (float(sh) + float(sz)) if ok else None}
        if ok:
            _cache_set(cache_key, out, ttl=3600.0, stale=7200.0)
        return out
    except Exception as e:
        log.warning("hithink prev day amounts failed: %s", e)
        return {"ok": False, "error": str(e)}


def _get_volume_profiles(today: str) -> dict[str, Any]:
    """Cached SH/SZ/HS ratio curves + previous complete-day totals.

    Curve priority:
      1) Eastmoney trends2 (fail-fast)
      2) disk last-good
      3) builtin A-share session profile (always available; NOT linear)
    Prev-day totals:
      1) EM complete days if any
      2) disk
      3) hithink/问财 (primary backup when EM down)
    """
    cache_key = f"vol_profile:{today}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    sh_curve: dict[int, float] = {}
    sz_curve: dict[int, float] = {}
    sh_by: dict[str, list[tuple[str, float]]] = {}
    sz_by: dict[str, list[tuple[str, float]]] = {}
    profile_source = "none"
    em_error = None

    try:
        sh_by = _parse_trends_amounts(_fetch_index_trends("1.000001", ndays=5, retries=1))
        sz_by = _parse_trends_amounts(_fetch_index_trends("0.399001", ndays=5, retries=1))
        sh_curve = _avg_cum_ratio_curve(sh_by, exclude_day=today)
        sz_curve = _avg_cum_ratio_curve(sz_by, exclude_day=today)
        if sh_curve or sz_curve:
            profile_source = "eastmoney"
    except Exception as e:
        em_error = str(e)
        log.warning("volume profile build failed: %s", e)

    hs_curve = _blend_curves(sh_curve, sz_curve)

    sh_days = _complete_day_totals(sh_by, exclude_day=today)
    sz_days = _complete_day_totals(sz_by, exclude_day=today)
    sh_map = {d: a for d, a in sh_days}
    sz_map = {d: a for d, a in sz_days}
    # ????????????????????????
    common = sorted(set(sh_map) & set(sz_map), reverse=True)
    prev_day = common[0] if common else None
    prev_sh = sh_map.get(prev_day) if prev_day else None
    prev_sz = sz_map.get(prev_day) if prev_day else None
    prev_hs = None
    if prev_sh is not None and prev_sz is not None:
        prev_hs = float(prev_sh) + float(prev_sz)
    prev_source = "eastmoney" if prev_hs else "none"

    # Backup curve from disk last-good if EM empty
    if len(hs_curve) < 30:
        disk = _load_disk_profile()
        if disk and len(disk.get("hs") or {}) >= 30:
            sh_curve = disk.get("sh") or sh_curve
            sz_curve = disk.get("sz") or sz_curve
            hs_curve = disk.get("hs") or hs_curve
            profile_source = "disk_cache"
            if prev_hs is None:
                d_sh = disk.get("prev_sh")
                d_sz = disk.get("prev_sz")
                # reject polluted cache where only one market was stored as HS
                if d_sh is not None and d_sz is not None and float(d_sh) > 0 and float(d_sz) > 0:
                    prev_day = disk.get("prev_day") or prev_day
                    prev_sh = float(d_sh)
                    prev_sz = float(d_sz)
                    prev_hs = float(d_sh) + float(d_sz)
                    prev_source = "disk_cache"

    # Always-available non-linear session profile (no waiting for EM)
    if len(hs_curve) < 30:
        sh_curve = dict(_BUILTIN_SESSION_CURVE)
        sz_curve = dict(_BUILTIN_SESSION_CURVE)
        hs_curve = dict(_BUILTIN_SESSION_CURVE)
        profile_source = "builtin"

    # EM sometimes returns only SZ (or only SH). Never leave a side empty when HS exists.
    if len(hs_curve) >= 30:
        if len(sh_curve) < 30:
            sh_curve = dict(hs_curve)
        if len(sz_curve) < 30:
            sz_curve = dict(hs_curve)

    # Prefer hithink for previous complete-day HS totals (SH+SZ both required).
    # Do not wait on EM; override incomplete/polluted EM or disk values.
    if _hithink_api_key():
        guess = None
        try:
            d0 = datetime.strptime(today, "%Y-%m-%d")
            for i in range(1, 8):
                d1 = d0 - timedelta(days=i)
                if d1.weekday() < 5:
                    guess = d1.strftime("%Y-%m-%d")
                    break
        except Exception:
            guess = None
        ht = _hithink_prev_day_amounts(prev_day or guess)
        if ht.get("ok") and ht.get("prev_sh") is not None and ht.get("prev_sz") is not None:
            prev_day = ht.get("prev_day") or prev_day or guess
            prev_sh = ht.get("prev_sh")
            prev_sz = ht.get("prev_sz")
            prev_hs = float(prev_sh) + float(prev_sz)
            prev_source = "hithink"

    payload: dict[str, Any] = {
        "sh": sh_curve,
        "sz": sz_curve,
        "hs": hs_curve,
        "prev_day": prev_day,
        "prev_sh": prev_sh,
        "prev_sz": prev_sz,
        "prev_hs": prev_hs,
        "profile_source": profile_source,
        "prev_source": prev_source,
        "em_error": em_error,
        "curve_points": len(hs_curve),
        "method_label": (
            "profile" if profile_source == "eastmoney"
            else ("profile_cache" if profile_source == "disk_cache" else "profile_builtin")
        ),
    }

    ok_curve = len(hs_curve) >= 30
    if ok_curve and profile_source == "eastmoney":
        _save_disk_profile(payload)

    # builtin/disk also long-TTL (usable); only pure-empty short cache
    ttl = VOL_PROFILE_TTL_OK if ok_curve else VOL_PROFILE_TTL_EMPTY
    _cache_set(cache_key, payload, ttl=ttl, stale=max(ttl, 300.0))
    return payload


def _fetch_index_quotes() -> list[dict[str, Any]]:
    session = requests.Session()
    session.trust_env = False
    params = {
        "fltt": "2",
        "secids": "1.000001,0.399001,0.399006,1.000680,1.000688,0.899050",
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
        "top": _top(non_st, 20),
        "yizi_top": _top(yizi, 8),
        "lb2_top": _top(sorted(lb2, key=lambda z: int(z.get("lbc") or z.get("days") or 0), reverse=True), 8),
    }


def market_overview(refresh: bool = False) -> dict[str, Any]:
    """??????? + ?????? + ?????(??ST)?"""
    cache_key = "market_overview"
    if not refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            out = dict(cached)
            out["cached"] = True
            out["stale"] = False
            return out

    try:
        return _market_overview_fresh(cache_key, refresh=refresh)
    except Exception as e:
        stale = _cache_stale_payload(cache_key)
        if stale and isinstance(stale, dict):
            out = dict(stale)
            out["ok"] = True
            out["stale"] = True
            out["cached"] = True
            out["error"] = str(e)
            log.warning("market_overview fallback to stale: %s", e)
            return out
        raise


def _market_overview_fresh(cache_key: str, refresh: bool = False) -> dict[str, Any]:
    progress = _session_progress()
    p = float(progress["progress"] or 0)

    quotes = _fetch_index_quotes()
    by_code = {str(x.get("f12")): x for x in quotes}
    amount_source = "eastmoney" if quotes else "none"
    # Prefer hithink amounts when configured (EM often blocked on cloud hosts)
    ht_amt = _hithink_index_amounts() if _hithink_api_key() else {"ok": False}

    def _apply_ht(code: str, ht_key: str, prefer: bool = False) -> None:
        nonlocal amount_source
        item = (ht_amt or {}).get(ht_key) or {}
        amt = item.get("amount")
        if amt is None:
            return
        row = by_code.get(code) or {}
        em_amt = _num(row.get("f6"))
        if (not prefer) and em_amt is not None:
            return
        if code not in by_code:
            by_code[code] = {"f12": code, "f14": item.get("name") or code}
        by_code[code]["f6"] = amt
        if item.get("change_pct") is not None:
            if prefer or by_code[code].get("f3") is None:
                by_code[code]["f3"] = item.get("change_pct")
        if item.get("price") is not None:
            if prefer or by_code[code].get("f2") is None:
                by_code[code]["f2"] = item.get("price")
        if item.get("name") and not by_code[code].get("f14"):
            by_code[code]["f14"] = item.get("name")
        if prefer:
            # preferred path: report hithink even if EM quote shells exist for name/price
            amount_source = "hithink"
        elif amount_source == "eastmoney" and em_amt is None:
            amount_source = "mixed" if quotes else "hithink"
        elif amount_source == "none":
            amount_source = "hithink"

    # Always prefer hithink amounts when API key is configured (do not wait for EM recovery).
    # EM quotes remain gap-fill only if hithink misses a field.
    prefer_ht = bool(_hithink_api_key()) and bool((ht_amt or {}).get("ok"))
    # Overlay hithink when prefer; otherwise fill gaps only
    for code, key in (
        ("000001", "sh"),
        ("399001", "sz"),
        ("899050", "bj"),
        ("399006", "cyb"),
        ("000688", "kc50"),
    ):
        _apply_ht(code, key, prefer=prefer_ht)
    # gap fill for any still missing
    for code, key in (
        ("000001", "sh"),
        ("399001", "sz"),
        ("899050", "bj"),
        ("399006", "cyb"),
        ("000688", "kc50"),
    ):
        _apply_ht(code, key, prefer=False)

    now_hhmm = (progress.get("asof_time") or "09:30")[:5]
    today = progress.get("day") or _now().strftime("%Y-%m-%d")
    prof = _get_volume_profiles(today)
    sh_curve = prof.get("sh") or {}
    sz_curve = prof.get("sz") or {}
    hs_curve = prof.get("hs") or {}
    prev_day = prof.get("prev_day")
    prev_hs = prof.get("prev_hs")
    method_label = str(prof.get("method_label") or "profile")

    def _idx(code: str, name_fallback: str, curve: Optional[dict[int, float]] = None) -> dict[str, Any]:
        row = by_code.get(code) or {}
        amt = _num(row.get("f6"))
        use_curve = curve if curve is not None else hs_curve
        pred, method, ratio = _predict_by_profile(amt, now_hhmm, use_curve, p, method_label=method_label)
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
    kc = _idx("000680", "科创综指", sh_curve)  # 科创板全市场成交额口径
    kc50 = _idx("000688", "科创50", sh_curve)
    bj50 = _idx("899050", "北证50", hs_curve)

    bj_amt = _bj_market_amount()
    if bj_amt is None:
        bj_amt = bj50.get("amount")
    bj_pred, bj_method, bj_ratio = _predict_by_profile(bj_amt, now_hhmm, hs_curve, p, method_label=method_label)
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
    hs_pred, hs_method, hs_ratio = _predict_by_profile(hs_amt, now_hhmm, hs_curve, p, method_label=method_label)
    total_pred, total_method, total_ratio = _predict_by_profile(total_amt, now_hhmm, hs_curve, p, method_label=method_label)

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
            "sh": {**sh, "board": "shanghai", "label": "上证"},
            "sz": {**sz, "board": "shenzhen", "label": "深成指"},
            "bj": bj,
            "cyb": {**cyb, "board": "chinext", "label": "创业板", "parent": "sz"},
            "kc": {**kc, "board": "star", "label": "科创板", "parent": "sh", "index_note": "科创综指成交额≈科创板"},
            "kc50": {**kc50, "board": "star50", "label": "科创50", "parent": "sh"},
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
            "method": "实际=优先问财/hithink成交额(东财可用则混合)；预测=当前额/同时刻累计成交占比曲线(东财分时→磁盘last-good→内置A股量能曲线，非线性)；前日对比优先问财；已取消线性回退",
            "predict_confidence": _predict_confidence(p, method=total_method if total_method not in (None, "none") else "unavailable"),
            "profile_minutes": len(hs_curve),
            "profile_source": prof.get("profile_source") or ("eastmoney" if hs_curve else "none"),
            "prev_source": prof.get("prev_source") or "none",
            "amount_source": amount_source,
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
        "structure": fund_structure.market_structure(refresh=refresh),
    }
    _cache_set(cache_key, result, ttl=_MARKET_TTL)
    return result

