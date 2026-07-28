# -*- coding: utf-8 -*-
"""A-share sector real-time fund flow service (Eastmoney board money-flow).

Provides industry / concept / region board rankings with main-force net inflow,
order-size breakdown, leading stock, and simple capital-flow signals.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
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
