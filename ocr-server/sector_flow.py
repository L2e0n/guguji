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
VOL_PROFILE_DAYS = Path(__file__).resolve().parent / "data" / "vol_profile_days.json"
IWENCAI_API_URL = os.environ.get("IWENCAI_BASE_URL", "https://openapi.iwencai.com").rstrip("/") + "/v1/query2data"
IWENCAI_SKILL_ID = "hithink-market-query"
IWENCAI_SKILL_VERSION = "1.0.0"
YIXIN_API_URL = os.environ.get(
    "YIXIN_API_URL", "https://openapi.billionsintelligence.com/api/v1/fin_db"
).rstrip("/")
YIXIN_AMOUNT_TTL = 45.0
YIXIN_CURVE_TTL = 1800.0

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
        "leader_name": row.get("f128") or row.get("f204") or "",
        "leader_code": row.get("f140") or row.get("f205") or "",
        # f128/f140/f136 are the true top-gainer trio; do NOT pair f204/f205 with f136.
        # f204/f205/f206 is a different field set (often top main-net; f206 != change%).
        "leader_change_pct": _num(row.get("f136")),
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
EM_QS_HOSTS = [
    "https://push2ex.eastmoney.com/getTopicQSPool",
    "https://push2exdelay.eastmoney.com/getTopicQSPool",
]
EM_CX_HOSTS = [
    "https://push2ex.eastmoney.com/getTopicCXPool",
    "https://push2exdelay.eastmoney.com/getTopicCXPool",
]
# typo-tolerant alias used by some EM frontends
EM_CX_HOSTS_ALT = [
    "https://push2ex.eastmoney.com/getTopicCXPooll",
    "https://push2exdelay.eastmoney.com/getTopicCXPooll",
]
EM_YZT_HOSTS = [
    "https://push2ex.eastmoney.com/getYesterdayZTPool",
    "https://push2exdelay.eastmoney.com/getYesterdayZTPool",
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
    # profile / profile_selfcal / profile_cache / hithink-assisted
    if "selfcal" in m:
        # path-fit adds info after open; still medium until afternoon
        if progress >= 0.55:
            return "high"
        if progress >= 0.18:
            return "medium"
        return "low"
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


def _em_daily_kline_rows(secid: str, lmt: int = 12) -> list[dict[str, Any]]:
    """Fetch recent daily OHLCV for an index. amount is full-day yuan turnover."""
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
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 101,
        "fqt": 1,
        "end": "20500101",
        "lmt": int(lmt),
        "_": int(time.time() * 1000),
    }
    hosts = [
        "https://push2his.eastmoney.com/api/qt/stock/kline/get",
        "https://push2hisdelay.eastmoney.com/api/qt/stock/kline/get",
        "https://92.push2his.eastmoney.com/api/qt/stock/kline/get",
        "https://push2.eastmoney.com/api/qt/stock/kline/get",
    ]
    last_err: Exception | None = None
    for url in hosts:
        try:
            r = session.get(url, params=params, headers=headers, timeout=8)
            r.raise_for_status()
            data = r.json()
            klines = ((data.get("data") or {}).get("klines")) or []
            rows: list[dict[str, Any]] = []
            for line in klines:
                if not isinstance(line, str):
                    continue
                parts = line.split(",")
                if len(parts) < 7:
                    continue
                day = parts[0].strip()
                amt = _num(parts[6])
                if not day or amt is None or float(amt) <= 0:
                    continue
                rows.append({"day": day, "amount": float(amt), "close": _num(parts[2])})
            if rows:
                return rows
        except Exception as e:
            last_err = e
            continue
    if last_err:
        log.warning("em daily kline %s failed: %s", secid, last_err)
    return []


def _em_prev_day_amounts(prev_day: Optional[str] = None, today: Optional[str] = None) -> dict[str, Any]:
    """Previous complete trading-day SH+SZ amounts via Eastmoney daily kline.

    This is more reliable than yixin historical NL for 深证成指, which can under-report
    full-market turnover on some days.
    """
    today = today or datetime.now(TZ_SH).strftime("%Y-%m-%d")
    cache_key = f"em_prev_kline:{today}:{prev_day or ''}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    sh_rows = _em_daily_kline_rows("1.000001", lmt=12)
    sz_rows = _em_daily_kline_rows("0.399001", lmt=12)
    if not sh_rows or not sz_rows:
        out = {"ok": False, "source": "eastmoney_kline", "error": "empty_kline"}
        _cache_set(cache_key, out, ttl=45.0, stale=180.0)
        return out

    sh_map = {r["day"]: r["amount"] for r in sh_rows}
    sz_map = {r["day"]: r["amount"] for r in sz_rows}
    days = sorted(set(sh_map) & set(sz_map))
    # pick requested prev_day if both have it and it is before today; else latest day < today
    pick = None
    if prev_day and prev_day in sh_map and prev_day in sz_map and prev_day < today:
        pick = prev_day
    else:
        cands = [d for d in days if d < today]
        pick = cands[-1] if cands else None
    if not pick:
        out = {"ok": False, "source": "eastmoney_kline", "error": "no_prev_day"}
        _cache_set(cache_key, out, ttl=45.0, stale=180.0)
        return out

    sh = float(sh_map[pick])
    sz = float(sz_map[pick])
    # sanity: each side should be multi-thousand 亿 on normal sessions
    if sh < 1e11 or sz < 1e11:
        out = {
            "ok": False,
            "source": "eastmoney_kline",
            "error": "amount_too_small",
            "prev_day": pick,
            "prev_sh": sh,
            "prev_sz": sz,
        }
        _cache_set(cache_key, out, ttl=45.0, stale=180.0)
        return out

    out = {
        "ok": True,
        "source": "eastmoney_kline",
        "prev_day": pick,
        "prev_sh": sh,
        "prev_sz": sz,
        "prev_hs": sh + sz,
    }
    _cache_set(cache_key, out, ttl=1800.0, stale=7200.0)
    return out


def _sohu_daily_amount_map(code: str, start: str, end: str) -> dict[str, float]:
    """Sohu index daily bars. code like zs_000001 / zs_399001. amount unit: 万元."""
    session = requests.Session()
    session.trust_env = False
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://q.stock.sohu.com/",
        "Accept": "*/*",
        "Connection": "close",
    }
    url = "https://q.stock.sohu.com/hisHq"
    params = {
        "code": code,
        "start": start.replace("-", ""),
        "end": end.replace("-", ""),
        "stat": 1,
        "order": "D",
        "period": "d",
    }
    try:
        r = session.get(url, params=params, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("sohu daily %s failed: %s", code, e)
        return {}
    rows = []
    if isinstance(data, list) and data:
        rows = (data[0] or {}).get("hq") or []
    elif isinstance(data, dict):
        rows = data.get("hq") or []
    out: dict[str, float] = {}
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 9:
            continue
        day = str(row[0]).strip()
        # sohu: [date, open, close, chg, chg%, low, high, volume, amount_wan, ...]
        amt_wan = _num(row[8])
        if not day or amt_wan is None or float(amt_wan) <= 0:
            continue
        # 万元 -> 元
        out[day] = float(amt_wan) * 10000.0
    return out


def _sohu_prev_day_amounts(prev_day: Optional[str] = None, today: Optional[str] = None) -> dict[str, Any]:
    """Previous complete SH+SZ amounts via Sohu daily history (server-reachable backup)."""
    today = today or datetime.now(TZ_SH).strftime("%Y-%m-%d")
    cache_key = f"sohu_prev:{today}:{prev_day or ''}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        end = today.replace("-", "")
        # look back ~3 weeks of calendar days
        start_dt = datetime.strptime(today, "%Y-%m-%d") - timedelta(days=21)
        start = start_dt.strftime("%Y%m%d")
    except Exception:
        start, end = "20260101", today.replace("-", "")
    sh_map = _sohu_daily_amount_map("zs_000001", start, end)
    sz_map = _sohu_daily_amount_map("zs_399001", start, end)
    if not sh_map or not sz_map:
        out = {"ok": False, "source": "sohu", "error": "empty"}
        _cache_set(cache_key, out, ttl=60.0, stale=300.0)
        return out
    days = sorted(set(sh_map) & set(sz_map))
    pick = None
    if prev_day and prev_day in sh_map and prev_day in sz_map and prev_day < today:
        pick = prev_day
    else:
        cands = [d for d in days if d < today]
        pick = cands[-1] if cands else None
    if not pick:
        out = {"ok": False, "source": "sohu", "error": "no_prev_day"}
        _cache_set(cache_key, out, ttl=60.0, stale=300.0)
        return out
    sh = float(sh_map[pick])
    sz = float(sz_map[pick])
    if sh < 1e11 or sz < 1e11:
        out = {
            "ok": False,
            "source": "sohu",
            "error": "amount_too_small",
            "prev_day": pick,
            "prev_sh": sh,
            "prev_sz": sz,
        }
        _cache_set(cache_key, out, ttl=60.0, stale=300.0)
        return out
    out = {
        "ok": True,
        "source": "sohu",
        "prev_day": pick,
        "prev_sh": sh,
        "prev_sz": sz,
        "prev_hs": sh + sz,
    }
    _cache_set(cache_key, out, ttl=1800.0, stale=7200.0)
    return out


def _tencent_daily_amount_map(code: str, lmt: int = 15) -> dict[str, float]:
    """Tencent newfq day bars. code: sh000001 / sz399001. amount unit in API: 万元."""
    session = requests.Session()
    session.trust_env = False
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://finance.qq.com/",
        "Accept": "*/*",
        "Connection": "close",
    }
    urls = [
        f"https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get?param={code},day,,,{int(lmt)},qfq",
        f"https://web.ifzq.gtimg.cn/appstock/app/newfqkline/get?param={code},day,,,{int(lmt)},qfq",
    ]
    last_err: Exception | None = None
    for url in urls:
        try:
            r = session.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            data = r.json()
            rows = ((data.get("data") or {}).get(code) or {}).get("day") or []
            out: dict[str, float] = {}
            for row in rows:
                if not isinstance(row, (list, tuple)) or len(row) < 9:
                    continue
                day = str(row[0]).strip()
                amt_wan = _num(row[8])
                if not day or amt_wan is None or float(amt_wan) <= 0:
                    continue
                out[day] = float(amt_wan) * 10000.0  # 万元 -> 元
            if out:
                return out
        except Exception as e:
            last_err = e
            continue
    if last_err:
        log.warning("tencent daily %s failed: %s", code, last_err)
    return {}


def _tencent_prev_day_amounts(prev_day: Optional[str] = None, today: Optional[str] = None) -> dict[str, Any]:
    """Previous complete SH+SZ(+BJ) amounts via Tencent day kline.

    BJ uses 北证50(bj899050) amount as the common media proxy for 京市/沪深京合计.
    """
    today = today or datetime.now(TZ_SH).strftime("%Y-%m-%d")
    cache_key = f"tencent_prev:{today}:{prev_day or ''}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    sh_map = _tencent_daily_amount_map("sh000001", lmt=15)
    sz_map = _tencent_daily_amount_map("sz399001", lmt=15)
    bj_map = _tencent_daily_amount_map("bj899050", lmt=15)
    if not sh_map or not sz_map:
        out = {"ok": False, "source": "tencent", "error": "empty"}
        _cache_set(cache_key, out, ttl=60.0, stale=300.0)
        return out
    days = sorted(set(sh_map) & set(sz_map))
    pick = None
    if prev_day and prev_day in sh_map and prev_day in sz_map and prev_day < today:
        pick = prev_day
    else:
        cands = [d for d in days if d < today]
        pick = cands[-1] if cands else None
    if not pick:
        out = {"ok": False, "source": "tencent", "error": "no_prev_day"}
        _cache_set(cache_key, out, ttl=60.0, stale=300.0)
        return out
    sh = float(sh_map[pick])
    sz = float(sz_map[pick])
    bj = float(bj_map[pick]) if pick in bj_map else None
    if sh < 1e11 or sz < 1e11:
        out = {
            "ok": False,
            "source": "tencent",
            "error": "amount_too_small",
            "prev_day": pick,
            "prev_sh": sh,
            "prev_sz": sz,
            "prev_bj": bj,
        }
        _cache_set(cache_key, out, ttl=60.0, stale=300.0)
        return out
    out = {
        "ok": True,
        "source": "tencent",
        "prev_day": pick,
        "prev_sh": sh,
        "prev_sz": sz,
        "prev_bj": bj,
        "prev_hs": sh + sz,
        "prev_total": (sh + sz + float(bj)) if bj is not None and bj > 0 else (sh + sz),
    }
    _cache_set(cache_key, out, ttl=1800.0, stale=7200.0)
    return out


def _tencent_prev_bj_amount(prev_day: Optional[str] = None, today: Optional[str] = None) -> Optional[float]:
    """Previous-day BJ amount via 北证50 Tencent day bar (yuan)."""
    today = today or datetime.now(TZ_SH).strftime("%Y-%m-%d")
    cache_key = f"tencent_prev_bj:{today}:{prev_day or ''}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached.get("prev_bj") if isinstance(cached, dict) else None
    bj_map = _tencent_daily_amount_map("bj899050", lmt=15)
    pick = None
    if prev_day and prev_day in bj_map and prev_day < today:
        pick = prev_day
    else:
        cands = sorted(d for d in bj_map if d < today)
        pick = cands[-1] if cands else None
    bj = float(bj_map[pick]) if pick and pick in bj_map else None
    if bj is not None and bj <= 0:
        bj = None
    out = {"prev_day": pick, "prev_bj": bj}
    _cache_set(cache_key, out, ttl=1800.0, stale=7200.0)
    return bj


def _tencent_index_amounts() -> dict[str, Any]:
    """Realtime SH/SZ(/CYB) amounts via Tencent qt. Returns yuan."""
    cache_key = "tencent_index_amounts"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    session = requests.Session()
    session.trust_env = False
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://finance.qq.com/",
        "Accept": "*/*",
        "Connection": "close",
    }
    # include more symbols for backup fill
    codes = "sh000001,sz399001,sz399006,sh000688,sh000680,bj899050"
    urls = [
        f"https://qt.gtimg.cn/q={codes}",
        f"https://web.sqt.gtimg.cn/q={codes}",
    ]
    text = ""
    last_err: Exception | None = None
    for url in urls:
        try:
            r = session.get(url, headers=headers, timeout=8)
            r.raise_for_status()
            text = r.content.decode("gbk", errors="replace")
            if "v_" in text:
                break
        except Exception as e:
            last_err = e
            continue
    if "v_" not in text:
        if last_err:
            log.warning("tencent qt failed: %s", last_err)
        out = {"ok": False, "source": "tencent", "error": "empty"}
        _cache_set(cache_key, out, ttl=20.0, stale=120.0)
        return out

    out: dict[str, Any] = {
        "ok": True,
        "source": "tencent",
        "sh": None,
        "sz": None,
        "bj": None,
        "cyb": None,
        "kc50": None,
    }
    # v_sh000001="1~上证指数~000001~price~prev~open~vol~...~amount_path~...";
    for m in re.finditer(r'v_([a-z]{2})(\d{6})="([^"]*)"', text):
        mkt, code6, body = m.group(1), m.group(2), m.group(3)
        parts = body.split("~")
        if len(parts) < 7:
            continue
        name = parts[1] if len(parts) > 1 else code6
        price = _num(parts[3]) if len(parts) > 3 else None
        chg = _num(parts[32]) if len(parts) > 32 else None  # percent often at 32
        # amount: prefer explicit yuan-like field; fallback 万元 field ~36/37 style
        amt = None
        # common: "price/vol/amount" packed in parts[35] style, and parts[37] 万元
        for idx in (37, 36, 35, 6):
            if idx < len(parts):
                raw = parts[idx]
                if not raw:
                    continue
                # packed like 3784.55/362473851/672042126229
                if "/" in str(raw):
                    segs = str(raw).split("/")
                    if len(segs) >= 3:
                        cand = _num(segs[2])
                        if cand is not None and cand > 1e11:
                            amt = float(cand)
                            break
                cand = _num(raw)
                if cand is None:
                    continue
                # if looks like 万元 scale for full market index
                if 1e6 < cand < 5e8:
                    amt = float(cand) * 10000.0
                    break
                if cand > 1e11:
                    amt = float(cand)
                    break
        if amt is None:
            continue
        item = {"amount": amt, "price": price, "change_pct": chg, "name": name, "code": code6}
        if code6 == "000001" and mkt == "sh":
            out["sh"] = item
        elif code6 == "399001":
            out["sz"] = item
        elif code6 == "399006":
            out["cyb"] = item
        elif code6 == "000688":
            out["kc50"] = item
        elif code6 == "899050":
            out["bj"] = item
    if not any(out.get(k) for k in ("sh", "sz", "cyb", "kc50", "bj")):
        out = {"ok": False, "source": "tencent", "error": "parse_fail"}
        _cache_set(cache_key, out, ttl=20.0, stale=120.0)
        return out
    _cache_set(cache_key, out, ttl=25.0, stale=180.0)
    return out


def _sina_index_amounts() -> dict[str, Any]:
    """Realtime SH/SZ amounts via Sina hq. Returns yuan."""
    cache_key = "sina_index_amounts"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    session = requests.Session()
    session.trust_env = False
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://finance.sina.com.cn",
        "Accept": "*/*",
        "Connection": "close",
    }
    # full quote lines carry amount in yuan; s_ compact lines carry amount in 万元
    lst = "sh000001,sz399001,sz399006,sh000688,bj899050,s_sh000001,s_sz399001,s_sz399006,s_sh000688"
    urls = [
        f"https://hq.sinajs.cn/list={lst}",
        f"https://hq.sinajs.cn/?list={lst}",
    ]
    text = ""
    last_err: Exception | None = None
    for url in urls:
        try:
            r = session.get(url, headers=headers, timeout=8)
            r.raise_for_status()
            text = r.content.decode("gbk", errors="replace")
            if "hq_str_" in text:
                break
        except Exception as e:
            last_err = e
            continue
    if "hq_str_" not in text:
        if last_err:
            log.warning("sina hq failed: %s", last_err)
        out = {"ok": False, "source": "sina", "error": "empty"}
        _cache_set(cache_key, out, ttl=20.0, stale=120.0)
        return out

    out: dict[str, Any] = {
        "ok": True,
        "source": "sina",
        "sh": None,
        "sz": None,
        "bj": None,
        "cyb": None,
        "kc50": None,
    }
    for line in text.splitlines():
        m = re.search(r'hq_str_((?:s_)?(?:sh|sz|bj)(\d{6}))="([^"]*)"', line)
        if not m:
            continue
        key, code6, body = m.group(1), m.group(2), m.group(3)
        if not body:
            continue
        parts = body.split(",")
        item = None
        if key.startswith("s_"):
            # name, price, change, pct, volume, amount_wan
            if len(parts) < 6:
                continue
            price = _num(parts[1])
            chg = _num(parts[3])
            amt_wan = _num(parts[5])
            if amt_wan is None or float(amt_wan) <= 0:
                continue
            item = {
                "amount": float(amt_wan) * 10000.0,
                "price": price,
                "change_pct": chg,
                "name": parts[0],
                "code": code6,
            }
        else:
            # standard: name, open, prev, price, high, low, ..., vol(idx8), amount(idx9)
            if len(parts) < 10:
                continue
            price = _num(parts[3])
            prev = _num(parts[2])
            chg = None
            if price is not None and prev not in (None, 0):
                try:
                    chg = (float(price) / float(prev) - 1.0) * 100.0
                except Exception:
                    chg = None
            amt = _num(parts[9])
            if amt is None or float(amt) <= 0:
                continue
            # if amount looks like 万元, scale
            if 1e6 < float(amt) < 5e8:
                amt = float(amt) * 10000.0
            item = {
                "amount": float(amt),
                "price": price,
                "change_pct": chg,
                "name": parts[0],
                "code": code6,
            }
        if not item:
            continue
        # prefer full-quote amount over compact if both exist
        slot = None
        if code6 == "000001" and "sh" in key:
            slot = "sh"
        elif code6 == "399001":
            slot = "sz"
        elif code6 == "399006":
            slot = "cyb"
        elif code6 == "000688":
            slot = "kc50"
        elif code6 == "899050":
            slot = "bj"
        if not slot:
            continue
        old = out.get(slot)
        if old and old.get("amount") and float(old["amount"]) >= float(item["amount"]):
            # keep larger/more complete amount
            if not key.startswith("s_"):
                out[slot] = item
            continue
        out[slot] = item

    if not any(out.get(k) for k in ("sh", "sz", "cyb", "kc50", "bj")):
        out = {"ok": False, "source": "sina", "error": "parse_fail"}
        _cache_set(cache_key, out, ttl=20.0, stale=120.0)
        return out
    _cache_set(cache_key, out, ttl=25.0, stale=180.0)
    return out



def _tencent_index_minute_by_day(code: str) -> dict[str, list[tuple[str, float]]]:
    """Tencent multi-day 1-min path with amount.

    code: sh000001 / sz399001
    Returns: day(YYYY-MM-DD) -> [(HH:MM, per_minute_amount_yuan)]
    Upstream amount is cumulative yuan; we convert to per-minute deltas to match EM trends shape.
    """
    cache_key = f"tencent_minute_by_day:{code}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    session = requests.Session()
    session.trust_env = False
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://finance.qq.com/",
        "Accept": "*/*",
        "Connection": "close",
    }
    urls = [
        f"https://web.ifzq.gtimg.cn/appstock/app/day/query?code={code}",
        f"https://ifzq.gtimg.cn/appstock/app/day/query?code={code}",
        f"https://proxy.finance.qq.com/ifzqgtimg/appstock/app/day/query?code={code}",
        # today-only fallback
        f"https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={code}",
        f"https://ifzq.gtimg.cn/appstock/app/minute/query?code={code}",
    ]
    last_err: Exception | None = None
    payload = None
    used = None
    for url in urls:
        try:
            r = session.get(url, headers=headers, timeout=12)
            r.raise_for_status()
            data = r.json()
            node = ((data.get("data") or {}).get(code) or {})
            if not isinstance(node, dict):
                continue
            # day/query: data = [{date, data:[...]}, ...]
            days = node.get("data")
            if isinstance(days, list) and days and isinstance(days[0], dict) and "date" in days[0]:
                payload = days
                used = "day_query"
                break
            # minute/query: data.data = ["0930 price vol amount", ...]
            inner = days if isinstance(days, dict) else None
            if isinstance(inner, dict) and isinstance(inner.get("data"), list) and inner.get("data"):
                today = datetime.now(TZ_SH).strftime("%Y%m%d")
                payload = [{"date": today, "data": inner.get("data")}]
                used = "minute_query"
                break
        except Exception as e:
            last_err = e
            continue
    out: dict[str, list[tuple[str, float]]] = {}
    if not payload:
        if last_err:
            log.warning("tencent minute %s failed: %s", code, last_err)
        _cache_set(cache_key, out, ttl=45.0, stale=180.0)
        return out

    for item in payload:
        if not isinstance(item, dict):
            continue
        d_raw = str(item.get("date") or "").strip()
        digits = re.sub(r"\D", "", d_raw)
        if len(digits) < 8:
            continue
        day = f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
        rows = item.get("data") or []
        pts: list[tuple[str, float]] = []
        prev_cum = 0.0
        for row in rows:
            parts = str(row).split()
            if len(parts) < 4:
                continue
            tm = _hhmm_from_any(parts[0])
            amt = _num(parts[3])
            if amt is None:
                continue
            cum = float(amt)
            # defensive: some feeds might already be per-bar if very small vs market
            if cum >= prev_cum:
                delta = cum - prev_cum
                prev_cum = cum
            else:
                # reset / non-monotonic: treat as per-minute
                delta = cum
                prev_cum = prev_cum + cum
            if delta < 0:
                delta = 0.0
            pts.append((tm, float(delta)))
        if pts:
            out[day] = pts
    ttl = 120.0 if out else 45.0
    _cache_set(cache_key, out, ttl=ttl, stale=max(ttl, 300.0))
    if out:
        log.info("tencent minute %s ok source=%s days=%s", code, used, list(out.keys()))
    return out


def _merge_minute_by_day(
    primary: dict[str, list[tuple[str, float]]],
    backup: dict[str, list[tuple[str, float]]],
) -> dict[str, list[tuple[str, float]]]:
    """Fill missing/short days from backup; never overwrite a healthier primary day."""
    if not backup:
        return primary
    if not primary:
        return dict(backup)
    out = dict(primary)
    for day, pts in backup.items():
        cur = out.get(day) or []
        if len(pts) > len(cur):
            out[day] = pts
    return out


def _avg_cum_ratio_curve(
    by_day: dict[str, list[tuple[str, float]]],
    exclude_day: str,
    max_days: int = 3,
) -> dict[int, float]:
    """Average cumulative volume share by session minute index (completed hist days).

    Prefer the most recent ``max_days`` complete sessions (default 3), aligning with
    the 3-day same-time baseline used by common market-capacity predictors:
        predict = cum_today / mean(cum_hist / full_hist)
    """
    # newest complete days first
    complete_days: list[str] = []
    for day, pts in sorted(by_day.items(), reverse=True):
        if day == exclude_day:
            continue
        if len(pts) < 200:
            continue
        total = sum(a for _, a in pts)
        if total > 0:
            complete_days.append(day)
        if max_days and len(complete_days) >= int(max_days):
            break
    use_set = set(complete_days) if complete_days else None

    buckets: dict[int, list[float]] = {}
    for day, pts in by_day.items():
        if day == exclude_day:
            continue
        if use_set is not None and day not in use_set:
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


def _abs_3d_baseline(
    by_day: dict[str, list[tuple[str, float]]],
    exclude_day: str,
    max_days: int = 3,
) -> tuple[dict[int, float], Optional[float], int]:
    """Absolute 3-day same-time cumulative baseline.

    Returns (s3_cum_by_idx, s3_full, n_days) in yuan.
    Formula mirror:
        predict = today_cum * s3_full / s3_cum[now_idx]
    """
    day_cums: list[tuple[str, dict[int, float], float]] = []
    for day, pts in sorted(by_day.items(), reverse=True):
        if day == exclude_day:
            continue
        if len(pts) < 200:
            continue
        cum_map = _minute_cum_from_pts(pts)
        if len(cum_map) < 180:
            continue
        full = float(max(cum_map.values())) if cum_map else 0.0
        if full <= 0:
            continue
        day_cums.append((day, cum_map, full))
        if len(day_cums) >= int(max_days):
            break
    if not day_cums:
        return {}, None, 0
    all_idx = set()
    for _, cm, _ in day_cums:
        all_idx |= set(cm.keys())
    s3_cum: dict[int, float] = {}
    for i in sorted(all_idx):
        vals = []
        for _, cm, _ in day_cums:
            prev = [k for k in cm if k <= i]
            if prev:
                vals.append(float(cm[max(prev)]))
        if vals:
            s3_cum[i] = sum(vals) / len(vals)
    s3_full = sum(f for _, _, f in day_cums) / len(day_cums)
    return s3_cum, float(s3_full), len(day_cums)


def _predict_by_abs_3d(
    amount: Optional[float],
    hhmm: str,
    s3_cum: dict[int, float],
    s3_full: Optional[float],
    progress: float,
) -> tuple[Optional[float], str, Optional[float]]:
    """predict = amount * s3_full / s3_cum[t]  (3-day absolute same-time)."""
    if amount is None or s3_full is None or s3_full <= 0 or not s3_cum:
        return None, "none", None
    if progress >= 0.995:
        return float(amount), "closed", 1.0
    idx = _hhmm_to_session_idx(hhmm)
    if idx is None:
        return None, "unavailable", None
    prev = [k for k in s3_cum if k <= idx]
    if not prev:
        return None, "unavailable", None
    base = float(s3_cum[max(prev)])
    if base <= 0:
        return None, "unavailable", None
    ratio = base / float(s3_full)
    # Allow very early open (09:30 cum share can be < 1%); only guard zero/noise.
    if ratio < 0.002:
        return None, "unavailable", None
    ratio = max(0.002, min(0.999, ratio))
    return float(amount) / ratio, "profile_3d_same_time", ratio


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


def _minute_cum_from_pts(pts: list[tuple[str, float]]) -> dict[int, float]:
    """Build session-minute cumulative amount path from per-minute points."""
    if not pts:
        return {}
    per: dict[int, float] = {}
    for tm, a in pts:
        idx = _hhmm_to_session_idx(tm)
        if idx is None:
            continue
        try:
            per[idx] = per.get(idx, 0.0) + float(a)
        except (TypeError, ValueError):
            continue
    if not per:
        return {}
    cum: dict[int, float] = {}
    s = 0.0
    for i in range(0, max(per.keys()) + 1):
        s += per.get(i, 0.0)
        cum[i] = s
    return cum


def _scale_today_cum(today_cum: dict[int, float], amount_now: Optional[float], hhmm: str) -> dict[int, float]:
    """Align path end to live quote amount (EM minute sum vs f6 can diverge)."""
    if not today_cum or amount_now is None or amount_now <= 0:
        return today_cum
    idx = _hhmm_to_session_idx(hhmm)
    if idx is None:
        return today_cum
    # use latest available cum <= idx
    prev = [i for i in today_cum if i <= idx]
    if not prev:
        return today_cum
    i0 = max(prev)
    base = float(today_cum.get(i0) or 0.0)
    if base <= 0:
        return today_cum
    k = float(amount_now) / base
    # only rescale if moderately off (data glitch guard)
    if k < 0.75 or k > 1.35:
        return today_cum
    return {i: float(v) * k for i, v in today_cum.items()}


def _predict_with_self_cal(
    amount: Optional[float],
    hhmm: str,
    hist_curve: dict[int, float],
    today_cum: dict[int, float],
    progress: float,
    method_label: str = "profile",
) -> tuple[Optional[float], str, Optional[float], dict[str, Any]]:
    """Hist profile prediction blended with today's own path fit.

    Fit full-day scale S so today_cum[i] ≈ S * hist_ratio[i] over the morning path.
    This auto-corrects when today is more/less front-loaded than the reference curve
    (e.g. risk-off dump days), without linear time fallback.
    """
    meta: dict[str, Any] = {"self_cal": False}
    pred_hist, method_hist, r_hist = _predict_by_profile(
        amount, hhmm, hist_curve, progress, method_label=method_label
    )
    if amount is None:
        return None, "none", None, meta
    if progress >= 0.99:
        return float(amount), "closed", 1.0, {**meta, "self_cal": False, "reason": "closed"}

    now_idx = _hhmm_to_session_idx(hhmm)
    if now_idx is None or not hist_curve:
        return pred_hist, method_hist, r_hist, {**meta, "reason": "no_idx_or_curve"}

    path = _scale_today_cum(today_cum or {}, amount, hhmm)
    if not path:
        return pred_hist, method_hist, r_hist, {**meta, "reason": "no_today_path"}

    pairs: list[tuple[float, float, int]] = []
    max_i = min(int(now_idx), max(path.keys()))
    for i in range(0, max_i + 1):
        r = hist_curve.get(i)
        c = path.get(i)
        if r is None or c is None:
            continue
        if float(r) < 0.08 or float(c) <= 0:
            continue
        pairs.append((float(r), float(c), i))

    if len(pairs) < 12:
        return pred_hist, method_hist, r_hist, {**meta, "reason": "few_points", "n": len(pairs)}

    # Recent-weighted average of implied full-day scales S_i = cum_i / hist_ratio_i.
    # Half-life shortens into the afternoon so stale morning S_i stop dominating.
    half_life = 28.0 - 16.0 * min(1.0, max(0.0, (float(progress) - 0.25) / 0.65))
    half_life = max(10.0, min(30.0, half_life))
    num = den = 0.0
    latest_S = None
    for r, c, i in pairs:
        Si = c / r
        w = 2.718281828 ** ((i - max_i) / half_life)
        num += w * Si
        den += w
        latest_S = Si
    if den <= 0:
        return pred_hist, method_hist, r_hist, {**meta, "reason": "bad_fit"}
    S_avg = num / den
    # Prefer latest implied scale more as the day progresses (0.35 -> 0.85).
    latest_w = 0.35 + 0.50 * min(1.0, max(0.0, (float(progress) - 0.25) / 0.65))
    if latest_S is not None and latest_S > 0:
        S = (1.0 - latest_w) * S_avg + latest_w * float(latest_S)
    else:
        S = S_avg
    if S <= 0:
        return pred_hist, method_hist, r_hist, {**meta, "reason": "bad_S"}

    # tighter clamp vs hist later in the day
    if pred_hist is not None and pred_hist > 0:
        band = 0.28 - 0.14 * min(1.0, max(0.0, (float(progress) - 0.40) / 0.50))
        band = max(0.10, min(0.28, band))
        S = max(float(pred_hist) * (1.0 - band), min(float(pred_hist) * (1.0 + band), S))

    # Self-cal ramps after ~24 trading minutes, peaks midday (~55%), then fades
    # to 0 by ~progress 0.90 so late-day comparisons stay on hist profile.
    w_cal = 0.0
    if 0.10 <= float(progress) < 0.55:
        w_cal = min(0.55, max(0.0, (float(progress) - 0.10) / 0.45 * 0.55))
    elif float(progress) >= 0.55:
        w_cal = max(0.0, 0.55 * (0.90 - float(progress)) / 0.35)

    # if hist ratio already very complete, almost no residual room for self-cal
    if r_hist is not None and float(r_hist) >= 0.92:
        w_cal *= max(0.0, min(1.0, (0.995 - float(r_hist)) / 0.075))

    if pred_hist is None:
        pred = float(S)
        method = "profile_selfcal"
    else:
        pred = (1.0 - w_cal) * float(pred_hist) + w_cal * float(S)
        method = "profile_selfcal" if w_cal >= 0.12 else method_hist

    # never predict below already-traded amount; bound residual near close
    pred = max(float(amount), float(pred))
    if r_hist is not None and float(r_hist) >= 0.90 and pred_hist is not None:
        hist_resid = max(0.0, float(pred_hist) - float(amount))
        pred = min(float(pred), float(amount) + 1.5 * hist_resid)

    ratio = (float(amount) / pred) if pred and pred > 0 else r_hist
    if ratio is not None:
        ratio = max(0.06, min(0.995, float(ratio)))

    meta = {
        "self_cal": bool(w_cal >= 0.05),
        "weight": round(w_cal, 3),
        "pred_hist": pred_hist,
        "pred_path": S,
        "fit_points": len(pairs),
        "now_idx": now_idx,
        "half_life": round(half_life, 1),
        "latest_w": round(latest_w, 3),
    }
    return float(pred), method, ratio, meta


def _builtin_session_curve() -> dict[int, float]:
    """Typical A-share cumulative volume share by session minute (0..239).

    Calibrated open-heavy profile (~61.5% by 11:15, ~65.5% by lunch). Old density
    model was nearly linear and systematically over-predicted morning full-day volume.
    """
    anchors: list[tuple[int, float]] = [
        (0, 0.015),
        (15, 0.165),
        (30, 0.285),
        (60, 0.445),
        (90, 0.555),
        (105, 0.615),
        (120, 0.655),
        (150, 0.735),
        (180, 0.835),
        (210, 0.915),
        (225, 0.965),
        (239, 1.0),
    ]
    curve: dict[int, float] = {}
    for i in range(240):
        for j in range(1, len(anchors)):
            i0, r0 = anchors[j - 1]
            i1, r1 = anchors[j]
            if i <= i1 or j == len(anchors) - 1:
                if i1 == i0:
                    curve[i] = r1
                else:
                    t = max(0.0, min(1.0, (i - i0) / float(i1 - i0)))
                    curve[i] = r0 + (r1 - r0) * t
                break
    prev = 0.0
    for i in range(240):
        prev = max(prev, float(curve.get(i, prev)))
        curve[i] = min(1.0, prev)
    curve[0] = max(curve.get(0, 0.01), 0.01)
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
            "prev_bj": payload.get("prev_bj"),
            "prev_hs": payload.get("prev_hs"),
            "prev_total": payload.get("prev_total"),
            "prev_source": payload.get("prev_source"),
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
            "prev_bj": raw.get("prev_bj"),
            "prev_hs": raw.get("prev_hs"),
            "prev_total": raw.get("prev_total"),
            "prev_source": raw.get("prev_source") or "disk_cache",
            "profile_source": "disk_cache",
            "saved_at": raw.get("saved_at"),
        }
    except Exception as e:
        log.warning("load vol profile disk failed: %s", e)
        return None



def _yixin_api_key() -> str:
    return (os.environ.get("YIXIN_API_KEY") or os.environ.get("FIN_API_KEY") or "").strip()


def _yixin_fin_db(query: str, timeout: int = 28) -> dict[str, Any]:
    key = _yixin_api_key()
    if not key:
        raise RuntimeError("YIXIN_API_KEY not configured")
    payload = {"query": query, "data_sources": ["auto"]}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-API-KEY": key,
    }
    session = requests.Session()
    session.trust_env = False
    r = session.post(YIXIN_API_URL, json=payload, headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError("yixin response not dict")
    return data


def _yixin_result_content(data: dict[str, Any]) -> str:
    res = data.get("result")
    if isinstance(res, list) and res:
        item = res[0] or {}
        if isinstance(item, dict):
            return str(item.get("content") or "")
    if isinstance(res, dict):
        return str(res.get("content") or "")
    return str(data.get("content") or "")


def _parse_markdown_amount_table(content: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not content:
        return rows
    for line in content.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        parts = [p.strip() for p in line.strip("|").split("|")]
        if len(parts) < 3:
            continue
        head = parts[0]
        if ("标的" in head) or head.startswith("---") or head == "时间":
            continue
        name = head
        m2 = re.search(r"(\d{6})", head)
        code6 = m2.group(1) if m2 else ""
        nums: list[float] = []
        for p in parts[1:]:
            s = p.replace(",", "").replace("%", "").strip()
            try:
                nums.append(float(s))
            except ValueError:
                continue
        if not nums:
            continue
        amt_cands = [n for n in nums if n >= 1e9]
        amount = amt_cands[-1] if amt_cands else None
        if amount is None:
            yi_cands = [n for n in nums if 10 < n < 50000]
            if yi_cands:
                amount = yi_cands[-1] * 1e8
        price = nums[0] if nums else None
        chg = nums[2] if len(nums) >= 3 and abs(nums[2]) < 30 else None
        rows.append({"name": name, "code": code6, "amount": amount, "price": price, "change_pct": chg})
    return rows


def _yixin_index_amounts() -> dict[str, Any]:
    cache_key = "yixin_index_amounts"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    if not _yixin_api_key():
        return {"ok": False, "error": "no_api_key"}
    try:
        data = _yixin_fin_db("上证指数、深证成指、创业板指 最新价 涨跌幅 成交额", timeout=28)
        content = _yixin_result_content(data)
        parsed = _parse_markdown_amount_table(content)
        out: dict[str, Any] = {"ok": False, "source": "yixin", "sh": None, "sz": None, "cyb": None, "raw_count": len(parsed)}
        for row in parsed:
            name = str(row.get("name") or "")
            code = str(row.get("code") or "")
            item = {"amount": row.get("amount"), "change_pct": row.get("change_pct"), "price": row.get("price"), "name": name, "code": code}
            if "000001" in code or "上证" in name:
                out["sh"] = item
            elif "399001" in code or "深证成" in name:
                out["sz"] = item
            elif "399006" in code or "创业板" in name:
                out["cyb"] = item
        out["ok"] = bool((out.get("sh") or {}).get("amount") or (out.get("sz") or {}).get("amount"))
        if out["ok"]:
            _cache_set(cache_key, out, ttl=YIXIN_AMOUNT_TTL, stale=300.0)
        return out
    except Exception as e:
        log.warning("yixin index amounts failed: %s", e)
        stale = _cache_stale_payload(cache_key)
        if stale:
            return stale
        return {"ok": False, "error": str(e)}


def _yixin_extract_index_amount(content: str, *, prefer: str) -> Optional[float]:
    """Parse one index amount from yixin markdown/text. prefer: sh|sz."""
    prefer = (prefer or "").lower()
    for row in _parse_markdown_amount_table(content):
        name = str(row.get("name") or "")
        code = str(row.get("code") or "")
        amt = row.get("amount")
        if amt is None:
            continue
        if prefer == "sh" and ("000001" in code or "上证" in name):
            return float(amt)
        if prefer == "sz" and ("399001" in code or "深证成" in name or "深成" in name):
            return float(amt)
    labels = (("上证",) if prefer == "sh" else ("深证成", "深成", "深证"))
    for label in labels:
        m = re.search(label + r"[^\d]{0,24}(\d+(?:\.\d+)?)\s*亿", content)
        if m:
            return float(m.group(1)) * 1e8
        m2 = re.search(label + r"[^\d]{0,40}(\d{11,})", content)
        if m2:
            return float(m2.group(1))
    # single-table fallback: only one large amount in content
    amts = []
    for row in _parse_markdown_amount_table(content):
        if row.get("amount") is not None:
            amts.append(float(row["amount"]))
    if len(amts) == 1 and amts[0] > 1e11:
        return amts[0]
    return None


def _yixin_prev_day_amounts(prev_day: Optional[str]) -> dict[str, Any]:
    if not prev_day or not _yixin_api_key():
        return {"ok": False}
    cache_key = f"yixin_prev:{prev_day}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        # Separate queries are much more reliable than a joint NL query.
        day = str(prev_day).strip()
        day_alt = day.replace("-", "")
        sh = sz = None
        contents: list[str] = []
        for q in (
            f"上证指数 {day} 成交额",
            f"深证成指 {day} 成交额",
            f"上证指数 {day_alt} 成交额",
            f"深证成指 {day_alt} 成交额",
        ):
            try:
                data = _yixin_fin_db(q, timeout=28)
                content = _yixin_result_content(data)
            except Exception as e:
                log.warning("yixin prev query failed %s: %s", q, e)
                continue
            if not content:
                continue
            contents.append(content)
            if sh is None and ("上证" in q):
                sh = _yixin_extract_index_amount(content, prefer="sh")
            if sz is None and ("深证" in q or "深成" in q):
                sz = _yixin_extract_index_amount(content, prefer="sz")
            if sh is not None and sz is not None:
                break
        if sh is None or sz is None:
            # last resort: one joint query
            try:
                data = _yixin_fin_db(f"上证指数和深证成指 {day} 全天成交额", timeout=30)
                content = _yixin_result_content(data)
                contents.append(content)
                if sh is None:
                    sh = _yixin_extract_index_amount(content, prefer="sh")
                if sz is None:
                    sz = _yixin_extract_index_amount(content, prefer="sz")
            except Exception as e:
                log.warning("yixin prev joint query failed: %s", e)
        ok = sh is not None and sz is not None and sh > 1e11 and sz > 1e11
        out = {
            "ok": ok,
            "source": "yixin",
            "prev_day": prev_day,
            "prev_sh": sh,
            "prev_sz": sz,
            "prev_hs": (float(sh) + float(sz)) if ok else None,
        }
        if ok:
            _cache_set(cache_key, out, ttl=3600.0, stale=7200.0)
        else:
            log.warning(
                "yixin prev day incomplete day=%s sh=%s sz=%s snippets=%s",
                prev_day,
                sh,
                sz,
                [c[:120].replace("\n", " ") for c in contents[:2]],
            )
        return out
    except Exception as e:
        log.warning("yixin prev day amounts failed: %s", e)
        return {"ok": False, "error": str(e)}


def _curve_from_minute_amounts(pts: list[tuple[str, float]]) -> dict[int, float]:
    if not pts:
        return {}
    total = sum(float(a) for _, a in pts)
    if total <= 0:
        return {}
    cum = 0.0
    curve: dict[int, float] = {}
    def _tm_key(tm: str) -> int:
        raw = tm.split(" ")[-1] if " " in str(tm) else str(tm)
        return _hhmm_to_session_idx(_hhmm_from_any(raw)) or 0
    for tm, a in sorted(pts, key=lambda x: _tm_key(x[0])):
        raw = str(tm).split(" ")[-1] if " " in str(tm) else str(tm)
        idx = _hhmm_to_session_idx(_hhmm_from_any(raw))
        if idx is None:
            continue
        cum += float(a)
        curve[idx] = cum / total
    if not curve:
        return {}
    last = 0.0
    full: dict[int, float] = {}
    for i in range(240):
        if i in curve:
            last = curve[i]
        full[i] = last
    full[239] = 1.0
    return full


def _load_day_curves_archive() -> dict[str, Any]:
    try:
        if not VOL_PROFILE_DAYS.exists():
            return {}
        raw = json.loads(VOL_PROFILE_DAYS.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception as e:
        log.warning("load vol profile days failed: %s", e)
        return {}


def _save_day_curves_archive(archive: dict[str, Any]) -> None:
    try:
        VOL_PROFILE_DAYS.parent.mkdir(parents=True, exist_ok=True)
        days = sorted([d for d in archive.keys() if re.match(r"\d{4}-\d{2}-\d{2}", d)])
        if len(days) > 12:
            for d in days[:-12]:
                archive.pop(d, None)
        VOL_PROFILE_DAYS.write_text(json.dumps(archive, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        log.warning("save vol profile days failed: %s", e)


def _archive_today_curve_if_complete(
    today: str,
    sh_by: dict[str, list[tuple[str, float]]],
    sz_by: dict[str, list[tuple[str, float]]],
) -> None:
    sh_pts = sh_by.get(today) or []
    sz_pts = sz_by.get(today) or []
    if len(sh_pts) < 200 and len(sz_pts) < 200:
        return
    sh_curve = _curve_from_minute_amounts(sh_pts) if len(sh_pts) >= 200 else {}
    sz_curve = _curve_from_minute_amounts(sz_pts) if len(sz_pts) >= 200 else {}
    bag: dict[str, float] = {}
    for tm, a in sh_pts:
        bag[tm] = bag.get(tm, 0.0) + float(a)
    for tm, a in sz_pts:
        bag[tm] = bag.get(tm, 0.0) + float(a)
    hs_pts = sorted(bag.items(), key=lambda x: _hhmm_to_session_idx(x[0]) or 0)
    hs_curve = _curve_from_minute_amounts(hs_pts) if len(hs_pts) >= 200 else _blend_curves(sh_curve, sz_curve)
    if len(hs_curve) < 30:
        return
    archive = _load_day_curves_archive()
    archive[today] = {
        "sh": _curve_to_str_keys(sh_curve),
        "sz": _curve_to_str_keys(sz_curve),
        "hs": _curve_to_str_keys(hs_curve),
        "sh_total": sum(a for _, a in sh_pts) if sh_pts else None,
        "sz_total": sum(a for _, a in sz_pts) if sz_pts else None,
        "saved_at": _now_iso(),
    }
    _save_day_curves_archive(archive)


def _curves_from_day_archive(exclude_day: str) -> tuple[dict[int, float], dict[int, float], dict[int, float], str]:
    archive = _load_day_curves_archive()
    sh_list: list[dict[int, float]] = []
    sz_list: list[dict[int, float]] = []
    hs_list: list[dict[int, float]] = []
    used = 0
    for day, payload in sorted(archive.items(), reverse=True):
        if day == exclude_day or not isinstance(payload, dict):
            continue
        sh = _curve_from_str_keys(payload.get("sh"))
        sz = _curve_from_str_keys(payload.get("sz"))
        hs = _curve_from_str_keys(payload.get("hs")) or _blend_curves(sh, sz)
        if len(hs) < 30:
            continue
        if sh:
            sh_list.append(sh)
        if sz:
            sz_list.append(sz)
        hs_list.append(hs)
        used += 1
        if used >= 5:
            break
    if not hs_list:
        return {}, {}, {}, "none"
    return (
        _blend_curves(*sh_list) if sh_list else {},
        _blend_curves(*sz_list) if sz_list else {},
        _blend_curves(*hs_list),
        "day_archive",
    )


def _yixin_hist_curves(exclude_day: str) -> tuple[dict[int, float], dict[int, float], dict[int, float], str]:
    cache_key = f"yixin_hist_curve:{exclude_day}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached.get("sh") or {}, cached.get("sz") or {}, cached.get("hs") or {}, cached.get("profile_source") or "yixin"
    if not _yixin_api_key():
        return {}, {}, {}, "none"
    try:
        d0 = datetime.strptime(exclude_day, "%Y-%m-%d")
    except Exception:
        return {}, {}, {}, "none"
    # Hot path: only 1 recent day SH shape (timeout-bounded). Full multi-day
    # learning comes from local day_archive after closes.
    sh_curves: list[dict[int, float]] = []
    sz_curves: list[dict[int, float]] = []
    for i in range(1, 8):
        d1 = d0 - timedelta(days=i)
        if d1.weekday() >= 5:
            continue
        day = d1.strftime("%Y-%m-%d")
        try:
            data = _yixin_fin_db(f"上证指数 {day} 1分钟K线 成交额", timeout=18)
            content = _yixin_result_content(data)
            pts: list[tuple[str, float]] = []
            for line in content.splitlines():
                line = line.strip()
                if not line.startswith("|"):
                    continue
                parts = [p.strip() for p in line.strip("|").split("|")]
                if len(parts) < 7:
                    continue
                if not re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", parts[0]):
                    continue
                try:
                    amt = float(parts[6].replace(",", ""))
                except ValueError:
                    continue
                if amt < 0:
                    continue
                pts.append((parts[0], amt))
            curve = _curve_from_minute_amounts(pts)
            if len(curve) >= 30:
                sh_curves.append(curve)
                break
        except Exception as e:
            log.warning("yixin hist curve SH %s failed: %s", day, e)
            break
    sh = _blend_curves(*sh_curves) if sh_curves else {}
    sz = dict(sh) if sh else {}
    hs = dict(sh) if sh else {}
    src = "yixin" if len(hs) >= 30 else "none"
    payload = {"sh": sh, "sz": sz, "hs": hs, "profile_source": src}
    if src != "none":
        _cache_set(cache_key, payload, ttl=YIXIN_CURVE_TTL, stale=YIXIN_CURVE_TTL * 2)
    return sh, sz, hs, src


def _hithink_api_key() -> str:
    return (os.environ.get("IWENCAI_API_KEY") or "").strip()


# Process-local: once iwencai reports daily quota exhausted, skip further
# network hits until the next calendar day (Asia/Shanghai).
_HITHINK_QUOTA_EXHAUSTED_DAY: str | None = None


def _hithink_quota_day() -> str:
    try:
        return datetime.now(TZ_SH).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _hithink_mark_quota_exhausted() -> None:
    global _HITHINK_QUOTA_EXHAUSTED_DAY
    _HITHINK_QUOTA_EXHAUSTED_DAY = _hithink_quota_day()


def _hithink_quota_blocked() -> bool:
    return _HITHINK_QUOTA_EXHAUSTED_DAY == _hithink_quota_day()


def _hithink_query(query: str, limit: int = 10, timeout: int = 30) -> dict[str, Any]:
    if _hithink_quota_blocked():
        raise RuntimeError("hithink quota exhausted today")
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
    if r.status_code == 401:
        txt = (r.text or "")[:240]
        if "次数已用完" in txt or "额度" in txt:
            _hithink_mark_quota_exhausted()
            raise RuntimeError("hithink quota exhausted today")
        raise RuntimeError(f"hithink unauthorized: {txt}")
    # Some iwencai skill responses return 200 with quota message body.
    try:
        preview = (r.text or "")[:240]
        if "次数已用完" in preview or ("额度" in preview and "升级" in preview):
            _hithink_mark_quota_exhausted()
            raise RuntimeError("hithink quota exhausted today")
    except RuntimeError:
        raise
    except Exception:
        pass
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
      1) Eastmoney multi-day trends (when hist days exist)
      1b) Tencent day/minute 1m amount path (server-reachable backup)
      2) local day-archive of completed sessions (self-learning)
      3) yixin 1m historical shapes
      4) disk last-good snapshot
      5) builtin open-heavy A-share profile (NOT linear)
    Prev-day totals:
      1) EM complete days if any
      2) hithink/问财
      3) yixin
      4) disk
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

    # Tencent 1-min amount path (server-reachable). Fill missing hist days / today path when EM thin.
    try:
        need_tx = (len(sh_curve) < 30 or len(sz_curve) < 30
                   or len(sh_by.get(today) or []) < 30 or len(sz_by.get(today) or []) < 30
                   or len(_complete_day_totals(sh_by, exclude_day=today)) < 1
                   or len(_complete_day_totals(sz_by, exclude_day=today)) < 1)
        if need_tx:
            tx_sh = _tencent_index_minute_by_day("sh000001")
            tx_sz = _tencent_index_minute_by_day("sz399001")
            if tx_sh or tx_sz:
                sh_by = _merge_minute_by_day(sh_by, tx_sh)
                sz_by = _merge_minute_by_day(sz_by, tx_sz)
                sh_curve2 = _avg_cum_ratio_curve(sh_by, exclude_day=today)
                sz_curve2 = _avg_cum_ratio_curve(sz_by, exclude_day=today)
                if len(sh_curve2) >= 30 or len(sz_curve2) >= 30:
                    sh_curve, sz_curve = sh_curve2, sz_curve2
                    if profile_source == "eastmoney":
                        profile_source = "eastmoney+tencent"
                    else:
                        profile_source = "tencent"
                elif not sh_curve and not sz_curve:
                    # keep any partial curves for today path even if hist incomplete
                    sh_curve, sz_curve = sh_curve2, sz_curve2
                    if sh_curve or sz_curve:
                        profile_source = "tencent"
    except Exception as e:
        log.warning("tencent minute profile fallback failed: %s", e)
        if em_error is None:
            em_error = str(e)

    hs_curve = _blend_curves(sh_curve, sz_curve)

    sh_days = _complete_day_totals(sh_by, exclude_day=today)
    sz_days = _complete_day_totals(sz_by, exclude_day=today)
    sh_map = {d: a for d, a in sh_days}
    sz_map = {d: a for d, a in sz_days}
    # complete historical days shared by SH/SZ (EM and/or Tencent merged into sh_by/sz_by)
    common = sorted(set(sh_map) & set(sz_map), reverse=True)
    prev_day = common[0] if common else None
    prev_sh = sh_map.get(prev_day) if prev_day else None
    prev_sz = sz_map.get(prev_day) if prev_day else None
    prev_hs = None
    if prev_sh is not None and prev_sz is not None:
        prev_hs = float(prev_sh) + float(prev_sz)
    if prev_hs:
        if str(profile_source).startswith("eastmoney"):
            prev_source = "eastmoney"
        elif profile_source == "tencent":
            prev_source = "tencent"
        elif "tencent" in str(profile_source):
            prev_source = "eastmoney+tencent"
        else:
            prev_source = str(profile_source or "minute_path")
    else:
        prev_source = "none"

    # Persist near-complete EM today path for tomorrow's multi-day average
    try:
        _archive_today_curve_if_complete(today, sh_by, sz_by)
    except Exception as e:
        log.warning("archive today curve failed: %s", e)

    # 2) self-learned day archive
    if len(hs_curve) < 30:
        a_sh, a_sz, a_hs, a_src = _curves_from_day_archive(exclude_day=today)
        if len(a_hs) >= 30:
            sh_curve, sz_curve, hs_curve = a_sh or a_hs, a_sz or a_hs, a_hs
            profile_source = a_src

    # 3) yixin historical 1m shapes (blend with open-heavy prior to damp single-day noise)
    if len(hs_curve) < 30:
        y_sh, y_sz, y_hs, y_src = _yixin_hist_curves(exclude_day=today)
        if len(y_hs) >= 30:
            prior = _BUILTIN_SESSION_CURVE
            # yixin provides shape; builtin anchors morning cumulative share (~0.62@11:15)
            hs_curve = _blend_curves(y_hs, y_hs, prior)  # 2/3 yixin + 1/3 prior
            sh_curve = _blend_curves(y_sh or y_hs, y_sh or y_hs, prior) if (y_sh or y_hs) else dict(hs_curve)
            sz_curve = _blend_curves(y_sz or y_hs, y_sz or y_hs, prior) if (y_sz or y_hs) else dict(hs_curve)
            profile_source = y_src

    # 4) disk last-good snapshot
    if len(hs_curve) < 30:
        disk = _load_disk_profile()
        if disk and len(disk.get("hs") or {}) >= 30:
            sh_curve = disk.get("sh") or sh_curve
            sz_curve = disk.get("sz") or sz_curve
            hs_curve = disk.get("hs") or hs_curve
            profile_source = "disk_cache"

    # 5) open-heavy builtin (NOT linear)
    if len(hs_curve) < 30:
        sh_curve = dict(_BUILTIN_SESSION_CURVE)
        sz_curve = dict(_BUILTIN_SESSION_CURVE)
        hs_curve = dict(_BUILTIN_SESSION_CURVE)
        profile_source = "builtin"

    if len(hs_curve) >= 30:
        if len(sh_curve) < 30:
            sh_curve = dict(hs_curve)
        if len(sz_curve) < 30:
            sz_curve = dict(hs_curve)

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

    # Prev full-day HS(+BJ) amount:
    # EM daily kline (when reachable) -> tencent day kline (stable) -> sohu -> yixin -> disk
    # (hithink intentionally excluded from fund-flow amount chain)
    # Note: yixin historical SZ amount can under-report vs exchange-wide EM kline.
    # Media HS+BJ total ~= SH + SZ + BJ50; always enrich prev_bj from Tencent when possible.
    prev_bj = None
    if prev_hs is None:
        emk = _em_prev_day_amounts(prev_day or guess, today=today)
        if emk.get("ok") and emk.get("prev_sh") and emk.get("prev_sz"):
            prev_day = emk.get("prev_day") or prev_day or guess
            prev_sh = float(emk["prev_sh"])
            prev_sz = float(emk["prev_sz"])
            prev_hs = float(prev_sh) + float(prev_sz)
            prev_source = "eastmoney_kline"
    if prev_hs is None:
        tk = _tencent_prev_day_amounts(prev_day or guess, today=today)
        if tk.get("ok") and tk.get("prev_sh") and tk.get("prev_sz"):
            prev_day = tk.get("prev_day") or prev_day or guess
            prev_sh = float(tk["prev_sh"])
            prev_sz = float(tk["prev_sz"])
            prev_hs = float(prev_sh) + float(prev_sz)
            if tk.get("prev_bj") is not None:
                prev_bj = float(tk["prev_bj"])
            prev_source = "tencent"
    if prev_hs is None:
        sk = _sohu_prev_day_amounts(prev_day or guess, today=today)
        if sk.get("ok") and sk.get("prev_sh") and sk.get("prev_sz"):
            prev_day = sk.get("prev_day") or prev_day or guess
            prev_sh = float(sk["prev_sh"])
            prev_sz = float(sk["prev_sz"])
            prev_hs = float(prev_sh) + float(prev_sz)
            prev_source = "sohu"
    # hithink intentionally skipped for fund-flow amount chain
    if prev_hs is None:
        yx = _yixin_prev_day_amounts(prev_day or guess) if _yixin_api_key() else {"ok": False}
        if yx.get("ok") and yx.get("prev_sh") and yx.get("prev_sz"):
            # Cross-check with EM kline when available; reject clearly bad yixin SZ/SH.
            emk2 = _em_prev_day_amounts(prev_day or guess, today=today)
            use_yx = True
            if emk2.get("ok") and emk2.get("prev_hs"):
                # Prefer EM whenever kline works.
                prev_day = emk2.get("prev_day") or prev_day or guess
                prev_sh = float(emk2["prev_sh"])
                prev_sz = float(emk2["prev_sz"])
                prev_hs = float(prev_sh) + float(prev_sz)
                prev_source = "eastmoney_kline"
                use_yx = False
            if use_yx:
                y_sh = float(yx["prev_sh"])
                y_sz = float(yx["prev_sz"])
                # Guard: SZ usually not far below SH for full-market kline口径; if yixin
                # SZ is suspiciously low vs SH on a normal day, keep only if no better source.
                prev_day = yx.get("prev_day") or prev_day or guess
                prev_sh = y_sh
                prev_sz = y_sz
                prev_hs = y_sh + y_sz
                prev_source = "yixin"
    if prev_hs is None:
        disk = _load_disk_profile() or {}
        d_sh = disk.get("prev_sh")
        d_sz = disk.get("prev_sz")
        if d_sh is not None and d_sz is not None and float(d_sh) > 0 and float(d_sz) > 0:
            # Prefer market daily bars over potentially stale/wrong yixin disk cache.
            emk3 = _em_prev_day_amounts(disk.get("prev_day") or prev_day or guess, today=today)
            tk3 = None if emk3.get("ok") else _tencent_prev_day_amounts(disk.get("prev_day") or prev_day or guess, today=today)
            if emk3.get("ok") and emk3.get("prev_hs"):
                prev_day = emk3.get("prev_day") or disk.get("prev_day") or prev_day or guess
                prev_sh = float(emk3["prev_sh"])
                prev_sz = float(emk3["prev_sz"])
                prev_hs = float(prev_sh) + float(prev_sz)
                prev_source = "eastmoney_kline"
            elif tk3 and tk3.get("ok") and tk3.get("prev_hs"):
                prev_day = tk3.get("prev_day") or disk.get("prev_day") or prev_day or guess
                prev_sh = float(tk3["prev_sh"])
                prev_sz = float(tk3["prev_sz"])
                prev_hs = float(prev_sh) + float(prev_sz)
                if tk3.get("prev_bj") is not None:
                    prev_bj = float(tk3["prev_bj"])
                prev_source = "tencent"
            else:
                sk3 = _sohu_prev_day_amounts(disk.get("prev_day") or prev_day or guess, today=today)
                if sk3.get("ok") and sk3.get("prev_hs"):
                    prev_day = sk3.get("prev_day") or disk.get("prev_day") or prev_day or guess
                    prev_sh = float(sk3["prev_sh"])
                    prev_sz = float(sk3["prev_sz"])
                    prev_hs = float(prev_sh) + float(prev_sz)
                    prev_source = "sohu"
                else:
                    prev_day = disk.get("prev_day") or prev_day or guess
                    prev_sh = float(d_sh)
                    prev_sz = float(d_sz)
                    prev_hs = float(d_sh) + float(d_sz)
                    if disk.get("prev_bj") is not None:
                        prev_bj = float(disk.get("prev_bj"))
                    prev_source = "disk_cache"

    # Always attach previous BJ (BJ50) so vs_prev can use HS+BJ scope.
    if prev_bj is None:
        try:
            tk_bj = _tencent_prev_day_amounts(prev_day or guess, today=today)
            if tk_bj.get("prev_bj") is not None:
                prev_bj = float(tk_bj["prev_bj"])
                if not prev_day and tk_bj.get("prev_day"):
                    prev_day = tk_bj.get("prev_day")
            elif prev_day:
                prev_bj = _tencent_prev_bj_amount(prev_day, today=today)
        except Exception as e:
            log.warning("prev bj enrich failed: %s", e)
    if prev_bj is None:
        try:
            d0 = _load_disk_profile() or {}
            if d0.get("prev_bj") is not None and float(d0.get("prev_bj") or 0) > 0:
                prev_bj = float(d0["prev_bj"])
        except Exception:
            pass
    prev_total = None
    if prev_hs is not None:
        prev_total = float(prev_hs) + (float(prev_bj) if prev_bj is not None and float(prev_bj) > 0 else 0.0)

    method_label = {
        "eastmoney": "profile_3d",
        "eastmoney+tencent": "profile_3d",
        "tencent": "profile_3d_tencent",
        "day_archive": "profile_archive",
        "yixin": "profile_yixin",
        "disk_cache": "profile_cache",
        "builtin": "profile_builtin",
    }.get(profile_source, "profile_3d")

    # today's realized minute path (for intraday self-calibration)
    today_cum_sh = _minute_cum_from_pts(sh_by.get(today) or [])
    today_cum_sz = _minute_cum_from_pts(sz_by.get(today) or [])
    if today_cum_sh or today_cum_sz:
        # HS path = SH+SZ minute sums when both exist; else whichever available
        today_cum_hs: dict[int, float] = {}
        keys = set(today_cum_sh) | set(today_cum_sz)
        for i in sorted(keys):
            today_cum_hs[i] = float(today_cum_sh.get(i) or 0.0) + float(today_cum_sz.get(i) or 0.0)
    else:
        today_cum_hs = {}

    # Absolute 3-day same-time baseline on HS (SH+SZ minute paths)
    hs_by_merged: dict[str, list[tuple[str, float]]] = {}
    for day in set(sh_by) | set(sz_by):
        sh_pts = {t: a for t, a in (sh_by.get(day) or [])}
        sz_pts = {t: a for t, a in (sz_by.get(day) or [])}
        times = sorted(set(sh_pts) | set(sz_pts), key=lambda t: _hhmm_to_session_idx(t) or 0)
        hs_by_merged[day] = [(t, float(sh_pts.get(t) or 0.0) + float(sz_pts.get(t) or 0.0)) for t in times]
    hs_s3_cum, hs_s3_full, hs_s3_days = _abs_3d_baseline(hs_by_merged, exclude_day=today, max_days=3)

    payload: dict[str, Any] = {
        "sh": sh_curve,
        "sz": sz_curve,
        "hs": hs_curve,
        "today_cum_sh": today_cum_sh,
        "today_cum_sz": today_cum_sz,
        "today_cum_hs": today_cum_hs,
        "prev_day": prev_day,
        "prev_sh": prev_sh,
        "prev_sz": prev_sz,
        "prev_bj": prev_bj,
        "prev_hs": prev_hs,
        "prev_total": prev_total,
        "profile_source": profile_source,
        "prev_source": prev_source,
        "em_error": em_error,
        "curve_points": len(hs_curve),
        "method_label": method_label,
        "today_path_points": len(today_cum_hs),
        "hs_s3_cum": hs_s3_cum,
        "hs_s3_full": hs_s3_full,
        "hs_s3_days": hs_s3_days,
    }

    ok_curve = len(hs_curve) >= 30
    # Persist last-good curve + prev amounts for next cold start
    if ok_curve and profile_source in ("eastmoney", "eastmoney+tencent", "tencent", "day_archive", "yixin", "builtin", "disk_cache"):
        if prev_hs is not None or profile_source != "disk_cache":
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


def _max_lianban(pool_stats: dict[str, Any]) -> int:
    mx = 1
    for row in (pool_stats.get("lb2_top") or []) + (pool_stats.get("top") or []):
        if not isinstance(row, dict):
            continue
        for k in ("board_count", "lbc", "days"):
            v = row.get(k)
            try:
                if v is not None:
                    mx = max(mx, int(v))
            except (TypeError, ValueError):
                continue
    return max(1, mx)


def _compute_sentiment_strength(
    zt: dict[str, Any],
    zb: dict[str, Any],
    sh: dict[str, Any],
    sz: dict[str, Any],
    day: Optional[str] = None,
) -> dict[str, Any]:
    """Self-built market sentiment score 0-100 (Eastmoney inputs only).

    Not claimed to equal any third-party black-box score. Tip thresholds 25/75
    are conventional short-term emotion bands used for UI guidance only.
    """
    zt_n = int(zt.get("count") or 0)
    zb_n = int(zb.get("count") or 0)
    max_lb = _max_lianban(zt)
    seal = (zt_n / (zt_n + zb_n)) if (zt_n + zb_n) > 0 else 0.5
    up = int(sh.get("up_count") or 0) + int(sz.get("up_count") or 0)
    down = int(sh.get("down_count") or 0) + int(sz.get("down_count") or 0)
    breadth = (up / (up + down)) if (up + down) > 0 else 0.5

    s_zt = min(100.0, zt_n / 1.2)  # ~120 limit-ups -> 100
    s_seal = seal * 100.0
    s_h = min(100.0, max_lb / 10.0 * 100.0)
    s_b = breadth * 100.0
    strong = 0.35 * s_zt + 0.30 * s_seal + 0.20 * s_h + 0.15 * s_b
    strong = max(0.0, min(100.0, float(strong)))

    if strong >= 75:
        band, band_label = "high", "偏热"
    elif strong <= 25:
        band, band_label = "low", "偏冷"
    else:
        band, band_label = "mid", "中性"

    tip = (
        "温馨提示：情绪指标过高（75），短期有释放亏钱效应的风险；"
        "情绪指标过低（25），短线有反弹回暖需求；提示仅供参考"
    )
    return {
        "ok": True,
        "source": "self",
        "strong": round(strong, 1),
        "ztjs": zt_n,
        "df_num": zb_n,  # 炸板家数（自研口径）
        "lbgd": max_lb,
        "seal_rate": round(seal, 4),
        "breadth": round(breadth, 4),
        "components": {
            "zt": round(s_zt, 2),
            "seal": round(s_seal, 2),
            "height": round(s_h, 2),
            "breadth": round(s_b, 2),
            "weights": {"zt": 0.35, "seal": 0.30, "height": 0.20, "breadth": 0.15},
        },
        "band": band,
        "band_label": band_label,
        "tip": tip,
        "label": "综合强度",
        "day": day,
        "note": "自研情绪分（东财涨停/炸板/涨跌家数），参考阈值 25/75",
    }


def _board_limit_pct(code: Any, name: Any = None) -> float:
    """Board limit-up/down percent for common A-share rules."""
    n = str(name or "").upper().replace(" ", "")
    if "ST" in n:
        return 5.0
    c = str(code or "").strip()
    if len(c) >= 6:
        c6 = c[-6:]
    else:
        c6 = c
    if c6.startswith(("300", "301", "688", "689")):
        return 20.0
    # BJ / 新三板精选层风格
    if c6.startswith(("8", "4", "9")) and not c6.startswith(("60", "00", "30")):
        # 北交所常见 8/4 开头
        if c6[0] in ("8", "4"):
            return 30.0
    return 10.0


def _lbc_of(x: dict[str, Any]) -> int:
    if not isinstance(x, dict):
        return 0
    lbc = x.get("lbc")
    if lbc is None and isinstance(x.get("zttj"), dict):
        lbc = x["zttj"].get("ct") or x["zttj"].get("days")
    if lbc is None:
        lbc = x.get("days") or x.get("ylbc")
    try:
        return int(lbc or 0)
    except (TypeError, ValueError):
        return 0


def _fbt_int(x: dict[str, Any], key: str = "fbt") -> int:
    try:
        v = x.get(key)
        if v is None:
            return 999999
        return int(v)
    except (TypeError, ValueError):
        return 999999


def _zbc_int(x: dict[str, Any]) -> int:
    try:
        return int(x.get("zbc") or 0)
    except (TypeError, ValueError):
        return 0


def _is_yizi_row(x: dict[str, Any]) -> bool:
    """Heuristic one-word board: first seal at/before open and never opened."""
    return _zbc_int(x) == 0 and 0 <= _fbt_int(x) <= 93000


def _as_pool_row(
    code: Any,
    name: Any,
    *,
    zdp: Any = None,
    amount: Any = None,
    lbc: Any = None,
    fbt: Any = None,
    zbc: Any = None,
    hybk: Any = None,
    fund: Any = None,
    extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "c": str(code or "").strip(),
        "n": str(name or "").strip(),
        "zdp": zdp,
        "amount": amount,
        "lbc": lbc,
        "fbt": fbt,
        "zbc": zbc,
        "hybk": hybk,
        "fund": fund,
    }
    if extra:
        row.update(extra)
    return row


def _em_clist_scan(
    *,
    pagesize: int = 200,
    sort: str = "f3:desc",
    fs: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Scan Eastmoney clist for A-share quotes used by near-limit / once-down rules."""
    session = requests.Session()
    session.trust_env = False
    # 沪深 A 股（不含北交，控制噪音；北交在主题池里仍可出现）
    fs = fs or "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
    fields = "f2,f3,f6,f8,f12,f14,f15,f16,f17,f18,f22,f100"
    fid = (sort.split(":")[0] if sort else "f3")
    po = 1
    if sort and sort.endswith(":asc"):
        po = 0
    params = {
        "pn": 1,
        "pz": int(pagesize),
        "po": int(po),
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": fid,
        "fs": fs,
        "fields": fields,
        "_": int(time.time() * 1000),
    }
    # order via fid + po already; keep sort string only for logging
    hosts = [
        EM_CLIST,
        "https://push2delay.eastmoney.com/api/qt/clist/get",
        "https://82.push2.eastmoney.com/api/qt/clist/get",
    ]
    for url in hosts:
        try:
            r = session.get(url, params=params, headers=EM_HEADERS, timeout=10)
            r.raise_for_status()
            data = r.json()
            diff = ((data.get("data") or {}).get("diff")) or []
            if isinstance(diff, list) and diff:
                return [x for x in diff if isinstance(x, dict)]
        except Exception as e:
            log.warning("clist scan failed %s: %s", url, e)
            continue
    return []


def _pool_item_to_daban(x: dict[str, Any], tab: str) -> Optional[dict[str, Any]]:
    if not isinstance(x, dict):
        return None
    code = str(x.get("c") or x.get("f12") or "").strip()
    name = str(x.get("n") or x.get("f14") or "").strip()
    if not code or not name:
        return None
    if _is_st_name(name):
        return None

    limit_times = _lbc_of(x)
    board_text = None
    if tab == "broken":
        board_text = "炸板"
        zbc = _zbc_int(x)
        if zbc > 0:
            board_text = f"炸{zbc}次"
    elif tab == "auction":
        fbt = _fbt_int(x)
        if fbt <= 92530:
            board_text = "竞价封"
        elif fbt <= 93030:
            board_text = "开盘封"
        else:
            board_text = "早封"
    elif tab == "near_limit":
        board_text = str(x.get("near_tag") or "逼近")
    elif tab == "wind":
        if limit_times >= 2:
            board_text = f"{limit_times}板"
        else:
            board_text = str(x.get("wind_tag") or "风向")
    elif tab == "limit_down":
        days = limit_times or 1
        board_text = f"跌停{days}天" if days > 1 else "跌停"
    elif tab == "new_stock":
        ods = x.get("ods")
        try:
            ods_i = int(ods) if ods is not None else None
        except (TypeError, ValueError):
            ods_i = None
        if name.startswith("N") or name.startswith("C"):
            board_text = "新股"
        elif ods_i is not None:
            board_text = f"次新{ods_i}日"
        else:
            board_text = "次新"
    elif tab == "natural":
        board_text = "自然涨停"
        if limit_times >= 2:
            board_text = f"自然{limit_times}板"
    elif tab == "once_down":
        board_text = str(x.get("once_tag") or "曾跌停")
    elif tab == "yizi":
        board_text = "一字"
        if limit_times and limit_times >= 2:
            board_text = f"一字{limit_times}板"
    elif tab == "lianban":
        board_text = f"{limit_times}板" if limit_times >= 2 else "连板"
    elif limit_times and limit_times >= 2:
        board_text = f"{limit_times}板"
    elif tab == "limit_up":
        board_text = "首板" if limit_times <= 1 else f"{limit_times}板"

    hybk = str(x.get("hybk") or x.get("f100") or "").strip() or None
    chg = _num(x.get("zdp") if x.get("zdp") is not None else x.get("f3"))
    # 涨停池涨跌幅接近 0 不展示
    if tab == "limit_up" and chg is not None and abs(float(chg)) < 1e-6:
        chg = None
    amount = x.get("amount") if x.get("amount") is not None else x.get("f6")
    return {
        "code": code,
        "name": name,
        "change_pct": chg,
        "board_text": board_text,
        "limit_times": limit_times or None,
        "theme": hybk,
        "industry": hybk,
        "tab": tab,
        "amount_yi": _yi(_num(amount)),
        "fbt": x.get("fbt"),
        "zbc": x.get("zbc"),
        "fund": _num(x.get("fund")),
        "score": _num(x.get("_score")),
        "rule": x.get("_rule"),
    }


def _fbt_sort_key(x: dict[str, Any]) -> int:
    return _fbt_int(x)


def _fund_of(x: dict[str, Any]) -> float:
    v = _num(x.get("fund"))
    return float(v) if v is not None else 0.0


def _amount_of(x: dict[str, Any]) -> float:
    v = _num(x.get("amount") if x.get("amount") is not None else x.get("f6"))
    return float(v) if v is not None else 0.0


def _zdp_of(x: dict[str, Any]) -> float:
    v = _num(x.get("zdp") if x.get("zdp") is not None else x.get("f3"))
    return float(v) if v is not None else -999.0


def _build_daban_from_em(
    zt_pool: list[dict[str, Any]],
    zb_pool: list[dict[str, Any]],
    qs_pool: list[dict[str, Any]],
    *,
    dt_pool: Optional[list[dict[str, Any]]] = None,
    cx_pool: Optional[list[dict[str, Any]]] = None,
    yzt_pool: Optional[list[dict[str, Any]]] = None,
    clist_up: Optional[list[dict[str, Any]]] = None,
    clist_speed: Optional[list[dict[str, Any]]] = None,
    clist_down: Optional[list[dict[str, Any]]] = None,
    limit: int = 15,
    day: Optional[str] = None,
) -> dict[str, Any]:
    """Build short-term board tabs from Eastmoney public pools + clist rules (v2).

    Rules aim to mirror common short-term watchlists (auction / near-limit / wind /
    natural first-board / once limit-down) without third-party proprietary feeds.
    """
    lim = max(5, min(int(limit or 500), 500))
    non_st_zt = [x for x in (zt_pool or []) if not _is_st_name(x.get("n") or "")]
    non_st_zb = [x for x in (zb_pool or []) if not _is_st_name(x.get("n") or "")]
    non_st_qs = [x for x in (qs_pool or []) if not _is_st_name(x.get("n") or "")]
    non_st_dt = [x for x in (dt_pool or []) if not _is_st_name(x.get("n") or "")]
    non_st_cx = [x for x in (cx_pool or []) if not _is_st_name(x.get("n") or "")]
    non_st_yzt = [x for x in (yzt_pool or []) if not _is_st_name(x.get("n") or "")]
    zt_codes = {str(x.get("c") or "") for x in non_st_zt}
    zb_codes = {str(x.get("c") or "") for x in non_st_zb}
    qs_codes = {str(x.get("c") or "") for x in non_st_qs}
    dt_codes = {str(x.get("c") or "") for x in non_st_dt}
    yzt_codes = {str(x.get("c") or "") for x in non_st_yzt}

    # ---------- 涨停：封单优先 → 连板 → 早封 → 成交额 ----------
    limit_up_src = sorted(
        non_st_zt,
        key=lambda z: (
            _fund_of(z),
            _lbc_of(z),
            -_fbt_int(z),  # earlier seal first when fund/lbc equal
            _amount_of(z),
        ),
        reverse=True,
    )[:lim]

    # ---------- 炸板：成交额 × 早炸/多次炸加权 ----------
    def _broken_score(z: dict[str, Any]) -> float:
        amt = _amount_of(z)
        zbc = max(1, _zbc_int(z))
        fbt = _fbt_int(z)
        early = 1.25 if fbt <= 100000 else (1.1 if fbt <= 103000 else 1.0)
        multi = 1.0 + min(0.35, 0.08 * max(0, zbc - 1))
        return amt * early * multi

    broken_src = sorted(non_st_zb, key=_broken_score, reverse=True)[:lim]
    for z in broken_src:
        z["_score"] = _broken_score(z)
        z["_rule"] = "amount*early*multi_zbc"

    # ---------- 自然涨停：非一字（盘中可交易封板），首板优先 ----------
    natural_src = []
    for z in non_st_zt:
        if _is_yizi_row(z):
            continue
        # 首板权重更高；2 板及以上也可进（非一字连板）但排序靠后
        natural_src.append(z)
    natural_src = sorted(
        natural_src,
        key=lambda z: (
            1 if _lbc_of(z) <= 1 else 0,
            _fund_of(z),
            _amount_of(z),
            -_fbt_int(z),
        ),
        reverse=True,
    )[:lim]
    for z in natural_src:
        z["_rule"] = "non_yizi_natural"

    # ---------- 跌停：封单/成交额 ----------
    limit_down_src = sorted(
        non_st_dt,
        key=lambda z: (_fund_of(z), _amount_of(z), -_fbt_int(z, "lbt")),
        reverse=True,
    )[:lim]
    for z in limit_down_src:
        z["_rule"] = "dt_fund_amount"

    # ---------- 新股/次新：开板天数升序，成交活跃优先 ----------
    def _ods_of(z: dict[str, Any]) -> int:
        try:
            return int(z.get("ods") if z.get("ods") is not None else 9999)
        except (TypeError, ValueError):
            return 9999

    new_stock_src = sorted(
        non_st_cx,
        key=lambda z: (
            _ods_of(z),
            -_amount_of(z),
            -_zdp_of(z),
        ),
    )[:lim]
    for z in new_stock_src:
        z["_rule"] = "cx_ods_amount"

    # ---------- 即将涨停：涨幅/涨速/距板 + 强势/炸板回封交叉 ----------
    clist_map: dict[str, dict[str, Any]] = {}
    for row in (clist_up or []) + (clist_speed or []) + (clist_down or []):
        code = str(row.get("f12") or "").strip()
        if code:
            # prefer row that has low/open fields; later rows only fill missing
            prev = clist_map.get(code)
            if prev is None:
                clist_map[code] = row
            else:
                merged = dict(prev)
                for kk, vv in row.items():
                    if merged.get(kk) is None and vv is not None:
                        merged[kk] = vv
                clist_map[code] = merged

    near_candidates: list[dict[str, Any]] = []
    seen_near: set[str] = set()

    def _push_near(row: dict[str, Any], tag: str, score: float, rule: str) -> None:
        code = str(row.get("c") or row.get("f12") or "").strip()
        if not code or code in seen_near or code in zt_codes:
            return
        name = str(row.get("n") or row.get("f14") or "").strip()
        if not name or _is_st_name(name):
            return
        item = dict(row)
        if "c" not in item:
            item["c"] = code
        if "n" not in item:
            item["n"] = name
        if item.get("zdp") is None and row.get("f3") is not None:
            item["zdp"] = row.get("f3")
        if item.get("amount") is None and row.get("f6") is not None:
            item["amount"] = row.get("f6")
        if item.get("hybk") is None and row.get("f100") is not None:
            item["hybk"] = row.get("f100")
        item["near_tag"] = tag
        item["_score"] = score
        item["_rule"] = rule
        near_candidates.append(item)
        seen_near.add(code)

    # clist primary: closest to limit-up with momentum
    for row in list(clist_map.values()):
        code = str(row.get("f12") or "").strip()
        name = str(row.get("f14") or "").strip()
        if not code or code in zt_codes or _is_st_name(name):
            continue
        px = _num(row.get("f2"))
        prev = _num(row.get("f18"))
        chg = _num(row.get("f3"))
        spd = _num(row.get("f22")) or 0.0
        if px is None or prev is None or prev <= 0:
            continue
        lim_pct = _board_limit_pct(code, name)
        limit_px = prev * (1.0 + lim_pct / 100.0)
        # already sealed / at limit
        if px >= limit_px * 0.998:
            continue
        # too weak
        if chg is None or chg < lim_pct * 0.45:
            continue
        gap_pct = max(0.0, (limit_px - px) / prev * 100.0)  # pct points to limit
        # distance score: smaller gap better
        score = float(chg) + float(spd) * 2.2 - gap_pct * 3.5
        if code in qs_codes:
            score += 5.0
        if code in zb_codes:
            score += 8.0  # 回封候选
        if code in yzt_codes:
            score += 2.5
        tag = "回封" if code in zb_codes else ("强势" if code in qs_codes else "逼近")
        _push_near(
            _as_pool_row(
                code,
                name,
                zdp=chg,
                amount=row.get("f6"),
                hybk=row.get("f100"),
                extra={"f2": px, "f18": prev, "f22": spd},
            ),
            tag,
            score,
            "clist_gap_speed",
        )

    # QS not yet sealed (exclude already at/near board limit)
    for z in non_st_qs:
        code = str(z.get("c") or "")
        name = str(z.get("n") or "")
        if code in zt_codes:
            continue
        chg = _zdp_of(z)
        lim_pct = _board_limit_pct(code, name)
        if chg >= lim_pct - 0.15:
            continue
        score = chg + (8.0 if code in zb_codes else 0.0) + 3.0
        _push_near(z, "强势", score, "qs_unsealed")

    # ZB re-seal candidates near limit (not sealed)
    for z in non_st_zb:
        code = str(z.get("c") or "")
        name = str(z.get("n") or "")
        if code in zt_codes:
            continue
        chg = _zdp_of(z)
        lim_pct = _board_limit_pct(code, name)
        if chg < max(5.0, lim_pct * 0.55):
            continue
        if chg >= lim_pct - 0.12:
            continue
        score = chg + 10.0 + min(5.0, _zbc_int(z) * 0.8)
        _push_near(z, "回封", score, "zb_reseal")

    near_src = sorted(near_candidates, key=lambda z: float(z.get("_score") or -999), reverse=True)[:lim]
    if len(near_src) < 5:
        # fallback: strongest QS / late-seal ZT excluded
        for z in sorted(non_st_qs, key=_zdp_of, reverse=True):
            _push_near(z, "强势", _zdp_of(z), "fallback_qs")
            if len(seen_near) >= lim:
                break
        near_src = sorted(near_candidates, key=lambda z: float(z.get("_score") or -999), reverse=True)[:lim]

    # ---------- 竞价/早封：竞价封 ∪ 开盘秒板 ∪ 昨涨停高开 / 高开强势 ----------
    auction_map: dict[str, dict[str, Any]] = {}

    def _push_auction(z: dict[str, Any], rule: str, score: float) -> None:
        code = str(z.get("c") or z.get("f12") or "").strip()
        if not code:
            return
        name = str(z.get("n") or z.get("f14") or "").strip()
        if not name or _is_st_name(name):
            return
        cur = auction_map.get(code)
        item = dict(z)
        if "c" not in item:
            item["c"] = code
        if "n" not in item:
            item["n"] = name
        item["_rule"] = rule
        item["_score"] = score
        if cur is None or score > float(cur.get("_score") or -999):
            auction_map[code] = item

    for z in non_st_zt:
        fbt = _fbt_int(z)
        if fbt <= 92530:
            _push_auction(z, "auction_seal", 1000.0 - fbt / 1e6 + _fund_of(z) / 1e12)
        elif fbt <= 93030:
            _push_auction(z, "open_seal", 800.0 - fbt / 1e6 + _fund_of(z) / 1e12)

    # 昨涨停今日表现：高开/强势优先（竞价情绪延续）
    for z in non_st_yzt:
        code = str(z.get("c") or "")
        chg = _zdp_of(z)
        # 高开或仍强
        if chg >= 3.0:
            row = dict(z)
            # map yesterday fields
            if row.get("fbt") is None:
                row["fbt"] = z.get("yfbt")
            if row.get("lbc") is None:
                row["lbc"] = z.get("ylbc")
            _push_auction(row, "prev_zt_strong", 500.0 + chg + _lbc_of(row) * 3.0)

    # clist 高开（开/昨收）
    for row in clist_map.values():
        code = str(row.get("f12") or "").strip()
        name = str(row.get("f14") or "").strip()
        if not code or _is_st_name(name):
            continue
        op = _num(row.get("f17"))
        prev = _num(row.get("f18"))
        chg = _num(row.get("f3")) or 0.0
        if op is None or prev is None or prev <= 0:
            continue
        open_pct = (op / prev - 1.0) * 100.0
        if open_pct < 5.0 and not (code in yzt_codes and open_pct >= 2.0):
            continue
        lim_pct = _board_limit_pct(code, name)
        # near open-limit
        score = 300.0 + open_pct * 3.0 + (5.0 if code in yzt_codes else 0.0) + float(chg) * 0.5
        if open_pct >= lim_pct * 0.95:
            score += 20.0
        _push_auction(
            _as_pool_row(code, name, zdp=chg, amount=row.get("f6"), hybk=row.get("f100"), fbt=92500 if open_pct >= lim_pct * 0.95 else 93000),
            "high_open",
            score,
        )

    auction_src = sorted(auction_map.values(), key=lambda z: float(z.get("_score") or -999), reverse=True)
    if len(auction_src) < 5:
        # fallback earliest seals
        extra = sorted(non_st_zt, key=_fbt_int)[: max(lim, 10)]
        for z in extra:
            _push_auction(z, "fallback_early_seal", 100.0 - _fbt_int(z) / 1e6)
        auction_src = sorted(auction_map.values(), key=lambda z: float(z.get("_score") or -999), reverse=True)
    auction_src = auction_src[:lim]

    # ---------- 风向标：连板梯队 + 题材龙头 + 少量即将 ----------
    wind_map: dict[str, dict[str, Any]] = {}

    def _push_wind(z: dict[str, Any], tag: str, score: float, rule: str) -> None:
        code = str(z.get("c") or "").strip()
        if not code:
            return
        name = str(z.get("n") or "").strip()
        if not name or _is_st_name(name):
            return
        item = dict(z)
        item["wind_tag"] = tag
        item["_score"] = score
        item["_rule"] = rule
        cur = wind_map.get(code)
        if cur is None or score > float(cur.get("_score") or -999):
            wind_map[code] = item

    # 连板高度梯队
    for z in non_st_zt:
        lbc = _lbc_of(z)
        score = lbc * 100.0 + _fund_of(z) / 1e8 + _amount_of(z) / 1e10
        tag = f"{lbc}板" if lbc >= 2 else "首板龙头"
        _push_wind(z, tag, score, "lianban_ladder")

    # 题材（hybk）涨停家数龙头
    theme_groups: dict[str, list[dict[str, Any]]] = {}
    for z in non_st_zt:
        th = str(z.get("hybk") or "").strip() or "其他"
        theme_groups.setdefault(th, []).append(z)
    for th, rows in theme_groups.items():
        if len(rows) < 2 and _lbc_of(rows[0]) < 2:
            continue
        leader = sorted(
            rows,
            key=lambda z: (_lbc_of(z), _fund_of(z), _amount_of(z)),
            reverse=True,
        )[0]
        score = 50.0 + len(rows) * 8.0 + _lbc_of(leader) * 20.0 + _fund_of(leader) / 1e9
        _push_wind(leader, f"{th[:6]}龙头", score, "theme_leader")

    # 即将高分少量进入风向
    for z in near_src[:5]:
        _push_wind(z, z.get("near_tag") or "发酵", 40.0 + float(z.get("_score") or 0) * 0.2, "near_seed")

    wind_src = sorted(wind_map.values(), key=lambda z: float(z.get("_score") or -999), reverse=True)[:lim]

    # ---------- 曾跌停：最低价触及跌停、现价已打开 ----------
    once_src: list[dict[str, Any]] = []
    for row in clist_map.values():
        code = str(row.get("f12") or "").strip()
        name = str(row.get("f14") or "").strip()
        if not code or not name or _is_st_name(name):
            continue
        if code in dt_codes:
            continue  # still limit-down -> belongs to 跌停 tab
        px = _num(row.get("f2"))
        low = _num(row.get("f16"))
        prev = _num(row.get("f18"))
        chg = _num(row.get("f3"))
        if px is None or low is None or prev is None or prev <= 0:
            continue
        lim_pct = _board_limit_pct(code, name)
        limit_dn = prev * (1.0 - lim_pct / 100.0)
        # touched limit-down intraday
        if low > limit_dn * 1.002:
            continue
        # opened / recovered
        if px <= limit_dn * 1.003:
            continue
        recover = (px - low) / prev * 100.0
        score = recover * 3.0 + (float(chg) if chg is not None else 0.0) + _amount_of({"amount": row.get("f6")}) / 1e10
        once_src.append(
            _as_pool_row(
                code,
                name,
                zdp=chg,
                amount=row.get("f6"),
                hybk=row.get("f100"),
                extra={
                    "once_tag": "曾跌停",
                    "_score": score,
                    "_rule": "low_hit_limit_dn",
                    "f16": low,
                    "f2": px,
                },
            )
        )
    once_src = sorted(once_src, key=lambda z: float(z.get("_score") or -999), reverse=True)[:lim]

    # 若 clist 空，用跌停池弱化兜底（仅展示已跌停，标记不同）
    if not once_src and non_st_dt:
        for z in sorted(non_st_dt, key=_amount_of, reverse=True)[: max(3, lim // 3)]:
            item = dict(z)
            item["once_tag"] = "跌停中"
            item["_rule"] = "fallback_dt"
            once_src.append(item)

    def _tab(key: str, label: str, src: list, rule: str = "") -> dict[str, Any]:
        items = []
        for x in src[:lim]:
            it = _pool_item_to_daban(x, key)
            if it:
                if rule and not it.get("rule"):
                    it["rule"] = rule
                items.append(it)
        return {
            "ok": bool(items),
            "key": key,
            "label": label,
            "items": items,
            "count": len(items),
            "day": day,
            "realtime": True,
            "source": "eastmoney",
            "rule": rule,
        }

    # ---------- 一字板 / 连板（从涨停池拆出，供打板观察） ----------
    yizi_src = sorted(
        [z for z in non_st_zt if _is_yizi_row(z)],
        key=lambda z: (_lbc_of(z), _fund_of(z), _amount_of(z)),
        reverse=True,
    )[:lim]
    for z in yizi_src:
        z["_rule"] = "yizi_fbt_zbc0"
    lianban_src = sorted(
        [z for z in non_st_zt if _lbc_of(z) >= 2],
        key=lambda z: (_lbc_of(z), _fund_of(z), _amount_of(z)),
        reverse=True,
    )[:lim]
    for z in lianban_src:
        z["_rule"] = "lianban_ge2"

    tabs = {
        "auction": _tab("auction", "竞价", auction_src, "early_seal+prevzt_high_open"),
        "near_limit": _tab("near_limit", "即将涨停", near_src, "gap_to_limit+speed+qs/zb"),
        "wind": _tab("wind", "风向标", wind_src, "lianban+theme_leader+near"),
        "limit_up": _tab("limit_up", "涨停", limit_up_src, "fund>lbc>fbt>amount"),
        "broken": _tab("broken", "炸板", broken_src, "amount*early*zbc"),
        "yizi": _tab("yizi", "一字", yizi_src, "early_seal_never_open"),
        "lianban": _tab("lianban", "连板", lianban_src, "lbc_ge2"),
        "natural": _tab("natural", "自然涨停", natural_src, "non_yizi_prefer_first"),
        "limit_down": _tab("limit_down", "跌停", limit_down_src, "dt_fund_amount"),
        "new_stock": _tab("new_stock", "新股", new_stock_src, "cx_ods_amount"),
        "once_down": _tab("once_down", "曾跌停", once_src, "intraday_low_limit_dn"),
    }
    any_ok = any(t.get("ok") for t in tabs.values())
    return {
        "ok": any_ok,
        "source": "eastmoney",
        "ruleset": "em_daban_v2",
        "day": day,
        "tabs": tabs,
        "tab_order": [
            "auction",
            "near_limit",
            "wind",
            "limit_up",
            "broken",
            "yizi",
            "lianban",
            "natural",
            "limit_down",
            "new_stock",
            "once_down",
        ],
        "note": "东财公开主题池+行情列表自研规则（非第三方同源）；竞价/风向/即将为近似口径",
    }


def _session_idx_to_hhmm(idx: int) -> str:
    """Map continuous-auction minute index 0..239 -> HH:MM."""
    try:
        i = int(idx)
    except Exception:
        i = 0
    i = max(0, min(239, i))
    if i <= 120:
        mins = 9 * 60 + 30 + i
    else:
        mins = 13 * 60 + (i - 120)
    return f"{mins // 60:02d}:{mins % 60:02d}"


def _ratio_at_idx(curve: dict[int, float], idx: int) -> Optional[float]:
    if not curve:
        return None
    try:
        i = int(idx)
    except Exception:
        return None
    if i in curve:
        return float(curve[i])
    prev = [k for k in curve if k <= i]
    if prev:
        return float(curve[max(prev)])
    nxt = [k for k in curve if k >= i]
    if nxt:
        return float(curve[min(nxt)])
    return None


def _cum_at_idx(today_cum: dict[int, float], idx: int) -> Optional[float]:
    if not today_cum:
        return None
    prev = [k for k in today_cum if k <= idx]
    if not prev:
        return None
    try:
        return float(today_cum[max(prev)])
    except Exception:
        return None


def _build_vs_prev_pace(
    *,
    prev_base: Optional[float],
    today_cum: dict[int, float],
    amount_now: Optional[float],
    curve: dict[int, float],
    now_hhmm: str,
    progress: float,
    sample_every: int = 5,
) -> dict[str, Any]:
    """Build intraday 放量/缩量 derivatives time-series.

    Layering (relative to yesterday full-day baseline):
      0) implied_full / pace: level of today vs yesterday
      1) delta_yi = implied_full - prev  → 放量/缩量 (first order vs 昨)
      2) speed_yi_10m = d(delta)/dt      → 放缩速度 (second order)

    speed > 0: 放量在加强 / 缩量在减弱
    speed < 0: 放量在减弱 / 缩量在加强
    speed ≈ 0: 势头不变

    No linear fallback: if curve ratio missing, point is skipped.
    """
    out: dict[str, Any] = {
        "ok": False,
        "unit": "yi",
        "sample_every": int(sample_every),
        "points": [],
        "latest": None,
        "note": "delta=推全天-昨全天(一阶)；speed=Δdelta/10分钟(二阶/放缩速度)",
        "y_metric": "speed_yi_10m",
        "y_unit": "yi_per_10m",
    }
    if prev_base is None or float(prev_base) <= 0:
        out["error"] = "no_prev_base"
        return out

    prev_base = float(prev_base)
    now_idx = _hhmm_to_session_idx(now_hhmm) or 0
    if progress >= 0.99:
        now_idx = 239

    path = _scale_today_cum(dict(today_cum or {}), amount_now, now_hhmm)
    if not path and amount_now is not None and amount_now > 0:
        path = {now_idx: float(amount_now)}

    step = max(1, int(sample_every or 5))
    idxs = list(range(0, now_idx + 1, step))
    if not idxs or idxs[-1] != now_idx:
        idxs.append(now_idx)

    pts: list[dict[str, Any]] = []
    for i in idxs:
        ratio = _ratio_at_idx(curve, i)
        cum = _cum_at_idx(path, i)
        if progress >= 0.99 and i >= 235 and amount_now is not None:
            implied = float(amount_now)
            ratio = 1.0
        else:
            if cum is None or cum <= 0:
                continue
            if ratio is None or ratio < 0.06:
                continue
            ratio = max(0.06, min(0.995, float(ratio)))
            implied = float(cum) / ratio
        delta = implied - prev_base
        delta_yi = delta / 1e8
        pace = implied / prev_base if prev_base else None
        pts.append(
            {
                "t": _session_idx_to_hhmm(i),
                "idx": i,
                "ratio": round(float(ratio), 4) if ratio is not None else None,
                "cum_yi": round(float(cum) / 1e8, 2) if cum is not None else None,
                "implied_yi": round(implied / 1e8, 2),
                "delta_yi": round(delta_yi, 2),
                "pace": round(float(pace), 4) if pace is not None else None,
                "csbl": round((float(pace) - 1.0) * 100.0, 2) if pace is not None else None,
                "speed_yi_10m": None,
            }
        )

    if not pts:
        out["error"] = "no_points"
        return out

    # Second derivative: change of delta over ~10 session minutes (not just adjacent sample).
    lookback_min = 10
    for j, p in enumerate(pts):
        i0 = int(p["idx"])
        target = i0 - lookback_min
        base = None
        for k in range(j - 1, -1, -1):
            if int(pts[k]["idx"]) <= target:
                base = pts[k]
                break
        # if sparse, fall back to at least 2 samples back
        if base is None and j >= 2:
            base = pts[j - 2]
        elif base is None and j >= 1:
            base = pts[j - 1]
        if base is None:
            continue
        di = int(p["idx"]) - int(base["idx"])
        if di <= 0:
            continue
        if p.get("delta_yi") is None or base.get("delta_yi") is None:
            continue
        # skip ultra-early noisy open (first ~15 trading minutes)
        if i0 < 15:
            continue
        speed = (float(p["delta_yi"]) - float(base["delta_yi"])) / float(di) * 10.0
        p["speed_yi_10m"] = round(float(speed), 2)

    # EMA smooth for display stability (chart primary series)
    alpha = 0.45
    ema = None
    for p in pts:
        s = p.get("speed_yi_10m")
        if s is None:
            p["speed_smooth_yi_10m"] = None
            continue
        ema = float(s) if ema is None else (alpha * float(s) + (1.0 - alpha) * ema)
        p["speed_smooth_yi_10m"] = round(float(ema), 2)

    latest = pts[-1]
    latest_speed = None
    latest_speed_raw = None
    for p in reversed(pts):
        if latest_speed is None and p.get("speed_smooth_yi_10m") is not None:
            latest_speed = p["speed_smooth_yi_10m"]
        if latest_speed_raw is None and p.get("speed_yi_10m") is not None:
            latest_speed_raw = p["speed_yi_10m"]
        if latest_speed is not None and latest_speed_raw is not None:
            break

    direction = "flat"
    d0 = latest.get("delta_yi")
    if d0 is not None:
        if d0 > 1:
            direction = "expand"
        elif d0 < -1:
            direction = "shrink"

    # Chinese labels for UI
    if direction == "expand":
        level_label = "放量"
    elif direction == "shrink":
        level_label = "缩量"
    else:
        level_label = "持平"

    spd = latest_speed if latest_speed is not None else latest_speed_raw
    if spd is None:
        speed_dir = "flat"
        speed_label = "势头不明"
    elif spd > 8:
        speed_dir = "accel_up"
        speed_label = "放量加速" if direction != "shrink" else "缩量减速"
    elif spd > 1.5:
        speed_dir = "up"
        speed_label = "放量加强" if direction != "shrink" else "缩量收敛"
    elif spd < -8:
        speed_dir = "accel_down"
        speed_label = "缩量加速" if direction != "expand" else "放量减速"
    elif spd < -1.5:
        speed_dir = "down"
        speed_label = "缩量加强" if direction != "expand" else "放量收敛"
    else:
        speed_dir = "flat"
        speed_label = "势头平稳"

    out.update(
        {
            "ok": True,
            "points": pts,
            "latest": {
                **latest,
                "speed_yi_10m": latest_speed if latest_speed is not None else latest_speed_raw,
                "speed_raw_yi_10m": latest_speed_raw,
                "direction": direction,
                "speed_dir": speed_dir,
                "level_label": level_label,
                "speed_label": speed_label,
                "prev_yi": round(prev_base / 1e8, 2),
            },
            "count": len(pts),
        }
    )
    return out


def _prev_trade_day_key(day_key: str, lookback: int = 12) -> Optional[str]:
    """Find a previous calendar day with non-empty ZT pool (proxy for trade day)."""
    try:
        base = datetime.strptime(str(day_key)[:8], "%Y%m%d")
    except Exception:
        return None
    for i in range(1, max(2, int(lookback)) + 1):
        d = (base - timedelta(days=i)).strftime("%Y%m%d")
        try:
            pool = _fetch_topic_pool(EM_ZT_HOSTS, d, 5, "fund:desc")
            if pool:
                return d
        except Exception:
            continue
    # fallback calendar previous weekday
    for i in range(1, 8):
        d = base - timedelta(days=i)
        if d.weekday() < 5:
            return d.strftime("%Y%m%d")
    return None


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
    # EM 上证/深成指 f6 = exchange-wide 两市口径. hithink/yixin index amounts can be
    # component-scale (esp. SZ) — only fill gaps, never override healthy EM amounts.
    need_backup = False
    for code in ("000001", "399001", "899050", "399006", "000688"):
        row = by_code.get(code) or {}
        if _num(row.get("f6")) is None:
            need_backup = True
            break
    tx_amt = _tencent_index_amounts() if need_backup else {"ok": False}
    sn_amt = _sina_index_amounts() if need_backup else {"ok": False}
    ht_amt = {"ok": False}  # hithink/问财 disabled for fund-flow market backup
    yx_amt = _yixin_index_amounts() if (need_backup and _yixin_api_key()) else {"ok": False}

    def _apply_backup(code: str, item: dict[str, Any], src: str) -> None:
        nonlocal amount_source
        if not item:
            return
        amt = item.get("amount")
        if amt is None:
            return
        try:
            if float(amt) < 1e9:
                return
        except (TypeError, ValueError):
            return
        row = by_code.get(code) or {}
        em_amt = _num(row.get("f6"))
        if em_amt is not None and em_amt > 0:
            return
        if code not in by_code:
            by_code[code] = {"f12": code, "f14": item.get("name") or code}
        by_code[code]["f6"] = amt
        if item.get("change_pct") is not None and by_code[code].get("f3") is None:
            by_code[code]["f3"] = item.get("change_pct")
        if item.get("price") is not None and by_code[code].get("f2") is None:
            by_code[code]["f2"] = item.get("price")
        if item.get("name") and not by_code[code].get("f14"):
            by_code[code]["f14"] = item.get("name")
        if amount_source == "eastmoney":
            amount_source = "mixed"
        elif amount_source == "none":
            amount_source = src
        elif amount_source not in (src, "mixed"):
            amount_source = "mixed"

    for code, key in (
        ("000001", "sh"),
        ("399001", "sz"),
        ("899050", "bj"),
        ("399006", "cyb"),
        ("000688", "kc50"),
    ):
        # Prefer free, server-reachable public quotes when EM is blocked.
        _apply_backup(code, (tx_amt or {}).get(key) or {}, "tencent")
        _apply_backup(code, (sn_amt or {}).get(key) or {}, "sina")
        # hithink backup disabled for fund-flow paths
        _apply_backup(code, (yx_amt or {}).get(key) or {}, "yixin")

    now_hhmm = (progress.get("asof_time") or "09:30")[:5]
    today = progress.get("day") or _now().strftime("%Y-%m-%d")
    prof = _get_volume_profiles(today)
    sh_curve = prof.get("sh") or {}
    sz_curve = prof.get("sz") or {}
    hs_curve = prof.get("hs") or {}
    prev_day = prof.get("prev_day")
    prev_hs = prof.get("prev_hs")
    prev_bj = prof.get("prev_bj")
    prev_total = prof.get("prev_total")
    if prev_total is None and prev_hs is not None:
        prev_total = float(prev_hs) + (float(prev_bj) if prev_bj is not None and float(prev_bj) > 0 else 0.0)
    method_label = str(prof.get("method_label") or "profile")

    today_cum_sh = prof.get("today_cum_sh") or {}
    today_cum_sz = prof.get("today_cum_sz") or {}
    today_cum_hs = prof.get("today_cum_hs") or {}
    self_cal_meta: dict[str, Any] = {}

    def _idx(
        code: str,
        name_fallback: str,
        curve: Optional[dict[int, float]] = None,
        today_cum: Optional[dict[int, float]] = None,
    ) -> dict[str, Any]:
        row = by_code.get(code) or {}
        amt = _num(row.get("f6"))
        use_curve = curve if curve is not None else hs_curve
        pred, method, ratio, meta = _predict_with_self_cal(
            amt, now_hhmm, use_curve, today_cum or {}, p, method_label=method_label
        )
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

    sh = _idx("000001", "上证指数", sh_curve, today_cum_sh)
    sz = _idx("399001", "深证成指", sz_curve, today_cum_sz)
    cyb = _idx("399006", "创业板指", sz_curve, today_cum_sz)
    kc = _idx("000680", "科创综指", sh_curve, today_cum_sh)  # 科创板全市场成交额口径
    kc50 = _idx("000688", "科创50", sh_curve, today_cum_sh)
    bj50 = _idx("899050", "北证50", hs_curve, today_cum_hs)

    bj_amt = _bj_market_amount()
    if bj_amt is None:
        bj_amt = bj50.get("amount")
    bj_pred, bj_method, bj_ratio, _bj_meta = _predict_with_self_cal(
        bj_amt, now_hhmm, hs_curve, today_cum_hs, p, method_label=method_label
    )
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
    hs_pred, hs_method, hs_ratio, self_cal_meta = _predict_with_self_cal(
        hs_amt, now_hhmm, hs_curve, today_cum_hs, p, method_label=method_label
    )
    total_pred, total_method, total_ratio, _tot_meta = _predict_with_self_cal(
        total_amt, now_hhmm, hs_curve, today_cum_hs, p, method_label=method_label
    )
    if not self_cal_meta.get("self_cal") and _tot_meta.get("self_cal"):
        self_cal_meta = _tot_meta

    # Prefer absolute 3-day same-time baseline when available (formula:
    # predict = cum * s3_full / s3_cum[t]). Blend lightly with path self-cal.
    s3_cum = prof.get("hs_s3_cum") or {}
    s3_full = prof.get("hs_s3_full")
    s3_n = int(prof.get("hs_s3_days") or 0)
    if s3_n >= 2 and s3_cum and s3_full:
        pred3, m3, r3 = _predict_by_abs_3d(hs_amt, now_hhmm, s3_cum, float(s3_full), p)
        if pred3 is not None:
            if hs_pred is not None and self_cal_meta.get("self_cal") and p >= 0.12:
                w = float(self_cal_meta.get("weight") or 0.25)
                w = max(0.0, min(0.55, w))
                hs_pred = (1.0 - w) * float(pred3) + w * float(hs_pred)
                hs_method = "profile_3d_selfcal"
            else:
                hs_pred, hs_method = float(pred3), m3
            hs_ratio = r3
        pred3t, m3t, r3t = _predict_by_abs_3d(total_amt, now_hhmm, s3_cum, float(s3_full), p)
        if pred3t is not None:
            if total_pred is not None and _tot_meta.get("self_cal") and p >= 0.12:
                w = float(_tot_meta.get("weight") or 0.25)
                w = max(0.0, min(0.55, w))
                total_pred = (1.0 - w) * float(pred3t) + w * float(total_pred)
                total_method = "profile_3d_selfcal"
            else:
                total_pred, total_method = float(pred3t), m3t
            total_ratio = r3t

    # vs_prev: align with media HS+BJ total (SH+SZ+BJ50)
    # intraday = full-day predict(total) vs previous complete day; after close use actual total
    prev_base = prev_total if prev_total is not None else prev_hs
    vs_prev: dict[str, Any] = {
        "prev_day": prev_day,
        # frontend still reads prev_hs_amount_yi; value is now HS+BJ baseline
        "prev_hs_amount": prev_base,
        "prev_hs_amount_yi": _yi(prev_base) if prev_base is not None else None,
        "prev_hs_only_amount": prev_hs,
        "prev_hs_only_amount_yi": _yi(prev_hs) if prev_hs is not None else None,
        "prev_bj_amount": prev_bj,
        "prev_bj_amount_yi": _yi(prev_bj) if prev_bj is not None else None,
        "prev_total_amount": prev_base,
        "prev_total_amount_yi": _yi(prev_base) if prev_base is not None else None,
        "prev_scope": "hs_bj" if (prev_bj is not None and float(prev_bj) > 0) else "hs",
        "basis": None,
        "today_ref": None,
        "today_ref_yi": None,
        "delta": None,
        "delta_yi": None,
        "direction": None,  # expand / shrink / flat
        "label": None,
    }
    if prev_base is not None and prev_base > 0:
        if p >= 0.99:
            today_ref = float(total_amt)  # closed: actual HS+BJ
            basis = "actual_total"
        elif total_pred is not None:
            today_ref = float(total_pred)
            basis = "predict_total"
        elif hs_pred is not None:
            today_ref = float(hs_pred) + float(bj_pred or bj_a or 0.0)
            basis = "predict_hs_plus_bj"
        else:
            today_ref = None
            basis = None
        if today_ref is not None:
            delta = today_ref - float(prev_base)
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

    # 放量/缩量 pace 时序（用于前端小图）
    try:
        vs_prev_pace = _build_vs_prev_pace(
            prev_base=prev_base if prev_base is not None else prev_hs,
            today_cum=today_cum_hs or {},
            amount_now=total_amt if total_amt else hs_amt,
            curve=hs_curve or {},
            now_hhmm=now_hhmm,
            progress=float(p or 0.0),
            sample_every=5,
        )
    except Exception as e:
        log.warning("vs_prev_pace failed: %s", e)
        vs_prev_pace = {"ok": False, "error": str(e), "points": []}

    day_key = _now().strftime("%Y%m%d")
    day_iso = _now().strftime("%Y-%m-%d")
    zt_pool, dt_pool, zb_pool, qs_pool = [], [], [], []
    cx_pool, yzt_pool = [], []
    clist_up, clist_speed, clist_down = [], [], []

    with ThreadPoolExecutor(max_workers=9) as pool:
        f_zt = pool.submit(_fetch_topic_pool, EM_ZT_HOSTS, day_key, 500, "fund:desc")
        f_dt = pool.submit(_fetch_topic_pool, EM_DT_HOSTS, day_key, 200, "fund:desc")
        f_zb = pool.submit(_fetch_topic_pool, EM_ZB_HOSTS, day_key, 200, "amount:desc")
        f_qs = pool.submit(_fetch_topic_pool, EM_QS_HOSTS, day_key, 200, "zdp:desc")
        f_cx = pool.submit(_fetch_topic_pool, EM_CX_HOSTS, day_key, 200, "ods:asc")
        f_yzt = pool.submit(_fetch_topic_pool, EM_YZT_HOSTS, day_key, 200, "zs:desc")
        f_cu = pool.submit(_em_clist_scan, pagesize=200, sort="f3:desc")
        f_cs = pool.submit(_em_clist_scan, pagesize=120, sort="f22:desc")
        f_cd = pool.submit(_em_clist_scan, pagesize=200, sort="f3:asc")
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
        try:
            qs_pool = f_qs.result()
        except Exception as e:
            log.warning("qs pool: %s", e)
        try:
            cx_pool = f_cx.result()
        except Exception as e:
            log.warning("cx pool: %s", e)
        if not cx_pool:
            try:
                cx_pool = _fetch_topic_pool(EM_CX_HOSTS_ALT, day_key, 200, "ods:asc")
            except Exception as e:
                log.warning("cx alt pool: %s", e)
        try:
            yzt_pool = f_yzt.result()
        except Exception as e:
            log.warning("yzt pool: %s", e)
        try:
            clist_up = f_cu.result() or []
        except Exception as e:
            log.warning("clist up: %s", e)
        try:
            clist_speed = f_cs.result() or []
        except Exception as e:
            log.warning("clist speed: %s", e)
        try:
            clist_down = f_cd.result() or []
        except Exception as e:
            log.warning("clist down: %s", e)

    zt = _pool_stats(zt_pool)
    dt = _pool_stats(dt_pool)
    zb = _pool_stats(zb_pool)

    # 昨日涨停/跌停/炸板家数（用于 今日/昨日 展示）
    prev_zt_n = prev_dt_n = prev_zb_n = None
    prev_day_key = None
    try:
        prev_day_key = _prev_trade_day_key(day_key)
        if prev_day_key:
            with ThreadPoolExecutor(max_workers=3) as ppool:
                p_zt = ppool.submit(_fetch_topic_pool, EM_ZT_HOSTS, prev_day_key, 500, "fund:desc")
                p_dt = ppool.submit(_fetch_topic_pool, EM_DT_HOSTS, prev_day_key, 200, "fund:desc")
                p_zb = ppool.submit(_fetch_topic_pool, EM_ZB_HOSTS, prev_day_key, 200, "amount:desc")
                try:
                    prev_zt_n = _pool_stats(p_zt.result() or []).get("count")
                except Exception as e:
                    log.warning("prev zt: %s", e)
                try:
                    prev_dt_n = _pool_stats(p_dt.result() or []).get("count")
                except Exception as e:
                    log.warning("prev dt: %s", e)
                try:
                    prev_zb_n = _pool_stats(p_zb.result() or []).get("count")
                except Exception as e:
                    log.warning("prev zb: %s", e)
    except Exception as e:
        log.warning("prev day pools: %s", e)

    if isinstance(zt, dict):
        zt = dict(zt)
        zt["prev_count"] = prev_zt_n
        zt["prev_day"] = prev_day_key
    if isinstance(dt, dict):
        dt = dict(dt)
        dt["prev_count"] = prev_dt_n
        dt["prev_day"] = prev_day_key
    if isinstance(zb, dict):
        zb = dict(zb)
        zb["prev_count"] = prev_zb_n
        zb["prev_day"] = prev_day_key

    daban_bundle = _build_daban_from_em(
        zt_pool,
        zb_pool,
        qs_pool,
        dt_pool=dt_pool,
        cx_pool=cx_pool,
        yzt_pool=yzt_pool,
        clist_up=clist_up,
        clist_speed=clist_speed,
        clist_down=clist_down,
        limit=500,
        day=day_iso,
    )
    strength = _compute_sentiment_strength(zt, zb, sh, sz, day=day_iso)

    volume_primary = "eastmoney"
    sources = ["eastmoney"]
    if amount_source not in ("eastmoney", "none", None):
        sources.append(str(amount_source))

    result = {
        "ok": True,
        "cached": False,
        "stale": False,
        "asof": _now_iso(),
        "source": "+".join(dict.fromkeys(sources)),
        "volume_primary": volume_primary,
        "session": progress,
        "strength": strength,
        "volume": {
            "unit": "yi",
            "sh": sh,
            "sz": sz,
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
                "source": volume_primary,
            },
            "total": {
                "name": "沪深京合计",
                "amount": total_amt,
                "amount_yi": _yi(total_amt),
                "predict_amount": total_pred,
                "predict_amount_yi": _yi(total_pred) if total_pred is not None else None,
                "predict_method": total_method,
                "profile_ratio": round(total_ratio, 4) if total_ratio is not None else None,
                "source": volume_primary,
            },
            "method": (
                "实际合计=东财指数/全市场口径（腾讯/新浪/问财兜底）；"
                "分市场拆解(上证/科创/深成/创业/北交)=东财既有口径(+备份)；"
                "预测=近3日同时点绝对基准外推(predict=cum*s3_full/s3_cum)，并与今日分时自校准混合"
            ),
            "predict_confidence": _predict_confidence(
                p,
                method=(total_method if total_method not in (None, "none") else "unavailable"),
            ),
            "profile_minutes": len(hs_curve),
            "profile_source": prof.get("profile_source") or ("eastmoney" if hs_curve else "none"),
            "prev_source": prof.get("prev_source") or "none",
            "amount_source": amount_source,
            "volume_primary": volume_primary,
            "asof_hhmm": now_hhmm,
            "self_cal": {
                "enabled": bool(self_cal_meta.get("self_cal")),
                "weight": self_cal_meta.get("weight"),
                "fit_points": self_cal_meta.get("fit_points") or prof.get("today_path_points"),
                "pred_hist_yi": _yi(self_cal_meta.get("pred_hist")) if self_cal_meta.get("pred_hist") is not None else None,
                "pred_path_yi": _yi(self_cal_meta.get("pred_path")) if self_cal_meta.get("pred_path") is not None else None,
                "s3_days": prof.get("hs_s3_days"),
                "s3_full_yi": _yi(prof.get("hs_s3_full")) if prof.get("hs_s3_full") is not None else None,
            },
            "vs_prev": vs_prev,
            "vs_prev_pace": vs_prev_pace,
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
                "limit_up_prev": zt.get("prev_count"),
                "limit_down_prev": dt.get("prev_count"),
                "broken_prev": zb.get("prev_count"),
                "prev_day": prev_day_key,
            },
            "note": "涨停/跌停/炸板统计=东财主题池（剔 ST）；下方打板明细=东财主题池+行情列表自研规则 v2",
        },
        "daban": daban_bundle if isinstance(daban_bundle, dict) else {"ok": False, "tabs": {}},
        "structure": fund_structure.market_structure(refresh=refresh),
    }
    _cache_set(cache_key, result, ttl=_MARKET_TTL)
    return result

