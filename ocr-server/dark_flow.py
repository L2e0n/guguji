#!/usr/bin/env python3
"""暗盘 / 明盘资金（同花顺问财 OpenAPI / hithink-market-query）。

API: POST https://openapi.iwencai.com/v1/query2data
Auth: Bearer IWENCAI_API_KEY + X-Claw-* headers
"""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Optional
from urllib import error as urlerror
from urllib import request as urlrequest

import fund_structure

log = logging.getLogger("dark-flow")

API_URL = "https://openapi.iwencai.com/v1/query2data"
SKILL_ID = "hithink-market-query"
SKILL_VERSION = "1.0.0"
CST = timezone(timedelta(hours=8))
YI = 1e8

# sort -> natural language query for 问财
SORT_QUERIES = {
    "dark_in": "主力暗盘资金,主力明盘资金,DDX,DDY,DDZ,主力增仓占比,最新价,涨跌幅 按主力暗盘资金从大到小排序",
    "dark_out": "主力暗盘资金,主力明盘资金,DDX,DDY,DDZ,主力增仓占比,最新价,涨跌幅 按主力暗盘资金从小到大排序",
    "light_in": "主力暗盘资金,主力明盘资金,DDX,DDY,DDZ,主力增仓占比,最新价,涨跌幅 按主力资金流向从大到小排序",
    "light_out": "主力暗盘资金,主力明盘资金,DDX,DDY,DDZ,主力增仓占比,最新价,涨跌幅 按主力资金流向从小到大排序",
}

_cache: dict[str, tuple[float, float, dict]] = {}
CACHE_TTL = 45.0
CACHE_STALE = 600.0  # 10 min last-good on 问财 failure


def _now_asof() -> str:
    return datetime.now(CST).strftime("%Y-%m-%dT%H:%M:%S+08:00")


def has_api_key() -> bool:
    return bool(os.environ.get("IWENCAI_API_KEY", "").strip())


def _headers(call_type: str = "normal") -> dict[str, str]:
    key = os.environ.get("IWENCAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError("IWENCAI_API_KEY 未配置")
    return {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "X-Claw-Call-Type": call_type,
        "X-Claw-Skill-Id": SKILL_ID,
        "X-Claw-Skill-Version": SKILL_VERSION,
        "X-Claw-Plugin-Id": "none",
        "X-Claw-Plugin-Version": "none",
        "X-Claw-Trace-Id": secrets.token_hex(32),
    }


def _query2data(query: str, page: int = 1, limit: int = 30, timeout: int = 35) -> dict:
    payload = {
        "query": query,
        "page": str(page),
        "limit": str(limit),
        "is_cache": "1",
        "expand_index": "true",
    }
    req = urlrequest.Request(
        API_URL,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=_headers("normal"),
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", "replace")
    except urlerror.HTTPError as e:
        err = e.read().decode("utf-8", "replace") if e.fp else ""
        raise RuntimeError(f"问财网关 HTTP {e.code}: {err[:300]}") from e
    except urlerror.URLError as e:
        raise RuntimeError(f"问财网关网络错误: {e.reason}") from e

    if not body.strip():
        return {}
    try:
        data = json.loads(body)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"问财响应非 JSON: {body[:200]}") from e
    if not isinstance(data, dict):
        raise RuntimeError("问财响应格式异常")
    if "datas" not in data and data.get("status_code") not in (None, 0, 200, "0", "200"):
        # gateway error
        msg = data.get("message") or data.get("msg") or data.get("error") or str(data)[:200]
        raise RuntimeError(f"问财网关错误: {msg}")
    return data


def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if not s or s.lower() in ("nan", "none", "--", "-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _pick(row: dict, *names: str) -> Any:
    """按字段名前缀/包含关系取值（问财字段常带日期后缀）。"""
    if not row:
        return None
    # exact
    for n in names:
        if n in row and row[n] not in (None, ""):
            return row[n]
    # prefix with [
    for n in names:
        for k, v in row.items():
            if k == n or k.startswith(n + "[") or k.startswith(n + ":"):
                if v not in (None, ""):
                    return v
    # fuzzy contains
    low_names = [n.lower() for n in names]
    for k, v in row.items():
        kl = str(k).lower()
        for n in low_names:
            if n in kl and v not in (None, ""):
                return v
    return None


def _code_plain(code: Any) -> str:
    s = str(code or "").strip().upper()
    s = s.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    s = re.sub(r"^(SH|SZ|BJ)", "", s)
    return s


def _normalize_row(row: dict) -> dict:
    dark = _to_float(_pick(row, "主力暗盘资金", "暗盘资金"))
    light = _to_float(
        _pick(row, "主力明盘资金", "明盘资金", "主力资金流向", "最新主力资金流向", "主力资金净流入")
    )
    ddx = _to_float(_pick(row, "ddx", "DDX", "大单动向"))
    ddy = _to_float(_pick(row, "ddy", "DDY", "涨跌动因"))
    ddz = _to_float(_pick(row, "ddz", "DDZ", "大单力度"))
    add_ratio = _to_float(_pick(row, "主力增仓占比", "增仓占比"))
    price = _to_float(_pick(row, "最新价", "收盘价", "现价"))
    chg = _to_float(_pick(row, "最新涨跌幅", "涨跌幅"))
    name = _pick(row, "股票简称", "名称", "name") or ""
    code_raw = _pick(row, "股票代码", "代码", "code") or ""
    code = _code_plain(code_raw)

    dark_yi = None if dark is None else dark / YI
    light_yi = None if light is None else light / YI

    signal = _signal(dark_yi, light_yi, ddx, ddy, add_ratio)

    return {
        "code": code,
        "code_raw": str(code_raw),
        "name": str(name),
        "price": price,
        "change_pct": chg,
        "dark_net": dark,
        "dark_net_yi": None if dark_yi is None else round(dark_yi, 4),
        "light_net": light,
        "light_net_yi": None if light_yi is None else round(light_yi, 4),
        "ddx": None if ddx is None else round(ddx, 4),
        "ddy": None if ddy is None else round(ddy, 4),
        "ddz": None if ddz is None else round(ddz, 4),
        "add_ratio": None if add_ratio is None else round(add_ratio, 4),
        "signal": signal,
    }


def _signal(dark_yi, light_yi, ddx, ddy, add_ratio) -> dict:
    dark_yi = dark_yi or 0
    ddx = ddx if ddx is not None else 0
    ddy = ddy if ddy is not None else 0
    if dark_yi > 0 and ddx > 0 and ddy > 0:
        return {"code": "strong_buy", "label": "吸筹共振", "tone": "up"}
    if dark_yi > 0 and ddx > 0:
        return {"code": "inflow", "label": "暗盘吸筹", "tone": "up"}
    if dark_yi > 0 and ddy < 0:
        return {"code": "warn", "label": "暗吸散接", "tone": "warn"}
    if dark_yi < 0 and ddx < 0:
        return {"code": "flee", "label": "暗盘派发", "tone": "down"}
    if dark_yi < 0:
        return {"code": "outflow", "label": "暗盘流出", "tone": "down"}
    if ddx > 0:
        return {"code": "ddx_up", "label": "大单偏多", "tone": "up"}
    if ddx < 0:
        return {"code": "ddx_down", "label": "大单偏空", "tone": "down"}
    return {"code": "neutral", "label": "中性", "tone": ""}


def health() -> dict:
    ok_key = has_api_key()
    sample = None
    err = None
    if ok_key:
        try:
            data = rank(sort="dark_in", limit=3, refresh=True)
            sample = {
                "count": data.get("count"),
                "top": (data.get("items") or [{}])[0].get("name"),
            }
        except Exception as e:
            err = str(e)
    return {
        "ok": ok_key and err is None,
        "service": "dark-flow",
        "source": "iwencai/hithink-market-query",
        "api": API_URL,
        "has_api_key": ok_key,
        "sample": sample,
        "error": err,
        "asof": _now_asof(),
    }


def rank(sort: str = "dark_in", limit: int = 30, page: int = 1, refresh: bool = False) -> dict:
    sort = (sort or "dark_in").strip().lower()
    if sort not in SORT_QUERIES:
        sort = "dark_in"
    limit = max(1, min(int(limit or 30), 50))
    page = max(1, min(int(page or 1), 20))
    cache_key = f"rank:{sort}:{page}:{limit}"
    now = time.time()

    def _unpack(hit):
        if len(hit) == 3:
            return hit[0], hit[1], hit[2]
        stored_at, payload = hit
        return stored_at + CACHE_TTL, stored_at + CACHE_STALE, payload

    if not refresh and cache_key in _cache:
        exp, _stale_until, cached = _unpack(_cache[cache_key])
        if now <= exp:
            out = dict(cached)
            out["cached"] = True
            out["stale"] = False
            return out

    query = SORT_QUERIES[sort]
    try:
        raw_data = _query2data(query, page=page, limit=limit)
        datas = raw_data.get("datas") or []
        items = [_normalize_row(r) for r in datas if isinstance(r, dict)]
        asof_day = None
        if datas:
            for k in datas[0].keys():
                m = re.search(r"\[(20\d{6})\]", str(k))
                if m:
                    d = m.group(1)
                    asof_day = f"{d[:4]}-{d[4:6]}-{d[6:]}"
                    break

        result = {
            "ok": True,
            "sort": sort,
            "page": page,
            "limit": limit,
            "count": int(raw_data.get("code_count") or len(items)),
            "returned": len(items),
            "items": items,
            "source": "iwencai",
            "query": query,
            "asof": _now_asof(),
            "asof_day": asof_day,
            "cached": False,
            "stale": False,
        }
        _cache[cache_key] = (now + CACHE_TTL, now + CACHE_STALE, result)
        return result
    except Exception as e:
        hit = _cache.get(cache_key)
        if hit:
            _exp, stale_until, cached = _unpack(hit)
            if now <= stale_until and isinstance(cached, dict):
                out = dict(cached)
                out["ok"] = True
                out["cached"] = True
                out["stale"] = True
                out["error"] = str(e)
                log.warning("dark rank fallback to stale: %s", e)
                return out
        raise


def query_stock(q: str) -> dict:
    q = (q or "").strip()
    if not q:
        raise ValueError("查询词不能为空")
    # strip market suffix for cleaner query
    q_clean = re.sub(r"\.(SH|SZ|BJ)$", "", q, flags=re.I)
    query = f"{q_clean} 主力暗盘资金,主力明盘资金,DDX,DDY,DDZ,主力增仓占比,最新价,涨跌幅"
    raw = _query2data(query, page=1, limit=5)
    datas = raw.get("datas") or []
    items = [_normalize_row(r) for r in datas if isinstance(r, dict)]
    # prefer exact code/name match
    q_plain = _code_plain(q_clean)
    best = None
    for it in items:
        if it["code"] == q_plain or it["name"] == q_clean or q_clean in it["name"]:
            best = it
            break
    if best is None and items:
        best = items[0]

    asof_day = None
    if datas:
        for k in datas[0].keys():
            m = re.search(r"\[(20\d{6})\]", str(k))
            if m:
                d = m.group(1)
                asof_day = f"{d[:4]}-{d[4:6]}-{d[6:]}"
                break

    profile = None
    try:
        code_for_profile = (best or {}).get("code") if best else None
        if not code_for_profile:
            code_for_profile = _code_plain(q_clean)
        if code_for_profile:
            profile = fund_structure.stock_fund_profile(code_for_profile, refresh=False)
    except Exception as e:
        log.warning("stock fund profile failed: %s", e)
        profile = {"ok": False, "error": str(e)}

    return {
        "ok": True,
        "query": q,
        "item": best,
        "profile": profile,
        "candidates": items,
        "source": "iwencai+eastmoney",
        "asof": _now_asof(),
        "asof_day": asof_day,
    }
