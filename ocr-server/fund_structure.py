#!/usr/bin/env python3
"""结构资金（订单规模代理）+ 个股资金成分（分档/北向/两融/龙虎榜席位）。

口径说明（务必对外诚实）：
- 主力 = 超大单 + 大单净流入（东财分档，size proxy，不是身份）
- 散户 = 小单净流入（代理）
- 北向 = 沪深股通通道
- 两融 = 融资余额/净买入（日级）
- 龙虎榜席位 = 事件样本，含机构/北向/游资启发式标签
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests

log = logging.getLogger("fund-structure")

TZ_SH = timezone(timedelta(hours=8))
YI = 1e8

EM_ULIST = "https://push2.eastmoney.com/api/qt/ulist.np/get"
EM_KAMT = "https://push2.eastmoney.com/api/qt/kamt.rtmin/get"
EM_DC = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EM_UT = "fa5fd1943c7b386f172d6893dbfba10b"
EM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://data.eastmoney.com/",
    "Accept": "*/*",
}

# 常见游资席位关键词（启发式，非完整名录）
HOT_MONEY_KEYWORDS = [
    "拉萨夺多", "拉萨天团", "拉萨东环路", "拉萨东城大道", "深圳金田路",
    "深圳红岭中路", "成都北一环路", "上海陆家嘴", "上海安福路", "上海中山东二路",
    "华鑫上海茅台路", "国泰君安宁波", "国泰君安南京太平南路", "中山中山北路",
    "中国银河绍兴", "华泰证券上海武定路", "东方财富拉萨", "财通证券杭州",
]

_cache: dict[str, tuple[float, float, Any]] = {}
_CACHE_STALE = 300.0


def _now() -> datetime:
    return datetime.now(TZ_SH)


def _now_iso() -> str:
    return _now().isoformat(timespec="seconds")


def _cache_get(key: str, allow_stale: bool = False):
    item = _cache.get(key)
    if not item:
        return None
    if len(item) == 3:
        exp, stale_until, val = item
    else:
        exp, val = item
        stale_until = exp + _CACHE_STALE
    now = time.time()
    if now <= exp:
        return val
    if allow_stale and now <= stale_until:
        return val
    return None


def _cache_set(key: str, val: Any, ttl: float = 12.0, stale: float | None = None) -> None:
    now = time.time()
    stale_span = _CACHE_STALE if stale is None else stale
    _cache[key] = (now + ttl, now + max(ttl, stale_span), val)


def _num(v: Any) -> Optional[float]:
    if v is None or v == "-" or v == "":
        return None
    if isinstance(v, (int, float)):
        if v != v:
            return None
        return float(v)
    try:
        return float(str(v).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None


def _yi(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    return round(float(v) / YI, 4)


def _secid(code: str) -> Optional[str]:
    c = re.sub(r"\D", "", str(code or ""))
    if not c:
        return None
    if c.startswith(("5", "6", "9")):
        return f"1.{c}"
    if c.startswith(("0", "1", "2", "3")):
        return f"0.{c}"
    if c.startswith(("4", "8")):
        return f"0.{c}"
    return f"1.{c}"


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _bucket_pack(
    main: Optional[float],
    super_net: Optional[float],
    large: Optional[float],
    mid: Optional[float],
    small: Optional[float],
    main_ratio_pct: Optional[float] = None,
) -> dict[str, Any]:
    """统一分档结构。单位：元 + 亿。"""
    super_v = super_net if super_net is not None else 0.0
    large_v = large if large is not None else 0.0
    mid_v = mid if mid is not None else 0.0
    small_v = small if small is not None else 0.0
    # 主力 = 超大+大；若缺分档则退回 main
    if super_net is None and large is None:
        force = main
    else:
        force = super_v + large_v
        if main is not None and force == 0 and main != 0:
            force = main
    if main is None:
        main = force

    abs_sum = abs(super_v) + abs(large_v) + abs(mid_v) + abs(small_v)
    size_main_share = None
    if abs_sum > 0:
        size_main_share = round((abs(super_v) + abs(large_v)) / abs_sum * 100, 2)

    scissors = None
    if force is not None and small is not None:
        scissors = force - small_v

    return {
        "identity": "size_proxy",
        "unit": "yi",
        "main_net": main,
        "main_net_yi": _yi(main),
        "force_net": force,  # 超大+大
        "force_net_yi": _yi(force),
        "super_net": super_net,
        "super_net_yi": _yi(super_net),
        "large_net": large,
        "large_net_yi": _yi(large),
        "mid_net": mid,
        "mid_net_yi": _yi(mid),
        "retail_net": small,
        "retail_net_yi": _yi(small),
        "scissors": scissors,  # 主力-散户
        "scissors_yi": _yi(scissors),
        "main_ratio_pct": main_ratio_pct,  # 东财主力净比 f184
        "size_main_share_pct": size_main_share,  # |超大+大| / 分档绝对值合计
        "note": "主力/散户为成交额分档代理，非账户身份",
    }


def _fetch_ulist(secids: str, fields: str) -> list[dict[str, Any]]:
    sess = _session()
    params = {
        "fltt": "2",
        "secids": secids,
        "fields": fields,
        "ut": EM_UT,
        "_": int(time.time() * 1000),
    }
    for url in (EM_ULIST, "https://push2delay.eastmoney.com/api/qt/ulist.np/get"):
        try:
            r = sess.get(url, params=params, headers=EM_HEADERS, timeout=10)
            r.raise_for_status()
            diff = ((r.json().get("data") or {}).get("diff")) or []
            if diff:
                return diff
        except Exception as e:
            log.warning("ulist failed %s: %s", url, e)
    return []


def _parse_kamt_last(series: list) -> Optional[dict[str, Any]]:
    """Parse kamt.rtmin s2n line: time,sh_net,sh_quota,sz_net,sz_quota,total_net (万元)."""
    if not series:
        return None
    for line in reversed(series):
        if not isinstance(line, str):
            continue
        parts = line.split(",")
        if len(parts) < 6:
            continue
        t = parts[0]
        vals = []
        ok = True
        for p in parts[1:6]:
            if p in ("-", "", None):
                ok = False
                break
            try:
                vals.append(float(p))
            except ValueError:
                ok = False
                break
        if not ok:
            continue
        # 万元 -> 元
        sh_net, sh_q, sz_net, sz_q, total = [v * 10000.0 for v in vals]
        return {
            "time": t,
            "sh_net": sh_net,
            "sz_net": sz_net,
            "total_net": total if abs(total) > 0 else (sh_net + sz_net),
            "sh_quota": sh_q,
            "sz_quota": sz_q,
        }
    return None


def fetch_northbound_rt() -> dict[str, Any]:
    cached = _cache_get("nb_rt")
    if cached is not None:
        return cached
    sess = _session()
    params = {
        "fields1": "f1,f2,f3,f4",
        "fields2": "f51,f52,f53,f54,f55,f56",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "_": int(time.time() * 1000),
    }
    out: dict[str, Any] = {
        "ok": False,
        "identity": "channel",
        "name": "北向资金",
        "asof_time": None,
        "total_net": None,
        "total_net_yi": None,
        "sh_net_yi": None,
        "sz_net_yi": None,
        "date": None,
    }
    try:
        r = sess.get(EM_KAMT, params=params, headers=EM_HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json().get("data") or {}
        parsed = _parse_kamt_last(data.get("s2n") or [])
        out["date"] = data.get("s2nDate")
        if parsed:
            total = parsed["total_net"]
            out.update(
                {
                    "ok": True,
                    "asof_time": parsed["time"],
                    "total_net": total,
                    "total_net_yi": _yi(total),
                    "sh_net": parsed["sh_net"],
                    "sh_net_yi": _yi(parsed["sh_net"]),
                    "sz_net": parsed["sz_net"],
                    "sz_net_yi": _yi(parsed["sz_net"]),
                    "sh_quota_yi": _yi(parsed["sh_quota"]),
                    "sz_quota_yi": _yi(parsed["sz_quota"]),
                }
            )
    except Exception as e:
        log.warning("northbound rt failed: %s", e)
        out["error"] = str(e)
    _cache_set("nb_rt", out, ttl=20.0)
    return out


def market_structure(refresh: bool = False) -> dict[str, Any]:
    """全市场结构资金：上证+深成指分档加总。"""
    cache_key = "market_structure"
    if not refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            out = dict(cached)
            out["cached"] = True
            out["stale"] = False
            return out
    try:
        return _market_structure_fresh(cache_key)
    except Exception as e:
        stale = _cache_get(cache_key, allow_stale=True)
        if stale and isinstance(stale, dict):
            out = dict(stale)
            out["ok"] = True
            out["cached"] = True
            out["stale"] = True
            out["error"] = str(e)
            log.warning("market_structure fallback to stale: %s", e)
            return out
        raise


def _market_structure_fresh(cache_key: str) -> dict[str, Any]:

    fields = "f12,f14,f2,f3,f62,f66,f72,f78,f84,f184"
    rows = _fetch_ulist("1.000001,0.399001", fields)
    by = {str(x.get("f12")): x for x in rows}

    def _side(code: str, name: str) -> dict[str, Any]:
        r = by.get(code) or {}
        pack = _bucket_pack(
            _num(r.get("f62")),
            _num(r.get("f66")),
            _num(r.get("f72")),
            _num(r.get("f78")),
            _num(r.get("f84")),
            _num(r.get("f184")),
        )
        pack.update(
            {
                "code": code,
                "name": r.get("f14") or name,
                "price": _num(r.get("f2")),
                "change_pct": _num(r.get("f3")),
            }
        )
        return pack

    sh = _side("000001", "上证指数")
    sz = _side("399001", "深证成指")

    def _sum(a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is None and b is None:
            return None
        return (a or 0.0) + (b or 0.0)

    hs = _bucket_pack(
        _sum(sh.get("main_net"), sz.get("main_net")),
        _sum(sh.get("super_net"), sz.get("super_net")),
        _sum(sh.get("large_net"), sz.get("large_net")),
        _sum(sh.get("mid_net"), sz.get("mid_net")),
        _sum(sh.get("retail_net"), sz.get("retail_net")),
        None,
    )
    # 主力净比：按 |主力| 加权近似
    w_sh = abs(sh.get("main_net") or 0)
    w_sz = abs(sz.get("main_net") or 0)
    if (w_sh + w_sz) > 0 and sh.get("main_ratio_pct") is not None and sz.get("main_ratio_pct") is not None:
        hs["main_ratio_pct"] = round(
            ((sh["main_ratio_pct"] or 0) * w_sh + (sz["main_ratio_pct"] or 0) * w_sz) / (w_sh + w_sz),
            2,
        )

    nb = fetch_northbound_rt()

    result = {
        "ok": True,
        "source": "eastmoney",
        "asof": _now_iso(),
        "cached": False,
        "scope": "沪深两市（上证指数+深证成指分档加总）",
        "hs": hs,
        "sh": sh,
        "sz": sz,
        "northbound": nb,
        "labels": {
            "force": "主力(超大+大单)",
            "retail": "散户(小单)",
            "scissors": "主力-散户剪刀差",
            "size_main_share": "大单体量占比(|超大+大|/分档绝对值)",
            "main_ratio": "主力净比(东财f184)",
            "northbound": "北向净买(通道)",
        },
        "disclaimer": "结构资金为订单规模/通道代理，不能识别国家队/公募/私募真实账户。",
    }
    _cache_set(cache_key, result, ttl=12.0)
    return result


def _label_seat(name: str) -> dict[str, str]:
    n = name or ""
    if "机构专用" in n:
        return {"tag": "institution", "label": "机构"}
    if any(k in n for k in ("沪股通", "深股通", "港股通", "股通专用")):
        return {"tag": "northbound", "label": "北向"}
    if any(k in n for k in HOT_MONEY_KEYWORDS):
        return {"tag": "hot_money", "label": "游资"}
    # 弱启发：部分量化常用营业部关键字（仅 suspect）
    if any(k in n for k in ("量化", "衍复", "九坤", "幻方", "明汯", "诚奇")):
        return {"tag": "quant_suspect", "label": "疑似量化"}
    return {"tag": "branch", "label": "营业部"}


def _dc_get(report: str, **kwargs) -> list[dict[str, Any]]:
    sess = _session()
    params = {
        "reportName": report,
        "columns": "ALL",
        "pageNumber": 1,
        "pageSize": kwargs.get("pageSize", 20),
        "source": "WEB",
        "client": "WEB",
        "_": int(time.time() * 1000),
    }
    for k in ("filter", "sortColumns", "sortTypes"):
        if kwargs.get(k):
            params[k] = kwargs[k]
    try:
        r = sess.get(EM_DC, params=params, headers=EM_HEADERS, timeout=12)
        r.raise_for_status()
        j = r.json()
        if not j.get("success"):
            return []
        return ((j.get("result") or {}).get("data")) or []
    except Exception as e:
        log.warning("dc %s failed: %s", report, e)
        return []


def fetch_stock_buckets(code: str) -> dict[str, Any]:
    secid = _secid(code)
    if not secid:
        return {"ok": False, "error": "invalid code"}
    rows = _fetch_ulist(
        secid,
        "f12,f14,f2,f3,f6,f8,f62,f66,f69,f72,f75,f78,f81,f84,f87,f184",
    )
    if not rows:
        return {"ok": False, "error": "no quote"}
    r = rows[0]
    pack = _bucket_pack(
        _num(r.get("f62")),
        _num(r.get("f66")),
        _num(r.get("f72")),
        _num(r.get("f78")),
        _num(r.get("f84")),
        _num(r.get("f184")),
    )
    pack.update(
        {
            "ok": True,
            "code": str(r.get("f12") or code),
            "name": r.get("f14"),
            "price": _num(r.get("f2")),
            "change_pct": _num(r.get("f3")),
            "amount": _num(r.get("f6")),
            "amount_yi": _yi(_num(r.get("f6"))),
            "turnover": _num(r.get("f8")),
            "super_ratio_pct": _num(r.get("f69")),
            "large_ratio_pct": _num(r.get("f75")),
            "mid_ratio_pct": _num(r.get("f81")),
            "retail_ratio_pct": _num(r.get("f87")),
        }
    )
    return pack


def fetch_stock_margin(code: str) -> dict[str, Any]:
    c = re.sub(r"\D", "", str(code or ""))
    rows = _dc_get(
        "RPTA_WEB_RZRQ_GGMX",
        filter=f'(SCODE="{c}")',
        sortColumns="DATE",
        sortTypes="-1",
        pageSize=5,
    )
    if not rows:
        return {"ok": False, "identity": "channel", "name": "两融", "items": []}
    items = []
    for x in rows[:5]:
        items.append(
            {
                "date": (x.get("DATE") or "")[:10],
                "rzye": _num(x.get("RZYE")),
                "rzye_yi": _yi(_num(x.get("RZYE"))),
                "rzjme": _num(x.get("RZJME")),  # 融资净买入
                "rzjme_yi": _yi(_num(x.get("RZJME"))),
                "rqye": _num(x.get("RQYE")),
                "rqye_yi": _yi(_num(x.get("RQYE"))),
                "rzrqye_yi": _yi(_num(x.get("RZRQYE"))),
            }
        )
    latest = items[0]
    prev = items[1] if len(items) > 1 else None
    delta_rzye = None
    if prev and latest.get("rzye") is not None and prev.get("rzye") is not None:
        delta_rzye = latest["rzye"] - prev["rzye"]
    return {
        "ok": True,
        "identity": "channel",
        "name": "两融",
        "latest": latest,
        "rzye_change": delta_rzye,
        "rzye_change_yi": _yi(delta_rzye),
        "items": items,
        "note": "日级融资融券，非盘中实时",
    }


def fetch_stock_lhb(code: str, days: int = 10) -> dict[str, Any]:
    c = re.sub(r"\D", "", str(code or ""))
    # 取最近买卖席位
    buy_rows = _dc_get(
        "RPT_BILLBOARD_DAILYDETAILSBUY",
        filter=f'(SECURITY_CODE="{c}")',
        sortColumns="TRADE_DATE",
        sortTypes="-1",
        pageSize=50,
    )
    sell_rows = _dc_get(
        "RPT_BILLBOARD_DAILYDETAILSSELL",
        filter=f'(SECURITY_CODE="{c}")',
        sortColumns="TRADE_DATE",
        sortTypes="-1",
        pageSize=50,
    )
    if not buy_rows and not sell_rows:
        return {
            "ok": True,
            "on_list": False,
            "identity": "seat_event",
            "name": "龙虎榜",
            "days": [],
            "note": "近端无龙虎榜记录",
        }

    # group by trade date
    by_day: dict[str, dict[str, Any]] = {}

    def _add(row: dict, side: str) -> None:
        day = (row.get("TRADE_DATE") or "")[:10]
        if not day:
            return
        slot = by_day.setdefault(
            day,
            {
                "date": day,
                "reason": row.get("EXPLANATION") or row.get("EXPLAIN"),
                "change_pct": _num(row.get("CHANGE_RATE")),
                "close": _num(row.get("CLOSE_PRICE")),
                "seats": [],
            },
        )
        if not slot.get("reason"):
            slot["reason"] = row.get("EXPLANATION")
        name = row.get("OPERATEDEPT_NAME") or ""
        lab = _label_seat(name)
        seat = {
            "name": name,
            "side": side,
            "buy": _num(row.get("BUY")),
            "buy_yi": _yi(_num(row.get("BUY"))),
            "sell": _num(row.get("SELL")),
            "sell_yi": _yi(_num(row.get("SELL"))),
            "net": _num(row.get("NET")),
            "net_yi": _yi(_num(row.get("NET"))),
            "tag": lab["tag"],
            "label": lab["label"],
        }
        # dedupe by name+side+net
        key = (seat["name"], seat["side"], seat["net"])
        exist = {(s["name"], s["side"], s["net"]) for s in slot["seats"]}
        if key not in exist:
            slot["seats"].append(seat)

    for r in buy_rows:
        _add(r, "buy")
    for r in sell_rows:
        _add(r, "sell")

    days_list = sorted(by_day.values(), key=lambda x: x["date"], reverse=True)[: max(1, days)]
    # summarize latest day tags
    latest = days_list[0] if days_list else None
    tag_summary = {"institution": 0, "hot_money": 0, "northbound": 0, "quant_suspect": 0, "branch": 0}
    if latest:
        for s in latest["seats"]:
            tag_summary[s.get("tag") or "branch"] = tag_summary.get(s.get("tag") or "branch", 0) + 1

    return {
        "ok": True,
        "on_list": True,
        "identity": "seat_event",
        "name": "龙虎榜",
        "latest_date": latest["date"] if latest else None,
        "latest": latest,
        "days": days_list,
        "tag_summary": tag_summary,
        "note": "仅覆盖上榜交易日；席位标签为启发式",
    }


def fetch_stock_north_flag(code: str) -> dict[str, Any]:
    """用最近龙虎榜是否出现股通席位 / 或持股接口弱标记。"""
    # 轻量：从 LHB 最近记录判断是否活跃北向席位；持股明细过旧则仅作补充
    c = re.sub(r"\D", "", str(code or ""))
    hold_rows = _dc_get(
        "RPT_MUTUAL_HOLD_DET",
        filter=f'(SECURITY_CODE="{c}")',
        sortColumns="HOLD_DATE",
        sortTypes="-1",
        pageSize=3,
    )
    latest_hold = None
    if hold_rows:
        x = hold_rows[0]
        latest_hold = {
            "date": (x.get("HOLD_DATE") or "")[:10],
            "org": x.get("ORG_NAME"),
            "hold_num": _num(x.get("HOLD_NUM")),
            "hold_ratio": _num(x.get("HOLD_SHARES_RATIO") or x.get("FREE_SHARES_RATIO")),
        }
    return {
        "ok": True,
        "identity": "channel",
        "name": "北向相关",
        "hold_sample": latest_hold,
        "note": "持股为历史披露样本；当日净买见全市场北向或席位",
    }


def stock_fund_profile(code: str, refresh: bool = False) -> dict[str, Any]:
    """个股资金成分卡片数据。"""
    c = re.sub(r"\D", "", str(code or ""))
    if not c:
        return {"ok": False, "error": "股票代码无效"}
    cache_key = f"stock_profile:{c}"
    if not refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            out = dict(cached)
            out["cached"] = True
            return out

    buckets = fetch_stock_buckets(c)
    margin = fetch_stock_margin(c)
    lhb = fetch_stock_lhb(c, days=5)
    north = fetch_stock_north_flag(c)
    # 若龙虎榜最新席位含北向，补充标记
    nb_on_list = False
    if lhb.get("latest"):
        for s in lhb["latest"].get("seats") or []:
            if s.get("tag") == "northbound":
                nb_on_list = True
                break
    north["on_recent_lhb"] = nb_on_list

    result = {
        "ok": True,
        "code": c,
        "name": buckets.get("name"),
        "asof": _now_iso(),
        "cached": False,
        "buckets": buckets,
        "margin": margin,
        "lhb": lhb,
        "northbound": north,
        "disclaimer": "分档=规模代理；两融=日级；龙虎榜=事件样本。非账户真实身份。",
    }
    _cache_set(cache_key, result, ttl=30.0)
    return result
