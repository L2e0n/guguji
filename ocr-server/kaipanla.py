# -*- coding: utf-8 -*-
"""开盘啦 (longhuvip) 公开盯盘接口适配层。

用途：
- 大盘量能 MarketCapacity（主源）
- 综合强度分 ChangeStatistics.strong
- 打板池 DaBanList / HisDaBanList（竞价/即将涨停/涨停/炸板）
- 风向标 ZhiShuStockList_W8 PlateID=801225

说明：非官方 API，无 token 但可能变更；限频 + 缓存由调用方负责。
"""
from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urlencode

import requests

log = logging.getLogger("guguji-kaipanla")

TZ_SH = timezone(timedelta(hours=8))
YI = 1e8
WAN = 1e4

HQ_HOSTS = (
    "https://apphq.longhuvip.com/w1/api/index.php",
    "https://apphwshhq.longhuvip.com/w1/api/index.php",
)
HIS_HOST = "https://apphis.longhuvip.com/w1/api/index.php"

DEFAULT_UA = "lhb/5.23.1 (com.kaipanla.www; build:1; iOS 18.2.0) Alamofire/4.9.1"
DEFAULT_VERSION = "5.23.0.1"
DEFAULT_APIV = "w44"

# 打板 Tab 配置：key -> 展示名 / 实时参数 / 历史参数
DABAN_TABS: dict[str, dict[str, Any]] = {
    "auction": {
        "label": "竞价",
        "pid_type": 8,
        "type": 18,
        "st": 20,
    },
    "near_limit": {
        "label": "即将涨停",
        "pid_type": 0,
        "type": 4,
        "st": 20,
    },
    "limit_up": {
        "label": "涨停",
        "pid_type": 1,
        "type": 6,
        "st": 30,
    },
    "broken": {
        "label": "炸板",
        "pid_type": 2,
        "type": 4,
        "st": 20,
    },
}

WIND_PLATE_ID = "801225"  # 开盘啦「风向标」板块


def _now() -> datetime:
    return datetime.now(TZ_SH)


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    s.headers.update(
        {
            "User-Agent": DEFAULT_UA,
            "Accept": "*/*",
            "Accept-Language": "zh-Hans-CN;q=1.0",
        }
    )
    return s


def _get_json(
    base: str,
    params: dict[str, Any],
    *,
    timeout: float = 12.0,
    session: Optional[requests.Session] = None,
) -> dict[str, Any]:
    sess = session or _session()
    url = f"{base}?{urlencode(params)}"
    r = sess.get(url, timeout=timeout)
    r.raise_for_status()
    body = r.content
    if not body:
        raise RuntimeError(f"empty body from {base}")
    data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"non-object JSON from {base}")
    return data


def _is_placeholder_list(lst: Any) -> bool:
    if not isinstance(lst, list) or not lst:
        return False
    row0 = lst[0]
    if not isinstance(row0, (list, tuple)) or not row0:
        return False
    s0 = str(row0[0] or "")
    return "kaipanla" in s0.lower()


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


def _to_int(v: Any) -> Optional[int]:
    f = _to_float(v)
    if f is None:
        return None
    try:
        return int(f)
    except (TypeError, ValueError):
        return None


def wan_to_yi(v: Any) -> Optional[float]:
    f = _to_float(v)
    if f is None:
        return None
    return f / WAN


def wan_to_yuan(v: Any) -> Optional[float]:
    f = _to_float(v)
    if f is None:
        return None
    return f * WAN


# ── MarketCapacity ──────────────────────────────────────────────


def fetch_market_capacity(
    market_type: int = 0,
    *,
    timeout: float = 12.0,
) -> dict[str, Any]:
    """拉取量能。Type=0 全市场；1/2 子市场。"""
    params = {
        "c": "HomeDingPan",
        "a": "MarketCapacity",
        "PhoneOSNew": "2",
        "VerSion": DEFAULT_VERSION,
        "apiv": DEFAULT_APIV,
        "Type": str(market_type),
    }
    sess = _session()
    last_err: Exception | None = None
    raw: dict[str, Any] | None = None
    used = ""
    for base in HQ_HOSTS:
        try:
            data = _get_json(base, params, timeout=timeout, session=sess)
            if "info" not in data:
                last_err = RuntimeError("missing info")
                continue
            raw = data
            used = base
            break
        except Exception as e:
            last_err = e
            continue
    if raw is None:
        raise RuntimeError(f"MarketCapacity failed: {last_err}")

    info = raw.get("info") or {}
    if not isinstance(info, dict):
        raise RuntimeError("MarketCapacity info not object")

    points: list[dict[str, Any]] = []
    pace_points: list[dict[str, Any]] = []
    prev_cum: float | None = None
    trends = info.get("trends") or []
    if not isinstance(trends, list):
        trends = []

    for row in trends:
        if not row or len(row) < 2:
            continue
        t = str(row[0])
        cum_yi = wan_to_yi(row[1])
        if cum_yi is None:
            continue
        amount_yi = cum_yi if prev_cum is None else round(cum_yi - prev_cum, 6)
        if amount_yi < 0:
            amount_yi = 0.0
        pt: dict[str, Any] = {
            "t": t,
            "amount_yi": round(amount_yi, 6),
            "cum_yi": round(cum_yi, 6),
            "source": "kaipanla",
        }
        yoy = wan_to_yi(row[2]) if len(row) > 2 else None
        if yoy is not None:
            pt["yoy_cum_yi"] = round(yoy, 6)
        base_c = wan_to_yi(row[3]) if len(row) > 3 else None
        if base_c is not None:
            pt["base_cum_yi"] = round(base_c, 6)
        proj_pct = _to_float(row[4]) if len(row) > 4 else None
        if proj_pct is not None:
            pt["proj_pct"] = proj_pct
        if len(row) > 5 and row[5] not in (None, ""):
            pt["proj_str"] = str(row[5])
        points.append(pt)
        prev_cum = cum_yi

        # pace = 今日预测全天 / 昨日全天；proj_pct 为相对昨日变化%
        if proj_pct is not None:
            pace = 1.0 + (proj_pct / 100.0)
            # 解析 proj_str 中「xxxx亿」
            implied_yi = None
            ps = pt.get("proj_str") or ""
            if "亿" in ps:
                try:
                    head = ps.split("亿", 1)[0]
                    # 可能带括号前缀数字
                    num = ""
                    for ch in head[::-1]:
                        if ch.isdigit() or ch == ".":
                            num = ch + num
                        elif num:
                            break
                    if num:
                        implied_yi = float(num)
                except Exception:
                    implied_yi = None
            yesterday_yi = wan_to_yi(info.get("s_zrtj") or info.get("q_zrtj"))
            prev_yi = yesterday_yi
            if implied_yi is not None and prev_yi and prev_yi > 0:
                pace = implied_yi / prev_yi
            pace_points.append(
                {
                    "t": t,
                    "pace": round(pace, 6),
                    "proj_pct": proj_pct,
                    "implied_yi": round(implied_yi, 4) if implied_yi is not None else None,
                    "prev_yi": round(prev_yi, 4) if prev_yi is not None else None,
                    "delta_yi": round((implied_yi - prev_yi), 4)
                    if (implied_yi is not None and prev_yi is not None)
                    else None,
                    "cum_yi": round(cum_yi, 4),
                }
            )

    last_yi = wan_to_yi(info.get("last"))
    yesterday_yi = wan_to_yi(info.get("s_zrtj") or info.get("q_zrtj"))
    # ycln 文档称单位为「元」≈ last×10000；也兼容万元误标
    ycln = _to_float(info.get("ycln"))
    predict_yi = None
    if ycln is not None and ycln > 0:
        # 若与 last(万元) 同量级则按万元，否则按元
        if last_yi is not None and abs(ycln / WAN - last_yi) < abs(ycln / YI - last_yi):
            predict_yi = ycln / WAN
        else:
            predict_yi = ycln / YI
    # 优先用 trends 最后有效 proj
    if points:
        last_pt = points[-1]
        if last_pt.get("proj_str") and "亿" in str(last_pt["proj_str"]):
            try:
                head = str(last_pt["proj_str"]).split("亿", 1)[0]
                num = ""
                for ch in head[::-1]:
                    if ch.isdigit() or ch == ".":
                        num = ch + num
                    elif num:
                        break
                if num:
                    predict_yi = float(num)
            except Exception:
                pass
        elif last_pt.get("proj_pct") is not None and yesterday_yi:
            predict_yi = yesterday_yi * (1.0 + float(last_pt["proj_pct"]) / 100.0)

    csbl = _to_float(info.get("csbl"))
    date = info.get("date")

    vs_prev: dict[str, Any] = {
        "prev_day": None,  # MarketCapacity 不直接给昨日期，仅金额
        "prev_hs_amount": wan_to_yuan(info.get("s_zrtj") or info.get("q_zrtj")),
        "prev_hs_amount_yi": yesterday_yi,
        "basis": "kaipanla_proj" if predict_yi is not None else None,
        "today_ref": (predict_yi * YI) if predict_yi is not None else wan_to_yuan(info.get("last")),
        "today_ref_yi": predict_yi if predict_yi is not None else last_yi,
        "delta": None,
        "delta_yi": None,
        "direction": None,
        "label": None,
    }
    if yesterday_yi is not None and yesterday_yi > 0 and vs_prev["today_ref_yi"] is not None:
        delta_yi = float(vs_prev["today_ref_yi"]) - float(yesterday_yi)
        vs_prev["delta_yi"] = round(delta_yi, 4)
        vs_prev["delta"] = delta_yi * YI
        if abs(delta_yi) < 1:
            vs_prev["direction"], vs_prev["label"] = "flat", "持平"
        elif delta_yi > 0:
            vs_prev["direction"], vs_prev["label"] = "expand", "放量"
        else:
            vs_prev["direction"], vs_prev["label"] = "shrink", "缩量"

    latest_pace = pace_points[-1] if pace_points else None

    return {
        "ok": True,
        "source": "kaipanla",
        "request_host": used,
        "date": date,
        "server_time": info.get("time"),
        "last_wan": _to_float(info.get("last")),
        "last_yi": round(last_yi, 4) if last_yi is not None else None,
        "amount": wan_to_yuan(info.get("last")),
        "predict_amount": (predict_yi * YI) if predict_yi is not None else None,
        "predict_amount_yi": round(predict_yi, 4) if predict_yi is not None else None,
        "predict_method": "kaipanla_market_capacity",
        "yesterday_yi": round(yesterday_yi, 4) if yesterday_yi is not None else None,
        "yclnstr": info.get("yclnstr"),
        "csbl": csbl,
        "points": points,
        "point_count": len(points),
        "vs_prev": vs_prev,
        "vs_prev_pace": {
            "source": "kaipanla",
            "points": pace_points,
            "latest": latest_pace,
            "unit": "ratio",  # 1.0 = 与昨持平
            "note": "pace=今日预测全天/昨日实际；前端×100 为百分比",
        },
        "raw_info_keys": list(info.keys())[:20],
    }


# ── ChangeStatistics 综合强度 ───────────────────────────────────


def fetch_change_statistics(*, timeout: float = 10.0) -> dict[str, Any]:
    params = {
        "c": "HomeDingPan",
        "a": "ChangeStatistics",
        "PhoneOSNew": "2",
        "VerSion": DEFAULT_VERSION,
        "apiv": DEFAULT_APIV,
        "st": "1000",
    }
    sess = _session()
    last_err: Exception | None = None
    raw = None
    for base in HQ_HOSTS:
        try:
            raw = _get_json(base, params, timeout=timeout, session=sess)
            break
        except Exception as e:
            last_err = e
            continue
    if raw is None:
        raise RuntimeError(f"ChangeStatistics failed: {last_err}")

    info = raw.get("info")
    row: dict[str, Any] = {}
    if isinstance(info, list) and info:
        if isinstance(info[0], dict):
            row = info[0]
    elif isinstance(info, dict):
        row = info

    strong = _to_float(row.get("strong"))
    return {
        "ok": True,
        "source": "kaipanla",
        "day": row.get("Day") or row.get("day"),
        "strong": strong,  # 综合强度分 0-100
        "ztjs": _to_int(row.get("ztjs")),  # 涨停家数
        "df_num": _to_int(row.get("df_num")),  # 跌停相关
        "lbgd": _to_int(row.get("lbgd")),  # 连板高度
        "tip": raw.get("tip"),
        "label": "综合强度",
    }


# ── DaBanList 打板 ──────────────────────────────────────────────


def _normalize_daban_row(row: list[Any], *, tab: str) -> Optional[dict[str, Any]]:
    if not row or len(row) < 2:
        return None
    code = str(row[0] or "").strip()
    name = str(row[1] or "").strip()
    if not code or "kaipanla" in code.lower():
        return None
    change_pct = _to_float(row[4]) if len(row) > 4 else None
    board_text = str(row[9]).strip() if len(row) > 9 and row[9] not in (None, "") else None
    limit_times = _to_int(row[10]) if len(row) > 10 else None
    theme = str(row[11]).strip() if len(row) > 11 and row[11] not in (None, "") else None
    industry = str(row[16]).strip() if len(row) > 16 and row[16] not in (None, "", "无") else None
    # 竞价额外字段
    auction_amount = _to_float(row[18]) if len(row) > 18 else None
    auction_ratio = _to_float(row[21]) if len(row) > 21 else None

    # 涨停池涨跌幅常为 0（已封板），前端改为不显示误导性 0%
    if tab == "limit_up" and change_pct is not None and abs(change_pct) < 1e-9:
        change_pct = None

    item: dict[str, Any] = {
        "code": code,
        "name": name,
        "change_pct": change_pct,
        "board_text": board_text,
        "limit_times": limit_times,
        "theme": theme,
        "industry": industry or (theme.split("、")[0] if theme else None),
        "tab": tab,
    }
    if tab == "auction":
        if auction_amount is not None:
            # 竞价额：观测上量级接近「元」
            item["auction_amount"] = auction_amount
            item["auction_amount_yi"] = round(auction_amount / YI, 4) if auction_amount > 1e5 else round(auction_amount / WAN, 4)
        if auction_ratio is not None:
            item["auction_ratio"] = auction_ratio
    return item


def _fetch_daban_once(
    *,
    pid_type: int,
    type_: int,
    st: int,
    day: Optional[str],
    use_history: bool,
    timeout: float,
    session: requests.Session,
) -> tuple[list[list[Any]], dict[str, Any]]:
    if use_history:
        if not day:
            raise RuntimeError("history daban requires day")
        params = {
            "c": "HisHomeDingPan",
            "a": "HisDaBanList",
            "PidType": str(pid_type),
            "Type": str(type_),
            "Day": day,
            "Order": "1",
            "st": str(st),
            "Index": "0",
            "Is_st": "1",
            "apiv": "w26",
            "PhoneOSNew": "1",
            "VerSion": DEFAULT_VERSION,
        }
        data = _get_json(HIS_HOST, params, timeout=timeout, session=session)
        meta = {"host": "apphis", "realtime": False, "day": data.get("day") or day}
    else:
        params = {
            "c": "HomeDingPan",
            "a": "DaBanList",
            "PidType": str(pid_type),
            "Type": str(type_),
            "Order": "1",
            "st": str(st),
            "Index": "0",
            "apiv": "w26",
            "PhoneOSNew": "1",
            "VerSion": DEFAULT_VERSION,
        }
        last_err: Exception | None = None
        data = None
        host_used = ""
        for base in HQ_HOSTS:
            try:
                data = _get_json(base, params, timeout=timeout, session=session)
                host_used = base
                break
            except Exception as e:
                last_err = e
                continue
        if data is None:
            raise RuntimeError(f"DaBanList failed: {last_err}")
        meta = {"host": host_used, "realtime": True, "day": data.get("day") or data.get("date")}

    lst = data.get("list") or []
    if not isinstance(lst, list):
        lst = []
    return lst, meta


def fetch_daban_tab(
    tab_key: str,
    *,
    day: Optional[str] = None,
    limit: Optional[int] = None,
    timeout: float = 12.0,
) -> dict[str, Any]:
    conf = DABAN_TABS.get(tab_key)
    if not conf:
        raise ValueError(f"unknown daban tab: {tab_key}")
    st = int(limit or conf["st"])
    pid_type = int(conf["pid_type"])
    type_ = int(conf["type"])
    sess = _session()

    # 先实时，占位/空则历史
    items: list[dict[str, Any]] = []
    meta: dict[str, Any] = {}
    try:
        lst, meta = _fetch_daban_once(
            pid_type=pid_type,
            type_=type_,
            st=st,
            day=None,
            use_history=False,
            timeout=timeout,
            session=sess,
        )
        if _is_placeholder_list(lst) or not lst:
            raise RuntimeError("placeholder_or_empty")
        for row in lst:
            if isinstance(row, list):
                it = _normalize_daban_row(row, tab=tab_key)
                if it:
                    items.append(it)
    except Exception as e:
        log.info("daban realtime %s fallback hist: %s", tab_key, e)
        trade_day = day
        if not trade_day:
            # 回退最近若干自然日
            trade_day = _resolve_trade_day(sess=sess)
        last_err: Exception | None = None
        for i in range(0, 10):
            d0 = datetime.strptime(trade_day, "%Y-%m-%d") - timedelta(days=i) if i else datetime.strptime(trade_day, "%Y-%m-%d")
            dstr = d0.strftime("%Y-%m-%d")
            try:
                lst, meta = _fetch_daban_once(
                    pid_type=pid_type,
                    type_=type_,
                    st=st,
                    day=dstr,
                    use_history=True,
                    timeout=timeout,
                    session=sess,
                )
                if _is_placeholder_list(lst) or not lst:
                    continue
                items = []
                for row in lst:
                    if isinstance(row, list):
                        it = _normalize_daban_row(row, tab=tab_key)
                        if it:
                            items.append(it)
                if items:
                    meta["day"] = dstr
                    break
            except Exception as e2:
                last_err = e2
                continue
        if not items and last_err:
            log.warning("daban hist %s failed: %s", tab_key, last_err)

    return {
        "ok": bool(items),
        "key": tab_key,
        "label": conf["label"],
        "count": len(items),
        "items": items[:st],
        "day": meta.get("day"),
        "realtime": bool(meta.get("realtime")),
        "source": "kaipanla",
    }


def _resolve_trade_day(*, sess: Optional[requests.Session] = None) -> str:
    """用 ChangeStatistics / MarketCapacity 的交易日，否则回退今天。"""
    try:
        cs = fetch_change_statistics()
        if cs.get("day"):
            return str(cs["day"])
    except Exception:
        pass
    try:
        mc = fetch_market_capacity(0)
        if mc.get("date"):
            return str(mc["date"])
    except Exception:
        pass
    return _now().strftime("%Y-%m-%d")


def _normalize_wind_row(row: list[Any]) -> Optional[dict[str, Any]]:
    if not row or len(row) < 2:
        return None
    code = str(row[0] or "").strip()
    name = str(row[1] or "").strip()
    if not code:
        return None
    # company-src: zl=2, bk=4, zf=6, lb=23
    strength_tag = str(row[2]).strip() if len(row) > 2 and row[2] not in (None, "") else None
    board = str(row[4]).strip() if len(row) > 4 and row[4] not in (None, "") else None
    change_pct = _to_float(row[6]) if len(row) > 6 else None
    limit_times = _to_int(row[23]) if len(row) > 23 else None
    price = _to_float(row[5]) if len(row) > 5 else None
    return {
        "code": code,
        "name": name,
        "change_pct": change_pct,
        "theme": board,
        "industry": board,
        "board_text": strength_tag,
        "limit_times": limit_times,
        "price": price,
        "tab": "wind",
    }


def fetch_wind_vane(
    *,
    day: Optional[str] = None,
    limit: int = 20,
    timeout: float = 12.0,
) -> dict[str, Any]:
    """风向标 = 板块 801225 成分股列表。"""
    sess = _session()
    items: list[dict[str, Any]] = []
    meta_day = day
    realtime = True

    def _parse(data: dict[str, Any]) -> list[dict[str, Any]]:
        lst = data.get("list") or []
        out: list[dict[str, Any]] = []
        if not isinstance(lst, list):
            return out
        for row in lst:
            if isinstance(row, list):
                it = _normalize_wind_row(row)
                if it:
                    out.append(it)
        return out

    # live
    try:
        params = {
            "st": str(limit),
            "Order": "1",
            "PlateID": WIND_PLATE_ID,
            "a": "ZhiShuStockList_W8",
            "Type": "6",
            "c": "ZhiShuRanking",
            "apiv": "w26",
            "PhoneOSNew": "1",
            "VerSion": DEFAULT_VERSION,
        }
        last_err: Exception | None = None
        data = None
        for base in HQ_HOSTS:
            try:
                data = _get_json(base, params, timeout=timeout, session=sess)
                break
            except Exception as e:
                last_err = e
                continue
        if data is None:
            raise RuntimeError(str(last_err))
        items = _parse(data)
        # Day 字段可能是 list
        day_field = data.get("Day")
        if isinstance(day_field, list) and day_field:
            meta_day = str(day_field[0])
        elif isinstance(day_field, str):
            meta_day = day_field
        if not items:
            raise RuntimeError("empty wind list")
    except Exception as e:
        log.info("wind live fallback hist: %s", e)
        realtime = False
        trade_day = day or _resolve_trade_day(sess=sess)
        for i in range(0, 10):
            d0 = datetime.strptime(trade_day, "%Y-%m-%d") - timedelta(days=i)
            dstr = d0.strftime("%Y-%m-%d")
            params = {
                "st": str(limit),
                "Order": "1",
                "PlateID": WIND_PLATE_ID,
                "Token": "0",
                "a": "ZhiShuStockList_W8",
                "Type": "6",
                "c": "ZhiShuRanking",
                "Date": dstr,
                "apiv": "w26",
                "PhoneOSNew": "1",
                "VerSion": DEFAULT_VERSION,
            }
            try:
                data = _get_json(HIS_HOST, params, timeout=timeout, session=sess)
                items = _parse(data)
                if items:
                    meta_day = dstr
                    break
            except Exception:
                continue

    return {
        "ok": bool(items),
        "key": "wind",
        "label": "风向标",
        "count": len(items),
        "items": items[:limit],
        "day": meta_day,
        "realtime": realtime,
        "source": "kaipanla",
        "plate_id": WIND_PLATE_ID,
    }


def fetch_daban_bundle(
    *,
    day: Optional[str] = None,
    tabs: Optional[list[str]] = None,
    limit: int = 15,
    timeout: float = 12.0,
) -> dict[str, Any]:
    """并行拉取多个打板 Tab + 风向标。"""
    order = tabs or ["auction", "near_limit", "wind", "limit_up", "broken"]
    out_tabs: dict[str, Any] = {}
    trade_day = day

    def _one(key: str) -> tuple[str, dict[str, Any]]:
        if key == "wind":
            return key, fetch_wind_vane(day=trade_day, limit=limit, timeout=timeout)
        return key, fetch_daban_tab(key, day=trade_day, limit=limit, timeout=timeout)

    with ThreadPoolExecutor(max_workers=min(6, max(2, len(order)))) as pool:
        futs = {pool.submit(_one, k): k for k in order}
        for fut in as_completed(futs):
            k = futs[fut]
            try:
                key, payload = fut.result()
                out_tabs[key] = payload
                if not trade_day and payload.get("day"):
                    trade_day = payload.get("day")
            except Exception as e:
                log.warning("daban tab %s failed: %s", k, e)
                label = DABAN_TABS.get(k, {}).get("label") or ("风向标" if k == "wind" else k)
                out_tabs[k] = {
                    "ok": False,
                    "key": k,
                    "label": label,
                    "count": 0,
                    "items": [],
                    "error": str(e),
                    "source": "kaipanla",
                }

    # 保持顺序
    ordered = {k: out_tabs[k] for k in order if k in out_tabs}
    any_ok = any(v.get("ok") for v in ordered.values())
    return {
        "ok": any_ok,
        "source": "kaipanla",
        "day": trade_day,
        "tab_order": order,
        "tabs": ordered,
        "asof": _now().strftime("%Y-%m-%dT%H:%M:%S+08:00"),
    }


def fetch_volume_and_strength(*, timeout: float = 12.0) -> dict[str, Any]:
    """并行量能 + 综合强度。"""
    result: dict[str, Any] = {
        "capacity": None,
        "strength": None,
        "errors": {},
    }
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_cap = pool.submit(fetch_market_capacity, 0)
        f_str = pool.submit(fetch_change_statistics)
        try:
            result["capacity"] = f_cap.result(timeout=timeout + 2)
        except Exception as e:
            result["errors"]["capacity"] = str(e)
            log.warning("kaipanla capacity: %s", e)
        try:
            result["strength"] = f_str.result(timeout=timeout + 2)
        except Exception as e:
            result["errors"]["strength"] = str(e)
            log.warning("kaipanla strength: %s", e)
    result["ok"] = bool(result.get("capacity") and result["capacity"].get("ok"))
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    t0 = time.time()
    vs = fetch_volume_and_strength()
    print("volume/strength", json.dumps({
        "cap_ok": bool(vs.get("capacity")),
        "last_yi": (vs.get("capacity") or {}).get("last_yi"),
        "strong": (vs.get("strength") or {}).get("strong"),
        "errs": vs.get("errors"),
    }, ensure_ascii=False))
    db = fetch_daban_bundle(limit=5)
    print("daban", json.dumps({
        "ok": db.get("ok"),
        "day": db.get("day"),
        "counts": {k: v.get("count") for k, v in (db.get("tabs") or {}).items()},
    }, ensure_ascii=False))
    print("elapsed", round(time.time() - t0, 2))
