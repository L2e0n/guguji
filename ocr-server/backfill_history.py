#!/usr/bin/env python3
"""One-shot / ops helper: seed daily snapshots.

Usage:
  python backfill_history.py              # save live dual/market/dark if available
  python backfill_history.py --limit-day 2026-07-31
"""
from __future__ import annotations

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import history_store
import sector_flow

log = logging.getLogger("backfill-history")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def save_live() -> None:
    try:
        dual = sector_flow.dual_rank(board_type="industry", period="1", top=20, primary_only=True, refresh=True)
        if dual.get("ok"):
            history_store.save_sector_dual(dual)
            log.info("saved sector_dual day=%s", dual.get("asof"))
    except Exception as e:
        log.warning("live dual: %s", e)
    try:
        mkt = sector_flow.market_overview(refresh=True)
        if mkt.get("ok"):
            history_store.save_market(mkt)
            struct = mkt.get("structure")
            if isinstance(struct, dict) and struct.get("ok"):
                history_store.save_structure(struct)
            log.info("saved market day=%s strength=%s", (mkt.get("session") or {}).get("day"), (mkt.get("strength") or {}).get("strong"))
    except Exception as e:
        log.warning("live market: %s", e)
    try:
        import dark_flow
        dark = dark_flow.rank(sort="dark_in", limit=45, refresh=True)
        if dark.get("ok"):
            history_store.save_dark_rank(dark)
            log.info("saved dark_rank day=%s", dark.get("asof_day") or dark.get("asof"))
    except Exception as e:
        log.warning("live dark: %s", e)


def backfill_limit_day(day_iso: str) -> None:
    """Backfill limit/daban snapshot for a past trading day from EM topic pools."""
    day_iso = day_iso[:10]
    day_key = day_iso.replace("-", "")
    log.info("backfill limit pools for %s", day_iso)
    with ThreadPoolExecutor(max_workers=6) as pool:
        f_zt = pool.submit(sector_flow._fetch_topic_pool, sector_flow.EM_ZT_HOSTS, day_key, 500, "fund:desc")
        f_dt = pool.submit(sector_flow._fetch_topic_pool, sector_flow.EM_DT_HOSTS, day_key, 200, "fund:desc")
        f_zb = pool.submit(sector_flow._fetch_topic_pool, sector_flow.EM_ZB_HOSTS, day_key, 200, "amount:desc")
        f_qs = pool.submit(sector_flow._fetch_topic_pool, sector_flow.EM_QS_HOSTS, day_key, 200, "zdp:desc")
        f_cx = pool.submit(sector_flow._fetch_topic_pool, sector_flow.EM_CX_HOSTS, day_key, 200, "ods:asc")
        f_yzt = pool.submit(sector_flow._fetch_topic_pool, sector_flow.EM_YZT_HOSTS, day_key, 200, "zs:desc")
        zt_pool = f_zt.result() or []
        dt_pool = f_dt.result() or []
        zb_pool = f_zb.result() or []
        qs_pool = f_qs.result() or []
        cx_pool = f_cx.result() or []
        yzt_pool = f_yzt.result() or []
    if not any([zt_pool, dt_pool, zb_pool]):
        log.warning("no pools for %s", day_iso)
        return
    zt = sector_flow._pool_stats(zt_pool)
    dt = sector_flow._pool_stats(dt_pool)
    zb = sector_flow._pool_stats(zb_pool)
    prev_day_key = sector_flow._prev_trade_day_key(day_key)
    prev_zt_n = prev_dt_n = prev_zb_n = None
    if prev_day_key:
        with ThreadPoolExecutor(max_workers=3) as ppool:
            p_zt = ppool.submit(sector_flow._fetch_topic_pool, sector_flow.EM_ZT_HOSTS, prev_day_key, 500, "fund:desc")
            p_dt = ppool.submit(sector_flow._fetch_topic_pool, sector_flow.EM_DT_HOSTS, prev_day_key, 200, "fund:desc")
            p_zb = ppool.submit(sector_flow._fetch_topic_pool, sector_flow.EM_ZB_HOSTS, prev_day_key, 200, "amount:desc")
            try:
                prev_zt_n = sector_flow._pool_stats(p_zt.result() or []).get("count")
            except Exception:
                pass
            try:
                prev_dt_n = sector_flow._pool_stats(p_dt.result() or []).get("count")
            except Exception:
                pass
            try:
                prev_zb_n = sector_flow._pool_stats(p_zb.result() or []).get("count")
            except Exception:
                pass
    if isinstance(zt, dict):
        zt = dict(zt); zt["prev_count"] = prev_zt_n; zt["prev_day"] = prev_day_key
    if isinstance(dt, dict):
        dt = dict(dt); dt["prev_count"] = prev_dt_n; dt["prev_day"] = prev_day_key
    if isinstance(zb, dict):
        zb = dict(zb); zb["prev_count"] = prev_zb_n; zb["prev_day"] = prev_day_key

    daban = sector_flow._build_daban_from_em(
        zt_pool, zb_pool, qs_pool,
        dt_pool=dt_pool, cx_pool=cx_pool, yzt_pool=yzt_pool,
        clist_up=[], clist_speed=[], clist_down=[],
        limit=15, day=day_iso,
    )
    strength = sector_flow._compute_sentiment_strength(zt, zb, {}, {}, day=day_iso)
    payload = {
        "ok": True,
        "cached": False,
        "stale": False,
        "asof": f"{day_iso}T15:00:00+08:00",
        "source": "eastmoney+backfill",
        "session": {"day": day_iso, "progress_pct": 100},
        "strength": strength,
        "volume": {"unit": "yi", "note": "历史回填仅含涨跌停/打板，量能以当时实盘快照为准"},
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
                "limit_up_prev": prev_zt_n,
                "limit_down_prev": prev_dt_n,
                "broken_prev": prev_zb_n,
                "prev_day": prev_day_key,
            },
            "note": "历史回填：东财主题池",
        },
        "daban": daban if isinstance(daban, dict) else {"ok": False, "tabs": {}},
        "structure": {"ok": False, "error": "history_backfill_no_etf"},
        "backfill": True,
    }
    ok = history_store.save_market(payload, day=day_iso)
    log.info("save market %s -> %s zt=%s dt=%s zb=%s", day_iso, ok, zt.get("count"), dt.get("count"), zb.get("count"))


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-day", action="append", default=[], help="YYYY-MM-DD backfill limit/daban")
    ap.add_argument("--live", action="store_true", help="also pull and save live snapshots")
    args = ap.parse_args(argv)
    if args.live or not args.limit_day:
        save_live()
    for d in args.limit_day:
        backfill_limit_day(d)
    print("days:", history_store.list_days("market", limit=10))
    print("dual:", history_store.list_days("sector_dual", limit=10))
    print("dark:", history_store.list_days("dark_rank", limit=10))


if __name__ == "__main__":
    main()
