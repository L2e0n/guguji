#!/usr/bin/env python3
"""CLI entry for QDII AI/semiconductor radar scan.

Examples:
  python scan_qdii_radar.py full
  python scan_qdii_radar.py universe --refresh-holdings
  python scan_qdii_radar.py quota
  python scan_qdii_radar.py status
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

import qdii_radar


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="QDII AI radar scanner")
    parser.add_argument(
        "command",
        choices=["full", "universe", "quota", "status", "pool", "events"],
        help="scan command",
    )
    parser.add_argument("--refresh-holdings", action="store_true", help="bypass holdings cache")
    parser.add_argument("--use-quota-cache", action="store_true", help="allow cached quota snapshots")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--pretty", action="store_true", default=True)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if args.command == "full":
        result = qdii_radar.run_full_scan(
            refresh_holdings=args.refresh_holdings,
            use_quota_cache=args.use_quota_cache,
        )
    elif args.command == "universe":
        result = qdii_radar.run_universe_and_score(refresh_holdings=args.refresh_holdings)
    elif args.command == "quota":
        result = qdii_radar.run_quota_scan(use_cache=args.use_quota_cache)
    elif args.command == "status":
        result = qdii_radar.get_status()
    elif args.command == "pool":
        result = {"ok": True, "count": 0, "items": qdii_radar.get_pool(limit=args.limit)}
        result["count"] = len(result["items"])
    else:
        result = {
            "ok": True,
            "count": 0,
            "items": qdii_radar.get_events(days=args.days, limit=args.limit),
        }
        result["count"] = len(result["items"])

    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None))
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
