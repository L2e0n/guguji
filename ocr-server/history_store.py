#!/usr/bin/env python3
"""Persistent daily snapshots for sector fund-flow and dark-flow history.

Start day (inclusive): 2026-07-31. Live endpoints auto-upsert the current
session day; history APIs read JSON payloads for date-button browsing.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger("history-store")

TZ_SH = timezone(timedelta(hours=8))
HISTORY_START = "2026-07-31"

_DEFAULT_DB = Path(__file__).resolve().parent / "data" / "flow_history.db"
_DB_PATH = Path(os.environ.get("GUGUJI_HISTORY_DB", str(_DEFAULT_DB)))
_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.now(TZ_SH).strftime("%Y-%m-%dT%H:%M:%S+08:00")


def _today() -> str:
    return datetime.now(TZ_SH).strftime("%Y-%m-%d")


def _connect() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _lock:
        conn = _connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    day TEXT NOT NULL,
                    skey TEXT NOT NULL DEFAULT '',
                    payload TEXT NOT NULL,
                    asof TEXT,
                    updated_at REAL NOT NULL,
                    UNIQUE(kind, day, skey)
                );
                CREATE INDEX IF NOT EXISTS idx_snap_kind_day ON snapshots(kind, day);
                """
            )
            conn.commit()
        finally:
            conn.close()


def upsert(kind: str, day: str, payload: Any, *, skey: str = "", asof: Optional[str] = None) -> bool:
    """Upsert one snapshot. day=YYYY-MM-DD. Returns True if stored."""
    if not kind or not day:
        return False
    day = str(day)[:10]
    if day < HISTORY_START:
        return False
    if not isinstance(payload, dict):
        return False
    # shallow copy + strip volatile cache flags for cleaner history
    body = dict(payload)
    body.pop("cached", None)
    body["history_day"] = day
    body["history_kind"] = kind
    try:
        raw = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    except Exception as e:
        log.warning("history serialize %s/%s: %s", kind, day, e)
        return False
    asof = asof or body.get("asof") or _now_iso()
    now = time.time()
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                """
                INSERT INTO snapshots(kind, day, skey, payload, asof, updated_at)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(kind, day, skey) DO UPDATE SET
                  payload=excluded.payload,
                  asof=excluded.asof,
                  updated_at=excluded.updated_at
                """,
                (kind, day, skey or "", raw, asof, now),
            )
            conn.commit()
            return True
        except Exception as e:
            log.warning("history upsert %s/%s: %s", kind, day, e)
            return False
        finally:
            conn.close()


def get(kind: str, day: str, skey: str = "") -> Optional[dict[str, Any]]:
    day = str(day or "")[:10]
    if not day:
        return None
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT payload, asof, updated_at FROM snapshots WHERE kind=? AND day=? AND skey=?",
                (kind, day, skey or ""),
            ).fetchone()
            if not row:
                return None
            try:
                data = json.loads(row["payload"])
            except Exception:
                return None
            if isinstance(data, dict):
                data = dict(data)
                data["history"] = True
                data["history_day"] = day
                data["asof"] = data.get("asof") or row["asof"]
                data["ok"] = data.get("ok", True)
            return data
        finally:
            conn.close()


def list_days(kind: str, *, skey: Optional[str] = None, limit: int = 120) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit or 120), 366))
    with _lock:
        conn = _connect()
        try:
            if skey is None:
                rows = conn.execute(
                    """
                    SELECT day, MAX(asof) AS asof, MAX(updated_at) AS updated_at
                    FROM snapshots
                    WHERE kind=? AND day>=?
                    GROUP BY day
                    ORDER BY day DESC
                    LIMIT ?
                    """,
                    (kind, HISTORY_START, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT day, asof, updated_at
                    FROM snapshots
                    WHERE kind=? AND skey=? AND day>=?
                    ORDER BY day DESC
                    LIMIT ?
                    """,
                    (kind, skey or "", HISTORY_START, limit),
                ).fetchall()
            return [
                {
                    "day": r["day"],
                    "asof": r["asof"],
                    "updated_at": r["updated_at"],
                }
                for r in rows
            ]
        finally:
            conn.close()


def trading_day_guess(asof: Optional[str] = None) -> str:
    """Best-effort trading day label for snapshots (calendar day in SH)."""
    if asof:
        s = str(asof)
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return s[:10]
    return _today()


def save_sector_dual(payload: dict[str, Any], *, day: Optional[str] = None) -> bool:
    if not isinstance(payload, dict) or not payload.get("ok"):
        return False
    d = day or trading_day_guess(payload.get("asof"))
    board = str(payload.get("board_type") or "industry")
    period = str(payload.get("period") or "1")
    skey = f"{board}:{period}"
    return upsert("sector_dual", d, payload, skey=skey, asof=payload.get("asof"))


def save_dark_rank(payload: dict[str, Any], *, day: Optional[str] = None) -> bool:
    if not isinstance(payload, dict) or not payload.get("ok"):
        return False
    d = day or payload.get("asof_day") or trading_day_guess(payload.get("asof"))
    sort = str(payload.get("sort") or "dark_in")
    return upsert("dark_rank", d, payload, skey=sort, asof=payload.get("asof"))


def save_market(payload: dict[str, Any], *, day: Optional[str] = None) -> bool:
    if not isinstance(payload, dict) or not payload.get("ok"):
        return False
    # prefer session day from payload
    sess = payload.get("session") if isinstance(payload.get("session"), dict) else {}
    d = day or sess.get("day") or trading_day_guess(payload.get("asof"))
    return upsert("market", d, payload, skey="overview", asof=payload.get("asof"))


def save_structure(payload: dict[str, Any], *, day: Optional[str] = None) -> bool:
    if not isinstance(payload, dict):
        return False
    d = day or trading_day_guess(payload.get("asof"))
    return upsert("structure", d, payload, skey="etf", asof=payload.get("asof"))


# ensure DB on import
try:
    init_db()
except Exception as e:
    log.warning("history init_db failed: %s", e)
