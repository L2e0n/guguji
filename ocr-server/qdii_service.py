"""QDII purchase-limit monitoring service (East Money primary source)."""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

log = logging.getLogger("guguji-qdii")

TZ_SH = timezone(timedelta(hours=8))
EM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
    ),
    "Referer": "https://fund.eastmoney.com/",
    "Accept": "application/json,text/plain,*/*",
}

# Treat as "no practical cap" when MAXSG is huge.
OPEN_MAXSG_THRESHOLD = 1e9
CACHE_TTL_SECONDS = 60
HOLDINGS_CACHE_TTL_SECONDS = 6 * 3600  # quarterly data; refresh a few times/day
BATCH_MAX = 50
HISTORY_LIMIT_DEFAULT = 50

_db_lock = threading.Lock()
_mem_cache: dict[str, tuple[float, dict]] = {}
_mem_lock = threading.Lock()
_holdings_cache: dict[str, tuple[float, dict]] = {}


def _now_iso() -> str:
    return datetime.now(TZ_SH).isoformat(timespec="seconds")


def _db_path() -> Path:
    env = os_environ_get("QDII_DB_PATH")
    if env:
        return Path(env)
    # Prefer persistent data dir next to module; fallback /tmp.
    base = Path(__file__).resolve().parent / "data"
    try:
        base.mkdir(parents=True, exist_ok=True)
        return base / "qdii.db"
    except OSError:
        return Path("/tmp/guguji_qdii.db")


def os_environ_get(key: str, default: str | None = None) -> str | None:
    import os

    return os.environ.get(key, default)


def init_db() -> None:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _db_lock:
        conn = sqlite3.connect(str(path), timeout=10)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    code TEXT PRIMARY KEY,
                    name TEXT,
                    sgzt TEXT,
                    sgzt_norm TEXT,
                    limit_amount REAL,
                    limit_text TEXT,
                    minsg REAL,
                    buyable INTEGER,
                    raw_json TEXT,
                    source TEXT,
                    updated_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    name TEXT,
                    sgzt TEXT,
                    sgzt_norm TEXT,
                    limit_amount REAL,
                    limit_text TEXT,
                    minsg REAL,
                    buyable INTEGER,
                    source TEXT,
                    changed_at TEXT NOT NULL,
                    prev_sgzt TEXT,
                    prev_limit_amount REAL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_code_time ON history(code, changed_at DESC)"
            )
            conn.commit()
        finally:
            conn.close()


def normalize_sgzt(sgzt: str | None, limit_amount: float | None, buyable: bool) -> str:
    text = (sgzt or "").strip()
    if not text:
        return "unknown"
    if "暂停" in text or "停止" in text or "封闭" in text:
        return "paused"
    if "限大额" in text or "限制大额" in text or "限额" in text:
        return "limit"
    if "开放" in text:
        return "open"
    if not buyable:
        return "paused"
    if limit_amount is not None and limit_amount >= 0 and limit_amount < OPEN_MAXSG_THRESHOLD:
        return "limit"
    return "open"


def parse_limit_from_text(*texts: str | None) -> float | None:
    joined = " ".join(t for t in texts if t)
    if not joined:
        return None
    # e.g. 单日累计购买上限1000元 / 上限100元 / 限购50000
    patterns = [
        r"上限\s*([0-9]+(?:\.[0-9]+)?)\s*万",
        r"上限\s*([0-9]+(?:\.[0-9]+)?)\s*元",
        r"限购\s*([0-9]+(?:\.[0-9]+)?)\s*万",
        r"限购\s*([0-9]+(?:\.[0-9]+)?)\s*元",
        r"单日[^\d]{0,12}([0-9]+(?:\.[0-9]+)?)\s*万",
        r"单日[^\d]{0,12}([0-9]+(?:\.[0-9]+)?)\s*元",
    ]
    for i, pat in enumerate(patterns):
        m = re.search(pat, joined)
        if m:
            val = float(m.group(1))
            if "万" in pat:
                val *= 10000
            return val
    return None


def parse_limit_amount(
    maxsg: Any,
    sgzt: str | None,
    sgztmark: str | None,
    trademarklist: list | None,
) -> float | None:
    text_bits = [sgztmark or ""]
    if trademarklist:
        text_bits.extend(str(x) for x in trademarklist)

    # Explicit pause => 0
    st = (sgzt or "") + " " + (sgztmark or "")
    if any(k in st for k in ("暂停申购", "停止申购", "暂停买入")):
        return 0.0

    # Prefer MAXSG when sensible
    try:
        max_val = float(maxsg) if maxsg not in (None, "", "--") else None
    except (TypeError, ValueError):
        max_val = None

    if max_val is not None:
        if max_val <= 0:
            return 0.0
        if max_val < OPEN_MAXSG_THRESHOLD:
            return max_val
        # huge MAXSG => treat as open unless text says otherwise
        text_limit = parse_limit_from_text(*text_bits, sgzt)
        return text_limit  # None means open/no cap

    return parse_limit_from_text(*text_bits, sgzt)


def _to_float(v: Any) -> float | None:
    try:
        if v in (None, "", "--"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def fetch_eastmoney_basic(code: str) -> dict:
    url = (
        "https://fundmobapi.eastmoney.com/FundMNewApi/FundMNBasicInformation"
        f"?FCODE={code}&deviceid=Wap&plat=Wap&product=EFund&version=2.0.0"
    )
    resp = requests.get(url, headers=EM_HEADERS, timeout=8)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("Success") and str(payload.get("ErrCode", "0")) not in ("0", "0.0"):
        raise ValueError(f"eastmoney error: {payload}")
    data = payload.get("Datas")
    if not isinstance(data, dict) or not data.get("FCODE"):
        raise ValueError(f"eastmoney empty for {code}")
    return data


def normalize_record(raw: dict, source: str = "eastmoney") -> dict:
    code = str(raw.get("FCODE") or raw.get("fundcode") or "").zfill(6)
    name = raw.get("SHORTNAME") or raw.get("name") or ""
    sgzt = raw.get("SGZT") or raw.get("sgzt") or ""
    sgztmark = raw.get("SGZTMARK")
    trademarks = raw.get("TRADEMARKLIST") or []
    if isinstance(trademarks, str):
        trademarks = [trademarks]
    maxsg = raw.get("MAXSG")
    minsg = _to_float(raw.get("MINSG"))
    buy_flag = raw.get("BUY")
    if isinstance(buy_flag, str):
        buyable = buy_flag.lower() in ("1", "true", "yes")
    elif buy_flag is None:
        buyable = "暂停" not in str(sgzt)
    else:
        buyable = bool(buy_flag)

    limit_amount = parse_limit_amount(maxsg, sgzt, sgztmark, trademarks)
    limit_text = ""
    if sgztmark:
        limit_text = str(sgztmark)
    elif trademarks:
        limit_text = "；".join(str(x) for x in trademarks)
    elif limit_amount is not None and limit_amount < OPEN_MAXSG_THRESHOLD:
        limit_text = f"单日累计购买上限{int(limit_amount) if limit_amount == int(limit_amount) else limit_amount}元。"
    elif sgzt:
        limit_text = str(sgzt)

    sgzt_norm = normalize_sgzt(sgzt, limit_amount, buyable)
    if sgzt_norm == "paused":
        buyable = False
        if limit_amount is None:
            limit_amount = 0.0

    return {
        "code": code,
        "name": name,
        "ftype": raw.get("FTYPE") or "",
        "sgzt": sgzt or "",
        "sgzt_norm": sgzt_norm,
        "limit_amount": limit_amount,
        "limit_text": limit_text,
        "minsg": minsg,
        "buyable": buyable,
        "shzt": raw.get("SHZT") or "",
        "dwjz": raw.get("DWJZ"),
        "fsrq": raw.get("FSRQ") or "",
        "rzdf": raw.get("RZDF"),
        "jjgs": raw.get("JJGS") or "",
        "source": source,
        "source_time": raw.get("SUBSCRIBETIME") or "",
        "updated_at": _now_iso(),
        "raw_mark": sgztmark,
        "maxsg_raw": maxsg,
    }


def get_cached(code: str) -> dict | None:
    with _mem_lock:
        item = _mem_cache.get(code)
        if not item:
            return None
        ts, data = item
        if time.time() - ts > CACHE_TTL_SECONDS:
            return None
        return dict(data)


def set_cached(code: str, data: dict) -> None:
    with _mem_lock:
        _mem_cache[code] = (time.time(), dict(data))


def save_snapshot(record: dict) -> dict:
    """Persist snapshot; append history when state/limit changes. Returns record + change flag."""
    init_db()
    path = _db_path()
    code = record["code"]
    changed = False
    prev_sgzt = None
    prev_limit = None

    with _db_lock:
        conn = sqlite3.connect(str(path), timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(
                "SELECT sgzt, sgzt_norm, limit_amount, name FROM snapshots WHERE code=?",
                (code,),
            )
            row = cur.fetchone()
            if row is not None:
                prev_sgzt = row["sgzt"]
                prev_limit = row["limit_amount"]
                prev_norm = row["sgzt_norm"]
                # Compare normalized state + limit
                same_limit = (prev_limit is None and record["limit_amount"] is None) or (
                    prev_limit is not None
                    and record["limit_amount"] is not None
                    and abs(float(prev_limit) - float(record["limit_amount"])) < 1e-9
                )
                if prev_norm != record["sgzt_norm"] or not same_limit:
                    changed = True
            else:
                # first sight: also write history as baseline
                changed = True

            conn.execute(
                """
                INSERT INTO snapshots(
                    code, name, sgzt, sgzt_norm, limit_amount, limit_text,
                    minsg, buyable, raw_json, source, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(code) DO UPDATE SET
                    name=excluded.name,
                    sgzt=excluded.sgzt,
                    sgzt_norm=excluded.sgzt_norm,
                    limit_amount=excluded.limit_amount,
                    limit_text=excluded.limit_text,
                    minsg=excluded.minsg,
                    buyable=excluded.buyable,
                    raw_json=excluded.raw_json,
                    source=excluded.source,
                    updated_at=excluded.updated_at
                """,
                (
                    code,
                    record.get("name") or "",
                    record.get("sgzt") or "",
                    record.get("sgzt_norm") or "",
                    record.get("limit_amount"),
                    record.get("limit_text") or "",
                    record.get("minsg"),
                    1 if record.get("buyable") else 0,
                    json.dumps(record, ensure_ascii=False),
                    record.get("source") or "",
                    record.get("updated_at") or _now_iso(),
                ),
            )

            if changed:
                conn.execute(
                    """
                    INSERT INTO history(
                        code, name, sgzt, sgzt_norm, limit_amount, limit_text,
                        minsg, buyable, source, changed_at, prev_sgzt, prev_limit_amount
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        code,
                        record.get("name") or "",
                        record.get("sgzt") or "",
                        record.get("sgzt_norm") or "",
                        record.get("limit_amount"),
                        record.get("limit_text") or "",
                        record.get("minsg"),
                        1 if record.get("buyable") else 0,
                        record.get("source") or "",
                        record.get("updated_at") or _now_iso(),
                        prev_sgzt,
                        prev_limit,
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    out = dict(record)
    out["changed"] = changed
    if changed and prev_sgzt is not None:
        out["prev_sgzt"] = prev_sgzt
        out["prev_limit_amount"] = prev_limit
    return out


def get_history(code: str, limit: int = HISTORY_LIMIT_DEFAULT) -> list[dict]:
    init_db()
    path = _db_path()
    limit = max(1, min(int(limit), 200))
    with _db_lock:
        conn = sqlite3.connect(str(path), timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT code, name, sgzt, sgzt_norm, limit_amount, limit_text,
                       minsg, buyable, source, changed_at, prev_sgzt, prev_limit_amount
                FROM history
                WHERE code=?
                ORDER BY id DESC
                LIMIT ?
                """,
                (code, limit),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()



def fetch_eastmoney_holdings(code: str) -> dict:
    """Top stock holdings from East Money FundMNInverstPosition (prefer top 10)."""
    code = str(code).strip().zfill(6)
    url = (
        "https://fundmobapi.eastmoney.com/FundMNewApi/FundMNInverstPosition"
        f"?FCODE={code}&deviceid=Wap&plat=Wap&product=EFund&version=2.0.0"
    )
    resp = requests.get(url, headers=EM_HEADERS, timeout=8)
    resp.raise_for_status()
    payload = resp.json()
    datas = payload.get("Datas") or {}
    if not isinstance(datas, dict):
        datas = {}
    stocks = datas.get("fundStocks") or []
    if not isinstance(stocks, list):
        stocks = []

    items: list[dict] = []
    for s in stocks:
        if not isinstance(s, dict):
            continue
        name = (s.get("GPJC") or s.get("GPDM") or "").strip()
        if not name:
            continue
        pct = _to_float(s.get("JZBL"))
        items.append(
            {
                "name": name,
                "pct": pct,
                "code": str(s.get("GPDM") or "").strip(),
            }
        )

    # Prefer top 10; if API returns fewer, show what we have (e.g. top 5).
    items = items[:10]

    as_of = payload.get("Expansion") or datas.get("FSRQ") or ""
    return {
        "as_of": str(as_of) if as_of is not None else "",
        "count": len(items),
        "items": items,
        "source": "eastmoney",
    }


def get_holdings(code: str, use_cache: bool = True) -> dict:
    code = str(code).strip().zfill(6)
    if use_cache:
        with _mem_lock:
            hit = _holdings_cache.get(code)
            if hit:
                ts, data = hit
                if time.time() - ts <= HOLDINGS_CACHE_TTL_SECONDS:
                    return dict(data)

    try:
        data = fetch_eastmoney_holdings(code)
    except Exception as e:
        log.warning("holdings fetch failed %s: %s", code, e)
        data = {"as_of": "", "count": 0, "items": [], "source": "eastmoney", "error": str(e)}

    with _mem_lock:
        _holdings_cache[code] = (time.time(), dict(data))
    return dict(data)


def attach_holdings(record: dict, use_cache: bool = True) -> dict:
    out = dict(record)
    code = out.get("code") or ""
    if code:
        out["holdings"] = get_holdings(code, use_cache=use_cache)
    else:
        out["holdings"] = {"as_of": "", "count": 0, "items": []}
    return out


def fetch_qdii(code: str, use_cache: bool = True) -> dict:
    code = str(code).strip().zfill(6)
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("invalid_code")

    if use_cache:
        cached = get_cached(code)
        if cached:
            cached = attach_holdings(cached, use_cache=True)
            cached["cached"] = True
            return cached

    raw = fetch_eastmoney_basic(code)
    record = normalize_record(raw, source="eastmoney")
    record = save_snapshot(record)
    record = attach_holdings(record, use_cache=use_cache)
    record["cached"] = False
    set_cached(code, dict(record))
    return record


def fetch_qdii_batch(codes: list[str], use_cache: bool = True) -> list[dict]:
    uniq: list[str] = []
    seen = set()
    for c in codes:
        c = str(c).strip().zfill(6)
        if re.fullmatch(r"\d{6}", c) and c not in seen:
            seen.add(c)
            uniq.append(c)
    uniq = uniq[:BATCH_MAX]
    results: dict[str, dict] = {}

    # Serve cache hits first
    pending = []
    for c in uniq:
        if use_cache:
            hit = get_cached(c)
            if hit:
                hit = dict(hit)
                hit["cached"] = True
                results[c] = hit
                continue
        pending.append(c)

    if pending:
        with ThreadPoolExecutor(max_workers=min(8, len(pending))) as pool:
            futs = {pool.submit(fetch_qdii, c, False): c for c in pending}
            for fut in as_completed(futs):
                c = futs[fut]
                try:
                    results[c] = fut.result()
                except Exception as e:
                    log.warning("qdii fetch failed %s: %s", c, e)
                    results[c] = {
                        "code": c,
                        "ok": False,
                        "error": str(e),
                        "updated_at": _now_iso(),
                    }

    ordered = []
    for c in uniq:
        item = results.get(c)
        if item is None:
            continue
        if "ok" not in item:
            item = dict(item)
            item["ok"] = "error" not in item
        ordered.append(item)
    return ordered
