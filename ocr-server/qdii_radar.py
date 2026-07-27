"""QD鸡 · 海外 AI 算力 / 半导体产业链 QDII 扫描雷达（后台逻辑）

职责：
1) 拉取 QDII 候选池（东财基金列表 + 名称关键词 + seed）
2) 按目标股票（硬件核心 + CSP）+ 名称规则打相似度分
3) 维护精池 watch_pool，扫描申购限额变化
4) 写入 radar_events（本阶段不做推送）
"""

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

import qdii_service

log = logging.getLogger("guguji-qdii-radar")

TZ_SH = timezone(timedelta(hours=8))
_db_lock = threading.Lock()
_config_lock = threading.Lock()
_config_cache: tuple[float, dict] | None = None

DEFAULT_CONFIG: dict[str, Any] = {
    "seeds": [
        "539002",
        "017653",
        "017654",
        "016664",
        "016665",
        "501225",
        "016668",
        "016667",
        "001668",
        "015202",
        "006373",
        "016701",
        "005698",
        "018043",
        "017091",
    ],
    "name_keywords_strong": [
        "芯片", "半导体", "晶圆", "光刻", "存储", "纳斯达克科技", "人工智能",
    ],
    "name_keywords_medium": [
        "科技", "数字经济", "智能", "信息", "纳斯达克", "标普信息",
        "高端制造", "互联", "互联网", "新兴市场", "美国", "全球精选",
    ],
    "name_exclude": [
        "债", "货币", "原油", "黄金", "商品", "REIT", "地产",
        "农业", "生物科技", "医疗", "医药", "健康",
    ],
    "score_threshold": 35,
    "name_strong_auto_pool": True,
    "core_holdings": [
        {"id": "TSM", "weight": 1.0, "aliases": ["台积电", "台灣積體", "台湾积体", "TSM", "TSMC"]},
        {"id": "ASML", "weight": 1.0, "aliases": ["阿斯麦", "艾司摩尔", "ASML"]},
        {"id": "AMD", "weight": 1.0, "aliases": ["AMD", "超威", "超微半导体"]},
        {"id": "INTC", "weight": 1.0, "aliases": ["英特尔", "Intel", "INTC"]},
        {"id": "ARM", "weight": 1.0, "aliases": ["ARM", "Arm", "安谋"]},
        {"id": "SMSN", "weight": 1.0, "aliases": ["三星电子", "三星", "Samsung", "SMSN", "005930"]},
        {"id": "MU", "weight": 1.0, "aliases": ["美光", "Micron", "MU"]},
        {"id": "SKHYNIX", "weight": 1.0, "aliases": ["SK海力士", "海力士", "Hynix", "SK hynix", "SKHYNIX"]},
        {"id": "NVDA", "weight": 1.0, "aliases": ["英伟达", "NVIDIA", "NVDA", "輝達", "辉达"]},
        {"id": "MRVL", "weight": 1.0, "aliases": ["Marvell", "迈威尔", "美满电子", "美满", "MRVL"]},
        {"id": "TSEM", "weight": 1.0, "aliases": ["Tower", "Tower Semiconductor", "塔尔半导体", "TSEM"]},
        {"id": "GFS", "weight": 1.0, "aliases": ["GlobalFoundries", "格芯", "格罗方德", "GFS"]},
        {"id": "LITE", "weight": 1.0, "aliases": ["Lumentum", "LITE"]},
        {"id": "COHR", "weight": 1.0, "aliases": ["Coherent", "COHR"]},
    ],
    # CSP / 云与平台巨头：权重略低于半导体硬件核心（默认 0.85）
    "csp_holdings": [
        {"id": "GOOGL", "weight": 0.85, "aliases": ["谷歌", "Google", "Alphabet", "GOOGL", "GOOG"]},
        {"id": "MSFT", "weight": 0.85, "aliases": ["微软", "Microsoft", "MSFT"]},
        {"id": "META", "weight": 0.85, "aliases": ["Meta", "Facebook", "脸书", "META"]},
        {"id": "AAPL", "weight": 0.85, "aliases": ["苹果", "Apple", "AAPL"]},
        {"id": "AMZN", "weight": 0.85, "aliases": ["亚马逊", "Amazon", "AMZN"]},
    ],
    # 兼容旧配置键
    "satellite_holdings": [],
    "holdings_score_cap": 60,
    "name_score_cap": 40,
    "universe_cache_hours": 12,
    "max_workers": 6,
    "quota_batch_size": 40,
    "quota_loosen_ratio": 3.0,
    "quota_open_threshold": 1e9,
    "event_cooldown_hours": 24,
}


def _now() -> datetime:
    return datetime.now(TZ_SH)


def _now_iso() -> str:
    return _now().isoformat(timespec="seconds")


def _config_path() -> Path:
    env = qdii_service.os_environ_get("QDII_RADAR_CONFIG")
    if env:
        return Path(env)
    return Path(__file__).resolve().parent / "qdii_radar_config.json"


def load_config(force: bool = False) -> dict[str, Any]:
    """Load config JSON merged over defaults. Cached 60s."""
    global _config_cache
    with _config_lock:
        if not force and _config_cache and time.time() - _config_cache[0] < 60:
            return json.loads(json.dumps(_config_cache[1]))
        cfg = json.loads(json.dumps(DEFAULT_CONFIG))
        path = _config_path()
        if path.exists():
            try:
                user = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(user, dict):
                    for k, v in user.items():
                        cfg[k] = v
            except Exception as e:
                log.warning("radar config load failed %s: %s", path, e)
        _config_cache = (time.time(), cfg)
        return json.loads(json.dumps(cfg))


def init_radar_db() -> None:
    """Ensure radar tables exist (same sqlite as qdii snapshots)."""
    qdii_service.init_db()
    path = qdii_service._db_path()
    with _db_lock:
        conn = sqlite3.connect(str(path), timeout=30)
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS universe_funds (
                    code TEXT PRIMARY KEY,
                    name TEXT,
                    ftype TEXT,
                    layer TEXT,
                    first_seen_at TEXT,
                    last_seen_at TEXT
                );
                CREATE TABLE IF NOT EXISTS similarity_scores (
                    code TEXT PRIMARY KEY,
                    name TEXT,
                    name_score REAL,
                    holdings_score REAL,
                    total_score REAL,
                    matched_json TEXT,
                    holdings_as_of TEXT,
                    force_watch INTEGER DEFAULT 0,
                    updated_at TEXT
                );
                CREATE TABLE IF NOT EXISTS watch_pool (
                    code TEXT PRIMARY KEY,
                    name TEXT,
                    total_score REAL,
                    reason TEXT,
                    force_watch INTEGER DEFAULT 0,
                    added_at TEXT,
                    updated_at TEXT
                );
                CREATE TABLE IF NOT EXISTS radar_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    code TEXT NOT NULL,
                    name TEXT,
                    title TEXT,
                    detail_json TEXT,
                    score REAL,
                    prev_limit_amount REAL,
                    limit_amount REAL,
                    prev_sgzt TEXT,
                    sgzt TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_radar_events_time ON radar_events(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_radar_events_code ON radar_events(code, created_at DESC);
                CREATE TABLE IF NOT EXISTS scan_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_type TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    ok INTEGER,
                    stats_json TEXT,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS radar_quota_state (
                    code TEXT PRIMARY KEY,
                    name TEXT,
                    sgzt TEXT,
                    sgzt_norm TEXT,
                    limit_amount REAL,
                    buyable INTEGER,
                    updated_at TEXT
                );
                """
            )
            conn.commit()
        finally:
            conn.close()

# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------

_FUND_LIST_URL = "https://fund.eastmoney.com/js/fundcode_search.js"


def fetch_eastmoney_fund_list() -> list[dict]:
    """Parse East Money fundcode search JS into [{code,name,ftype}]."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://fund.eastmoney.com/",
    }
    resp = requests.get(_FUND_LIST_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    text = resp.content.decode("utf-8", errors="replace")
    m = re.search(r"var\s+r\s*=\s*(\[.*\])\s*;?", text, re.S)
    if not m:
        raise ValueError("fundcode_search parse failed")
    arr = json.loads(m.group(1).rstrip(";"))
    out: list[dict] = []
    for row in arr:
        if not isinstance(row, (list, tuple)) or len(row) < 4:
            continue
        code = str(row[0]).strip().zfill(6)
        if not re.fullmatch(r"\d{6}", code):
            continue
        name = str(row[2] or "").strip()
        ftype = str(row[3] or "").strip()
        out.append({"code": code, "name": name, "ftype": ftype})
    return out


def is_qdii_like(item: dict) -> bool:
    ftype = item.get("ftype") or ""
    name = item.get("name") or ""
    if "QDII" in ftype.upper() or "QDII" in name.upper():
        return True
    if "指数型-海外" in ftype or "海外股票" in ftype:
        return True
    return False


def name_excluded(name: str, cfg: dict) -> bool:
    for kw in cfg.get("name_exclude") or []:
        if kw and kw in name:
            return True
    return False


def score_name(name: str, cfg: dict) -> tuple[float, list[str]]:
    """Return (name_score 0..cap, hit keywords)."""
    hits: list[str] = []
    score = 0.0
    for kw in cfg.get("name_keywords_strong") or []:
        if kw and kw in name:
            hits.append(kw)
            score += 25
            break
    for kw in cfg.get("name_keywords_medium") or []:
        if kw and kw in name and kw not in hits:
            hits.append(kw)
            score += 10
            if score >= 40:
                break
    if hits and any(x in name for x in ("全球", "海外", "美国", "纳斯达克", "QDII")):
        score += 5
    cap = float(cfg.get("name_score_cap") or 40)
    return min(cap, score), hits


def build_u1_candidates(all_funds: list[dict], cfg: dict) -> list[dict]:
    """Theme pool: QDII-like + (keyword hit or seed)."""
    seeds = {str(c).zfill(6) for c in (cfg.get("seeds") or [])}
    out: list[dict] = []
    seen = set()
    for item in all_funds:
        code = item["code"]
        name = item.get("name") or ""
        if code in seen:
            continue
        if not is_qdii_like(item) and code not in seeds:
            continue
        if name_excluded(name, cfg) and code not in seeds:
            continue
        nscore, hits = score_name(name, cfg)
        if code in seeds or nscore > 0:
            row = dict(item)
            row["name_score"] = nscore
            row["name_hits"] = hits
            row["force_watch"] = 1 if code in seeds else 0
            out.append(row)
            seen.add(code)
    by_code = {x["code"]: x for x in all_funds}
    for s in seeds:
        if s not in seen:
            base = by_code.get(s) or {"code": s, "name": "", "ftype": "QDII"}
            nscore, hits = score_name(base.get("name") or "", cfg)
            base = dict(base)
            base["name_score"] = nscore
            base["name_hits"] = hits
            base["force_watch"] = 1
            out.append(base)
            seen.add(s)
    return out


def _compile_holding_patterns(cfg: dict) -> list[dict]:
    rows = []
    # core=半导体/算力硬件；csp=云与平台巨头（权重通常略低）
    # satellite_holdings 仅作旧配置兼容
    for bucket, key, default_w in (
        ("core", "core_holdings", 1.0),
        ("csp", "csp_holdings", 0.85),
        ("csp", "satellite_holdings", 0.85),
    ):
        for item in cfg.get(key) or []:
            aliases = [a for a in (item.get("aliases") or []) if a]
            if not aliases:
                continue
            aliases_sorted = sorted(aliases, key=len, reverse=True)
            pattern = re.compile("|".join(re.escape(a) for a in aliases_sorted), re.IGNORECASE)
            rows.append(
                {
                    "id": item.get("id") or aliases[0],
                    "weight": float(item.get("weight") or default_w),
                    "bucket": bucket,
                    "pattern": pattern,
                    "aliases": aliases_sorted,
                }
            )
    return rows


def match_holdings(holdings_items: list[dict], cfg: dict) -> tuple[float, list[dict]]:
    """Score holdings vs target stocks. Returns (score, matched detail)."""
    patterns = _compile_holding_patterns(cfg)
    matched: list[dict] = []
    used_ids = set()
    total = 0.0
    for h in holdings_items or []:
        hname = str(h.get("name") or "")
        hcode = str(h.get("code") or "")
        text = f"{hname} {hcode}"
        pct = h.get("pct")
        try:
            pct_f = float(pct) if pct is not None else 0.0
        except (TypeError, ValueError):
            pct_f = 0.0
        for p in patterns:
            if p["id"] in used_ids:
                continue
            if p["pattern"].search(text):
                contrib = pct_f * float(p["weight"])
                total += contrib
                matched.append(
                    {
                        "id": p["id"],
                        "bucket": p["bucket"],
                        "name": hname,
                        "pct": pct_f,
                        "weight": p["weight"],
                        "contrib": round(contrib, 4),
                    }
                )
                used_ids.add(p["id"])
                break
    cap = float(cfg.get("holdings_score_cap") or 60)
    # Amplify so multi-core concentration reaches threshold more naturally.
    score = min(cap, total * 1.5)
    return round(score, 2), matched


def score_fund(code: str, name: str, cfg: dict, use_holdings_cache: bool = True) -> dict:
    nscore, name_hits = score_name(name or "", cfg)
    holdings = qdii_service.get_holdings(code, use_cache=use_holdings_cache)
    hscore, matched = match_holdings(holdings.get("items") or [], cfg)
    total = round(min(100.0, nscore + hscore), 2)
    force = 1 if str(code).zfill(6) in {str(c).zfill(6) for c in (cfg.get("seeds") or [])} else 0
    strong_auto = bool(cfg.get("name_strong_auto_pool"))
    strong_hit = any(k in (name or "") for k in (cfg.get("name_keywords_strong") or []))
    thr = float(cfg.get("score_threshold") or 35)
    in_pool = bool(force or total >= thr or (strong_auto and strong_hit))
    reason_parts = []
    if force:
        reason_parts.append("seed")
    if strong_hit:
        reason_parts.append("name_strong")
    if total >= thr:
        reason_parts.append("score")
    return {
        "code": str(code).zfill(6),
        "name": name,
        "name_score": nscore,
        "name_hits": name_hits,
        "holdings_score": hscore,
        "total_score": total,
        "matched": matched,
        "holdings_as_of": holdings.get("as_of") or "",
        "holdings_count": holdings.get("count") or 0,
        "force_watch": force,
        "in_pool": in_pool,
        "reason": "+".join(reason_parts) if reason_parts else "below_threshold",
        "updated_at": _now_iso(),
    }

# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    path = qdii_service._db_path()
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def upsert_universe(rows: list[dict], layer: str = "U1") -> None:
    init_radar_db()
    now = _now_iso()
    with _db_lock:
        conn = _conn()
        try:
            for r in rows:
                code = r["code"]
                cur = conn.execute("SELECT code FROM universe_funds WHERE code=?", (code,))
                if cur.fetchone():
                    conn.execute(
                        "UPDATE universe_funds SET name=?, ftype=?, layer=?, last_seen_at=? WHERE code=?",
                        (r.get("name") or "", r.get("ftype") or "", layer, now, code),
                    )
                else:
                    conn.execute(
                        "INSERT INTO universe_funds(code,name,ftype,layer,first_seen_at,last_seen_at) VALUES(?,?,?,?,?,?)",
                        (code, r.get("name") or "", r.get("ftype") or "", layer, now, now),
                    )
            conn.commit()
        finally:
            conn.close()


def save_scores(scores: list[dict]) -> None:
    init_radar_db()
    now = _now_iso()
    with _db_lock:
        conn = _conn()
        try:
            for s in scores:
                conn.execute(
                    """
                    INSERT INTO similarity_scores(
                        code,name,name_score,holdings_score,total_score,
                        matched_json,holdings_as_of,force_watch,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(code) DO UPDATE SET
                        name=excluded.name,
                        name_score=excluded.name_score,
                        holdings_score=excluded.holdings_score,
                        total_score=excluded.total_score,
                        matched_json=excluded.matched_json,
                        holdings_as_of=excluded.holdings_as_of,
                        force_watch=excluded.force_watch,
                        updated_at=excluded.updated_at
                    """,
                    (
                        s["code"],
                        s.get("name") or "",
                        float(s.get("name_score") or 0),
                        float(s.get("holdings_score") or 0),
                        float(s.get("total_score") or 0),
                        json.dumps(s.get("matched") or [], ensure_ascii=False),
                        s.get("holdings_as_of") or "",
                        int(s.get("force_watch") or 0),
                        s.get("updated_at") or now,
                    ),
                )
            conn.commit()
        finally:
            conn.close()


def rebuild_watch_pool(scores: list[dict]) -> tuple[list[dict], list[dict]]:
    """Replace watch_pool with in_pool scores. Returns (pool, newly_added)."""
    init_radar_db()
    now = _now_iso()
    pool = [s for s in scores if s.get("in_pool")]
    pool.sort(key=lambda x: (-float(x.get("total_score") or 0), x.get("code") or ""))
    newly: list[dict] = []
    with _db_lock:
        conn = _conn()
        try:
            old = {
                r["code"]: dict(r)
                for r in conn.execute(
                    "SELECT code, name, total_score, reason, force_watch, added_at FROM watch_pool"
                ).fetchall()
            }
            conn.execute("DELETE FROM watch_pool")
            for s in pool:
                code = s["code"]
                if code not in old:
                    newly.append(s)
                    added_at = now
                else:
                    added_at = old[code].get("added_at") or now
                conn.execute(
                    """
                    INSERT INTO watch_pool(code,name,total_score,reason,force_watch,added_at,updated_at)
                    VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        code,
                        s.get("name") or "",
                        float(s.get("total_score") or 0),
                        s.get("reason") or "",
                        int(s.get("force_watch") or 0),
                        added_at,
                        now,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
    return pool, newly


def insert_event(
    event_type: str,
    code: str,
    name: str,
    title: str,
    detail: dict,
    score: float | None = None,
    prev_limit: float | None = None,
    limit_amount: float | None = None,
    prev_sgzt: str | None = None,
    sgzt: str | None = None,
) -> int:
    init_radar_db()
    with _db_lock:
        conn = _conn()
        try:
            cur = conn.execute(
                """
                INSERT INTO radar_events(
                    event_type, code, name, title, detail_json, score,
                    prev_limit_amount, limit_amount, prev_sgzt, sgzt, created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event_type,
                    code,
                    name or "",
                    title or "",
                    json.dumps(detail or {}, ensure_ascii=False),
                    score,
                    prev_limit,
                    limit_amount,
                    prev_sgzt,
                    sgzt,
                    _now_iso(),
                ),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            conn.close()


def recent_event_exists(code: str, event_type: str, hours: float) -> bool:
    init_radar_db()
    cutoff = (_now() - timedelta(hours=hours)).isoformat(timespec="seconds")
    with _db_lock:
        conn = _conn()
        try:
            row = conn.execute(
                """
                SELECT id FROM radar_events
                WHERE code=? AND event_type=? AND created_at>=?
                ORDER BY id DESC LIMIT 1
                """,
                (code, event_type, cutoff),
            ).fetchone()
            return row is not None
        finally:
            conn.close()


def log_scan_run(run_type: str, started_at: str, ok: bool, stats: dict, error: str | None = None) -> None:
    init_radar_db()
    with _db_lock:
        conn = _conn()
        try:
            conn.execute(
                """
                INSERT INTO scan_runs(run_type, started_at, finished_at, ok, stats_json, error)
                VALUES(?,?,?,?,?,?)
                """,
                (
                    run_type,
                    started_at,
                    _now_iso(),
                    1 if ok else 0,
                    json.dumps(stats or {}, ensure_ascii=False),
                    error,
                ),
            )
            conn.commit()
        finally:
            conn.close()


def _limit_rank(limit_amount: float | None, sgzt_norm: str | None, buyable: bool | None) -> float:
    """Higher = easier to buy."""
    if sgzt_norm == "paused" or buyable is False:
        return -1.0
    if limit_amount is None:
        return 1e12 if sgzt_norm == "open" else 0.0
    if limit_amount >= qdii_service.OPEN_MAXSG_THRESHOLD or sgzt_norm == "open":
        return 1e12
    return float(limit_amount)


def is_quota_loosen(prev: dict, cur: dict, cfg: dict) -> bool:
    ratio = float(cfg.get("quota_loosen_ratio") or 3.0)
    prev_rank = _limit_rank(prev.get("limit_amount"), prev.get("sgzt_norm"), prev.get("buyable"))
    cur_rank = _limit_rank(cur.get("limit_amount"), cur.get("sgzt_norm"), cur.get("buyable"))
    if cur_rank <= prev_rank:
        return False
    if prev_rank < 0 and cur_rank >= 0:
        return True
    open_thr = float(cfg.get("quota_open_threshold") or 1e9)
    if prev_rank < open_thr <= cur_rank:
        return True
    if prev_rank > 0 and cur_rank >= prev_rank * ratio:
        return True
    if prev_rank >= 0 and cur_rank - prev_rank >= 900 and cur_rank >= 1000:
        return True
    return False


def is_quota_tighten(prev: dict, cur: dict, cfg: dict) -> bool:
    prev_rank = _limit_rank(prev.get("limit_amount"), prev.get("sgzt_norm"), prev.get("buyable"))
    cur_rank = _limit_rank(cur.get("limit_amount"), cur.get("sgzt_norm"), cur.get("buyable"))
    if cur_rank >= prev_rank:
        return False
    if prev_rank >= 0 and cur_rank < 0:
        return True
    open_thr = float(cfg.get("quota_open_threshold") or 1e9)
    if prev_rank >= open_thr > cur_rank:
        return True
    if cur_rank > 0 and prev_rank >= cur_rank * float(cfg.get("quota_loosen_ratio") or 3.0):
        return True
    return False


def format_limit(limit_amount: float | None, sgzt_norm: str | None, buyable: bool | None) -> str:
    if sgzt_norm == "paused" or buyable is False:
        return "暂停申购"
    if limit_amount is None:
        return "未知"
    if limit_amount >= qdii_service.OPEN_MAXSG_THRESHOLD or sgzt_norm == "open":
        return "开放申购"
    if limit_amount >= 10000 and float(limit_amount) % 10000 == 0:
        return f"{limit_amount/10000:.0f}万元"
    return f"{limit_amount:.0f}元"

# ---------------------------------------------------------------------------
# Scan pipelines
# ---------------------------------------------------------------------------

def run_universe_and_score(refresh_holdings: bool = False) -> dict:
    """Daily-ish: refresh U1, score holdings, rebuild watch pool, emit E2 new-in-pool."""
    started = _now_iso()
    cfg = load_config(force=True)
    init_radar_db()
    stats: dict[str, Any] = {"phase": "universe_score"}
    try:
        all_funds = fetch_eastmoney_fund_list()
        stats["all_funds"] = len(all_funds)
        u1 = build_u1_candidates(all_funds, cfg)
        stats["u1"] = len(u1)
        upsert_universe(u1, layer="U1")

        scores: list[dict] = []
        max_workers = int(cfg.get("max_workers") or 6)
        seeds = {str(c).zfill(6) for c in (cfg.get("seeds") or [])}

        def _one(item: dict) -> dict:
            return score_fund(
                item["code"],
                item.get("name") or "",
                cfg,
                use_holdings_cache=not refresh_holdings,
            )

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futs = {pool.submit(_one, it): it["code"] for it in u1}
            for fut in as_completed(futs):
                code = futs[fut]
                try:
                    scores.append(fut.result())
                except Exception as e:
                    log.warning("score failed %s: %s", code, e)
                    scores.append(
                        {
                            "code": code,
                            "name": "",
                            "name_score": 0,
                            "holdings_score": 0,
                            "total_score": 0,
                            "matched": [],
                            "force_watch": 1 if code in seeds else 0,
                            "in_pool": code in seeds,
                            "reason": "error",
                            "error": str(e),
                            "updated_at": _now_iso(),
                        }
                    )

        # 冷启动（精池此前为空）只建基线，不刷 E2 上新事件
        prev_pool_n = 0
        with _db_lock:
            conn = _conn()
            try:
                prev_pool_n = int(
                    conn.execute("SELECT COUNT(*) AS n FROM watch_pool").fetchone()["n"]
                )
            finally:
                conn.close()

        save_scores(scores)
        pool_rows, newly = rebuild_watch_pool(scores)
        stats["scored"] = len(scores)
        stats["pool"] = len(pool_rows)
        stats["newly"] = len(newly)
        stats["cold_start"] = prev_pool_n == 0

        cooldown = float(cfg.get("event_cooldown_hours") or 24)
        new_events = 0
        if prev_pool_n > 0:
            for s in newly:
                if recent_event_exists(s["code"], "E2_new_pool", cooldown):
                    continue
                title = f"上新入池 {s['code']} {s.get('name') or ''}".strip()
                insert_event(
                    "E2_new_pool",
                    s["code"],
                    s.get("name") or "",
                    title,
                    {
                        "total_score": s.get("total_score"),
                        "name_score": s.get("name_score"),
                        "holdings_score": s.get("holdings_score"),
                        "matched": (s.get("matched") or [])[:8],
                        "reason": s.get("reason"),
                    },
                    score=float(s.get("total_score") or 0),
                )
                new_events += 1
        stats["e2_events"] = new_events
        log_scan_run("universe_score", started, True, stats)
        return {"ok": True, "stats": stats, "pool": pool_rows[:50], "newly": newly[:20]}
    except Exception as e:
        log.exception("universe_score failed")
        log_scan_run("universe_score", started, False, stats, error=str(e))
        return {"ok": False, "error": str(e), "stats": stats}


def _get_watch_pool_codes() -> list[dict]:
    init_radar_db()
    with _db_lock:
        conn = _conn()
        try:
            rows = conn.execute(
                "SELECT code, name, total_score, reason, force_watch FROM watch_pool ORDER BY total_score DESC"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def _get_quota_state(code: str) -> dict | None:
    with _db_lock:
        conn = _conn()
        try:
            row = conn.execute(
                "SELECT code,name,sgzt,sgzt_norm,limit_amount,buyable,updated_at FROM radar_quota_state WHERE code=?",
                (code,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()


def _save_quota_state(rec: dict) -> None:
    with _db_lock:
        conn = _conn()
        try:
            conn.execute(
                """
                INSERT INTO radar_quota_state(code,name,sgzt,sgzt_norm,limit_amount,buyable,updated_at)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(code) DO UPDATE SET
                    name=excluded.name,
                    sgzt=excluded.sgzt,
                    sgzt_norm=excluded.sgzt_norm,
                    limit_amount=excluded.limit_amount,
                    buyable=excluded.buyable,
                    updated_at=excluded.updated_at
                """,
                (
                    rec["code"],
                    rec.get("name") or "",
                    rec.get("sgzt") or "",
                    rec.get("sgzt_norm") or "",
                    rec.get("limit_amount"),
                    1 if rec.get("buyable") else 0,
                    rec.get("updated_at") or _now_iso(),
                ),
            )
            conn.commit()
        finally:
            conn.close()


def run_quota_scan(use_cache: bool = False) -> dict:
    """Intraday: scan watch pool purchase limits and emit E1/E3 events."""
    started = _now_iso()
    cfg = load_config()
    init_radar_db()
    stats: dict[str, Any] = {"phase": "quota_scan"}
    try:
        pool = _get_watch_pool_codes()
        if not pool:
            boot = run_universe_and_score(refresh_holdings=False)
            if not boot.get("ok"):
                return boot
            pool = _get_watch_pool_codes()
        stats["pool"] = len(pool)
        codes = [p["code"] for p in pool]
        batch_size = min(int(cfg.get("quota_batch_size") or 40), qdii_service.BATCH_MAX)
        items: list[dict] = []
        for i in range(0, len(codes), batch_size):
            chunk = codes[i : i + batch_size]
            items.extend(qdii_service.fetch_qdii_batch(chunk, use_cache=use_cache))

        by_code = {it.get("code"): it for it in items if it.get("code")}
        e1 = e3 = 0
        cooldown = float(cfg.get("event_cooldown_hours") or 24)
        score_map = {p["code"]: p for p in pool}

        for code in codes:
            cur = by_code.get(code)
            if not cur or cur.get("ok") is False:
                continue
            cur_state = {
                "code": code,
                "name": cur.get("name") or score_map.get(code, {}).get("name") or "",
                "sgzt": cur.get("sgzt") or "",
                "sgzt_norm": cur.get("sgzt_norm") or "",
                "limit_amount": cur.get("limit_amount"),
                "buyable": bool(cur.get("buyable")),
                "updated_at": cur.get("updated_at") or _now_iso(),
            }
            prev = _get_quota_state(code)
            _save_quota_state(cur_state)
            if not prev:
                continue
            prev_state = {
                "limit_amount": prev.get("limit_amount"),
                "sgzt_norm": prev.get("sgzt_norm"),
                "buyable": bool(prev.get("buyable")),
                "sgzt": prev.get("sgzt"),
            }
            score = float(score_map.get(code, {}).get("total_score") or 0)
            if is_quota_loosen(prev_state, cur_state, cfg):
                if not recent_event_exists(code, "E1_quota_loosen", cooldown):
                    prev_txt = format_limit(
                        prev_state.get("limit_amount"), prev_state.get("sgzt_norm"), prev_state.get("buyable")
                    )
                    cur_txt = format_limit(
                        cur_state.get("limit_amount"), cur_state.get("sgzt_norm"), cur_state.get("buyable")
                    )
                    title = f"放额 {code} {cur_state['name']}: {prev_txt} → {cur_txt}"
                    insert_event(
                        "E1_quota_loosen",
                        code,
                        cur_state["name"],
                        title,
                        {"prev": prev_txt, "curr": cur_txt, "prev_raw": prev_state, "curr_raw": cur_state},
                        score=score,
                        prev_limit=prev_state.get("limit_amount"),
                        limit_amount=cur_state.get("limit_amount"),
                        prev_sgzt=prev_state.get("sgzt"),
                        sgzt=cur_state.get("sgzt"),
                    )
                    e1 += 1
            elif is_quota_tighten(prev_state, cur_state, cfg):
                if not recent_event_exists(code, "E3_quota_tighten", cooldown):
                    prev_txt = format_limit(
                        prev_state.get("limit_amount"), prev_state.get("sgzt_norm"), prev_state.get("buyable")
                    )
                    cur_txt = format_limit(
                        cur_state.get("limit_amount"), cur_state.get("sgzt_norm"), cur_state.get("buyable")
                    )
                    title = f"从紧 {code} {cur_state['name']}: {prev_txt} → {cur_txt}"
                    insert_event(
                        "E3_quota_tighten",
                        code,
                        cur_state["name"],
                        title,
                        {"prev": prev_txt, "curr": cur_txt, "prev_raw": prev_state, "curr_raw": cur_state},
                        score=score,
                        prev_limit=prev_state.get("limit_amount"),
                        limit_amount=cur_state.get("limit_amount"),
                        prev_sgzt=prev_state.get("sgzt"),
                        sgzt=cur_state.get("sgzt"),
                    )
                    e3 += 1

        stats["fetched"] = len(items)
        stats["e1_events"] = e1
        stats["e3_events"] = e3
        log_scan_run("quota_scan", started, True, stats)
        return {"ok": True, "stats": stats}
    except Exception as e:
        log.exception("quota_scan failed")
        log_scan_run("quota_scan", started, False, stats, error=str(e))
        return {"ok": False, "error": str(e), "stats": stats}


def run_full_scan(refresh_holdings: bool = False, use_quota_cache: bool = False) -> dict:
    """Universe score + quota scan."""
    a = run_universe_and_score(refresh_holdings=refresh_holdings)
    b = run_quota_scan(use_cache=use_quota_cache) if a.get("ok") else {"ok": False, "skipped": True}
    return {
        "ok": bool(a.get("ok") and b.get("ok")),
        "universe": a,
        "quota": b,
        "finished_at": _now_iso(),
    }


# ---------------------------------------------------------------------------
# Query APIs
# ---------------------------------------------------------------------------

def get_pool(limit: int = 100) -> list[dict]:
    init_radar_db()
    limit = max(1, min(int(limit), 500))
    with _db_lock:
        conn = _conn()
        try:
            rows = conn.execute(
                """
                SELECT w.code, w.name, w.total_score, w.reason, w.force_watch,
                       w.added_at, w.updated_at,
                       s.name_score, s.holdings_score, s.matched_json, s.holdings_as_of,
                       q.sgzt, q.sgzt_norm, q.limit_amount, q.buyable, q.updated_at AS quota_updated_at
                FROM watch_pool w
                LEFT JOIN similarity_scores s ON s.code = w.code
                LEFT JOIN radar_quota_state q ON q.code = w.code
                ORDER BY w.total_score DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                try:
                    d["matched"] = json.loads(d.pop("matched_json") or "[]")
                except Exception:
                    d["matched"] = []
                    d.pop("matched_json", None)
                d["buyable"] = bool(d.get("buyable")) if d.get("buyable") is not None else None
                d["limit_text"] = format_limit(d.get("limit_amount"), d.get("sgzt_norm"), d.get("buyable"))
                out.append(d)
            return out
        finally:
            conn.close()


def get_events(days: int = 7, limit: int = 100, event_type: str | None = None) -> list[dict]:
    init_radar_db()
    days = max(1, min(int(days), 90))
    limit = max(1, min(int(limit), 500))
    cutoff = (_now() - timedelta(days=days)).isoformat(timespec="seconds")
    with _db_lock:
        conn = _conn()
        try:
            if event_type:
                rows = conn.execute(
                    """
                    SELECT id, event_type, code, name, title, detail_json, score,
                           prev_limit_amount, limit_amount, prev_sgzt, sgzt, created_at
                    FROM radar_events
                    WHERE created_at >= ? AND event_type = ?
                    ORDER BY id DESC LIMIT ?
                    """,
                    (cutoff, event_type, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, event_type, code, name, title, detail_json, score,
                           prev_limit_amount, limit_amount, prev_sgzt, sgzt, created_at
                    FROM radar_events
                    WHERE created_at >= ?
                    ORDER BY id DESC LIMIT ?
                    """,
                    (cutoff, limit),
                ).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                try:
                    d["detail"] = json.loads(d.pop("detail_json") or "{}")
                except Exception:
                    d["detail"] = {}
                    d.pop("detail_json", None)
                out.append(d)
            return out
        finally:
            conn.close()


def get_status() -> dict:
    init_radar_db()
    cfg = load_config()
    with _db_lock:
        conn = _conn()
        try:
            pool_n = conn.execute("SELECT COUNT(*) AS n FROM watch_pool").fetchone()["n"]
            uni_n = conn.execute("SELECT COUNT(*) AS n FROM universe_funds").fetchone()["n"]
            ev_n = conn.execute("SELECT COUNT(*) AS n FROM radar_events").fetchone()["n"]
            last = conn.execute(
                "SELECT id, run_type, started_at, finished_at, ok, stats_json, error FROM scan_runs ORDER BY id DESC LIMIT 5"
            ).fetchall()
            last_runs = []
            for r in last:
                d = dict(r)
                try:
                    d["stats"] = json.loads(d.pop("stats_json") or "{}")
                except Exception:
                    d["stats"] = {}
                    d.pop("stats_json", None)
                d["ok"] = bool(d.get("ok"))
                last_runs.append(d)
        finally:
            conn.close()
    return {
        "ok": True,
        "time": _now_iso(),
        "pool_count": pool_n,
        "universe_count": uni_n,
        "events_count": ev_n,
        "score_threshold": cfg.get("score_threshold"),
        "core_targets": [x.get("id") for x in (cfg.get("core_holdings") or [])],
        "csp_targets": [x.get("id") for x in (cfg.get("csp_holdings") or cfg.get("satellite_holdings") or [])],
        "satellite_targets": [x.get("id") for x in (cfg.get("csp_holdings") or cfg.get("satellite_holdings") or [])],
        "seeds": cfg.get("seeds") or [],
        "last_runs": last_runs,
        "push_enabled": False,
        "note": "后台扫描已启用；推送未接入",
    }


def get_scores(limit: int = 100, min_score: float = 0) -> list[dict]:
    init_radar_db()
    limit = max(1, min(int(limit), 500))
    with _db_lock:
        conn = _conn()
        try:
            rows = conn.execute(
                """
                SELECT code, name, name_score, holdings_score, total_score,
                       matched_json, holdings_as_of, force_watch, updated_at
                FROM similarity_scores
                WHERE total_score >= ? OR force_watch = 1
                ORDER BY total_score DESC
                LIMIT ?
                """,
                (float(min_score), limit),
            ).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                try:
                    d["matched"] = json.loads(d.pop("matched_json") or "[]")
                except Exception:
                    d["matched"] = []
                    d.pop("matched_json", None)
                out.append(d)
            return out
        finally:
            conn.close()
