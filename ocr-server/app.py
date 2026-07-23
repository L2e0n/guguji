#!/usr/bin/env python3
"""
咕咕鸡大作战 - OCR持仓识别服务
基于 PaddleOCR 的基金持仓截图识别

启动:
  pip install -r requirements.txt
  python app.py

访问:
  http://localhost:5000
"""

import os
import re
import json
import hmac
import logging
import time
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path

from flask import Flask, request, jsonify
from flask_cors import CORS
from PIL import Image
import requests

import qdii_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("guguji-ocr")

# Paddle 3.x CPU runtime can fail on some hosts unless PIR is disabled.
os.environ.setdefault("FLAGS_enable_pir_api", "0")

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": [
    "https://ji.guguji.icu",
    "https://qdii.guguji.icu",
    "https://ocr.guguji.icu",
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
]}})
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB
app.config["UPLOAD_FOLDER"] = "/tmp/guguji_ocr"
Image.MAX_IMAGE_PIXELS = 20_000_000

# PaddleOCR (延迟加载, 首次请求时初始化)
_ocr = None


def has_valid_worker_token() -> bool:
    """仅允许 Cloudflare Worker 调用公开 Tunnel 后的 OCR 接口。"""
    secret = os.environ.get("OCR_SHARED_SECRET", "")
    provided = request.headers.get("X-OCR-Token", "")
    return bool(secret) and hmac.compare_digest(provided, secret)

def get_ocr():
    global _ocr
    if _ocr is None:
        log.info("正在加载 PaddleOCR 模型(首次加载较慢)...")
        from paddleocr import PaddleOCR
        try:
            _ocr = PaddleOCR(use_angle_cls=False, lang="ch", show_log=False, enable_mkldnn=False)
        except (TypeError, ValueError):
            # Newer PaddleOCR builds removed some legacy kwargs such as show_log.
            _ocr = PaddleOCR(use_angle_cls=False, lang="ch", enable_mkldnn=False)
        log.info("PaddleOCR 模型加载完成")
    return _ocr


# ── 基金代码 → 名称 简易映射 ──
FUND_CODE_MAP_PATH = Path(__file__).parent / "fund_codes.json"
_fund_code_map = None
_fund_catalog = None
FUND_CATALOG_URL = "https://fund.eastmoney.com/js/fundcode_search.js"
FUND_CATALOG_CACHE_PATH = Path("/tmp/guguji_fund_catalog.json")
FUND_CATALOG_MAX_AGE_SECONDS = 7 * 24 * 60 * 60

def load_fund_map():
    global _fund_code_map
    if _fund_code_map is not None:
        return _fund_code_map
    if FUND_CODE_MAP_PATH.exists():
        with open(FUND_CODE_MAP_PATH, encoding="utf-8") as f:
            _fund_code_map = json.load(f)
        log.info(f"已加载 {len(_fund_code_map)} 个基金代码映射")
    else:
        _fund_code_map = {}
        log.warning("fund_codes.json 不存在, 将尝试在线查询基金代码")
    return _fund_code_map


# ── 在线查询基金代码 ──
def canonical_fund_name(name: str) -> str:
    """将 OCR 名称与基金目录中的常见异体统一。"""
    name = normalize_fund_name(name)
    name = re.sub(r"[·•，,。._-]", "", name)
    return (
        name.replace("发起式", "发起")
        .replace("年期", "年")
        .replace("国开行债券", "国开债")
    )


def fund_match_key(name: str) -> str:
    """去除容易因改名而变化的通用后缀，保留基金名称的专有部分。"""
    return re.sub(r"灵活配置|混合", "", canonical_fund_name(name))


def load_fund_catalog() -> list[dict]:
    """加载东财公开基金全量名录，避免搜索接口把股票或指数排在基金前面。"""
    global _fund_catalog
    if _fund_catalog is not None:
        return _fund_catalog
    if FUND_CATALOG_CACHE_PATH.exists():
        age = max(0, time.time() - FUND_CATALOG_CACHE_PATH.stat().st_mtime)
        if age < FUND_CATALOG_MAX_AGE_SECONDS:
            try:
                _fund_catalog = json.loads(FUND_CATALOG_CACHE_PATH.read_text(encoding="utf-8"))
                log.info("已从本地缓存加载 %d 条基金目录", len(_fund_catalog))
                return _fund_catalog
            except (OSError, json.JSONDecodeError):
                pass
    try:
        resp = requests.get(
            FUND_CATALOG_URL,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        match = re.search(r"var r\s*=\s*(\[.*\])\s*;?\s*$", resp.text, re.S)
        if not match:
            raise ValueError("基金目录格式异常")
        _fund_catalog = [
            {
                "code": str(row[0]),
                "name": row[2],
                "key": canonical_fund_name(row[2]),
                "match_key": fund_match_key(row[2]),
            }
            for row in json.loads(match.group(1))
            if len(row) >= 3 and is_fund_code(str(row[0]))
        ]
        FUND_CATALOG_CACHE_PATH.write_text(json.dumps(_fund_catalog, ensure_ascii=False), encoding="utf-8")
        os.chmod(FUND_CATALOG_CACHE_PATH, 0o600)
        log.info("已加载 %d 条基金目录", len(_fund_catalog))
    except Exception as e:
        log.warning("基金目录加载失败: %s", e)
        _fund_catalog = []
    return _fund_catalog


@lru_cache(maxsize=4096)
def lookup_fund_nav(code: str) -> float:
    """按已确认基金代码查询净值，不使用模糊名称搜索结果。"""
    try:
        resp = requests.get(
            "https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx",
            params={"m": "1", "key": code},
            timeout=5,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        candidate = next(
            (item for item in resp.json().get("Datas", []) if str(item.get("CODE", "")) == code),
            None,
        )
        nav = (candidate or {}).get("FundBaseInfo", {}).get("DWJZ")
        return float(nav) if nav not in (None, "") else 0.0
    except Exception as e:
        log.warning("基金净值查询失败 [%s]: %s", code, e)
        return 0.0


def lookup_fund(name: str) -> dict | None:
    """从基金名录精确或高置信匹配基金，不返回股票和指数代码。"""
    key = canonical_fund_name(name)
    if len(key) < 5:
        return None
    catalog = load_fund_catalog()
    exact = next((item for item in catalog if item["key"] == key), None)
    if exact is None:
        prefix_matches = [item for item in catalog if item["key"].startswith(key) or key.startswith(item["key"])]
        if len(prefix_matches) == 1:
            exact = prefix_matches[0]
        else:
            # OCR 可能漏掉“主题”“发起式”等字；限制同一基金公司前缀并要求高相似度。
            candidates = [item for item in catalog if item["key"][:2] == key[:2]]
            search_key = fund_match_key(name)

            def match_score(item: dict) -> float:
                prefix = 0
                for left, right in zip(search_key, item["match_key"]):
                    if left != right:
                        break
                    prefix += 1
                class_bonus = 0.12 if key[-1:] in "ABCD" and item["key"].endswith(key[-1:]) else 0
                return SequenceMatcher(None, search_key, item["match_key"]).ratio() + min(prefix, 8) * 0.06 + class_bonus

            scored = [(match_score(item), item) for item in candidates]
            score, candidate = max(scored, default=(0.0, None), key=lambda pair: pair[0])
            if score >= 0.74:
                exact = candidate
    if exact is None:
        log.info("未匹配到基金名称: %s", name)
        return None
    return {"code": exact["code"], "name": exact["name"], "nav": lookup_fund_nav(exact["code"])}


def normalize_fund_name(name: str) -> str:
    """清理基金名称"""
    name = re.sub(r'[\s　　]+', '', name)
    name = re.sub(r'[\(\)（）]', '', name)
    return name


# ── OCR 识别 ──
def ocr_image(image_path: str) -> list[dict]:
    """对图片进行 OCR, 返回按 y 坐标排序的文本块列表"""
    ocr = get_ocr()
    result = ocr.ocr(image_path)
    if not result or not result[0]:
        return []

    boxes_texts = []
    for line in result[0]:
        if line is None:
            continue
        box, (text, confidence) = line
        if confidence < 0.5:
            continue
        # box: [[x1,y1],[x2,y2],[x3,y3],[x4,y4]]
        y_center = (box[0][1] + box[2][1]) / 2
        x_center = (box[0][0] + box[2][0]) / 2
        boxes_texts.append({
            "text": text.strip(),
            "x": x_center,
            "y": y_center,
            "confidence": confidence,
        })

    # 按 y 排序(从上到下), y 相近的再按 x 排序(从左到右)
    boxes_texts.sort(key=lambda b: (round(b["y"] / 15), b["x"]))
    return boxes_texts


# ── 持仓解析 ──
def is_fund_code(text: str) -> bool:
    """是否为 6 位基金代码"""
    return bool(re.match(r'^\d{6}$', text.strip()))


def is_numeric(text: str) -> bool:
    """是否为数值(带可选负号和小数点)"""
    return bool(re.match(r'^-?\d+(\.\d+)?$', text.strip().replace(",", "")))


def parse_number(text: str) -> float | None:
    """解析截图中带千分位和正负号的金额。"""
    value = text.strip().replace(",", "").replace("+", "").replace("¥", "").replace("￥", "")
    return float(value) if is_numeric(value) else None


def looks_like_fund_name(text: str) -> bool:
    """排除收益列和分类标题，保留可能的基金产品名称。"""
    text = text.strip()
    if len(re.findall(r"[一-鿿]", text)) < 4:
        return False
    if re.search(r"按最近|日收益|持有收益|持仓基金|策略收益|偏债类|收起|查看全部", text):
        return False
    return bool(re.search(r"基金|债券|短债|纯债|混合|货币|指数|股票|联接|FOF|QDII", text, re.I))


def parse_named_holdings(items: list[dict], fund_map: dict) -> list[dict]:
    """解析不展示代码的持仓截图，按名称查询基金并反推份额和成本。"""
    name_cache: dict[str, dict | None] = {}
    funds = []
    viewport_width = max((item["x"] for item in items), default=1)
    name_rows = [item for item in items if looks_like_fund_name(item["text"].strip().lstrip("·• "))]
    name_y_values = sorted(item["y"] for item in name_rows)
    row_gaps = [b - a for a, b in zip(name_y_values, name_y_values[1:]) if b - a > 40]
    default_card_height = sorted(row_gaps)[len(row_gaps) // 2] if row_gaps else 260

    for item_index, item in enumerate(items):
        name = item["text"].strip().lstrip("·• ")
        if not looks_like_fund_name(name):
            continue

        next_name_y = next((y for y in name_y_values if y > item["y"] + 30), None)
        card_height = (next_name_y - item["y"]) if next_name_y else default_card_height
        card_height = max(card_height, default_card_height * 0.6)

        # 手机上的长基金名可能会换行，例如“...发起”下一行补“联接C”。
        suffix = next(
            (
                other["text"].strip()
                for other in items[item_index + 1:]
                if 0 < other["y"] - item["y"] <= card_height * 0.55
                and other["x"] < viewport_width * 0.48
                and re.match(r"^[一-鿿A-Z()（）]+$", other["text"].strip())
                and re.search(r"联接|[A-Z]$", other["text"].strip())
            ),
            "",
        )
        if suffix:
            name += suffix

        normalized_name = normalize_fund_name(name)
        if normalized_name not in name_cache:
            mapped = next(
                (
                    {"code": code, "name": mapped_name, "nav": 0.0}
                    for code, mapped_name in fund_map.items()
                    if normalize_fund_name(mapped_name) == normalized_name
                ),
                None,
            )
            name_cache[normalized_name] = mapped or lookup_fund(name)
        matched = name_cache[normalized_name]
        if not matched or not is_fund_code(matched.get("code", "")):
            continue

        # 这类持仓页会把持有金额放在基金名下一行、左侧同一列。
        amounts = [
            other for other in items
            if 0 < other["y"] - item["y"] <= card_height * 0.5
            and other["x"] < viewport_width * 0.45
        ]
        amount_values = [parse_number(other["text"]) for other in amounts]
        market_value = next((value for value in amount_values if value is not None and value > 0), 0.0)

        # 不同手机截图宽度不同，按基金名右侧同一行的数值列取日收益和持有收益。
        card_row = [
            other for other in items
            if -card_height * 0.15 <= other["y"] - item["y"] <= card_height * 0.45
        ]
        day_numbers = sorted(
            (
                (other["x"], value) for other in card_row
                if viewport_width * 0.45 <= other["x"] < viewport_width * 0.80
                and (value := parse_number(other["text"])) is not None
            ),
            key=lambda pair: pair[0],
        )
        holding_numbers = sorted(
            (
                (other["x"], value) for other in card_row
                if other["x"] >= viewport_width * 0.80
                and (value := parse_number(other["text"])) is not None
            ),
            key=lambda pair: pair[0],
        )
        day_profit = day_numbers[0][1] if day_numbers else 0.0
        holding_profit = holding_numbers[0][1] if holding_numbers else None

        row_rates = sorted(
            (
                (other["x"], parse_number(other["text"].replace("%", "")))
                for other in items
                if 0 < other["y"] - item["y"] <= card_height * 0.65
                and viewport_width * 0.45 <= other["x"] < viewport_width * 0.80
                and other["text"].strip().endswith("%")
            ),
            key=lambda pair: pair[0],
        )
        day_rate = row_rates[0][1] if row_rates else None

        dates = [
            match.group(1) for other in items
            if 0 < other["y"] - item["y"] <= card_height * 0.85
            and other["x"] >= viewport_width * 0.70
            and (match := re.search(r"(\d{2}-\d{2})", other["text"]))
        ]
        snapshot_date = dates[0] if dates else ""

        nav = float(matched.get("nav") or 0)
        # 且慢“策略收益明细”只给日收益率和日收益，可反推当日持有金额。
        if market_value <= 0 and day_rate not in (None, 0) and day_profit:
            market_value = abs(day_profit / (day_rate / 100))
        shares = market_value / nav if market_value > 0 and nav > 0 else 0.0
        cost = (market_value - holding_profit) / shares if shares > 0 and holding_profit is not None else 0.0
        fund = {"code": matched["code"], "name": matched.get("name") or name}
        if shares > 0:
            fund["shares"] = round(shares, 2)
        if cost > 0:
            # 保留内部精度；四位小数仅用于界面展示，避免累计收益被放大舍入误差。
            fund["cost"] = round(cost, 8)
        if shares > 0 and nav > 0:
            # 截图日收盘净值和前一日净值，供前端先展示与原截图一致的收益。
            fund["snapshot_current_price"] = round(nav, 4)
            fund["snapshot_base_price"] = round(nav - day_profit / shares, 4)
            fund["snapshot_date"] = snapshot_date
        funds.append(fund)

    return funds


def parse_ocr_to_funds(items: list[dict]) -> list[dict]:
    """从 OCR 结果中解析出基金持仓列表"""
    fund_map = load_fund_map()
    funds = []

    # 提取所有纯文本行
    lines = [item["text"] for item in items]

    # 策略1: 查找 6位代码 + 附近信息
    # 先找所有 6 位数字(基金代码候选)
    code_candidates = []
    for i, text in enumerate(lines):
        code = text.strip()
        if is_fund_code(code):
            code_candidates.append((i, code))

    # 对于每个候选代码, 取前后几行作为上下文
    for idx, code in code_candidates:
        context_before = lines[max(0, idx - 3):idx]
        context_after = lines[idx + 1:min(len(lines), idx + 4)]

        # 提取名称(通常在代码前面一行/同区域)
        name = ""
        shares = 0
        cost = 0.0

        full_context = context_before + [code] + context_after
        full_text = " ".join(full_context)

        # 查找持有份额
        shares_patterns = [
            r'持有份额[：:]\s*([\d,]+\.?\d*)',
            r'份额[：:]\s*([\d,]+\.?\d*)',
            r'持仓[量份][：:]?\s*([\d,]+\.?\d*)',
            r'([\d,]+\.?\d*)\s*份',
            r'持有\s*([\d,]+\.?\d*)',
        ]
        for p in shares_patterns:
            m = re.search(p, full_text)
            if m:
                shares = float(m.group(1).replace(",", ""))
                break

        # 查找成本价
        cost_patterns = [
            r'成本[价均][：:]\s*([\d,]+\.?\d*)',
            r'持有成本[：:]\s*([\d,]+\.?\d*)',
            r'成本[：:]?\s*([\d,]+\.?\d*)',
            r'单价[：:]?\s*([\d,]+\.?\d*)',
        ]
        for p in cost_patterns:
            m = re.search(p, full_text)
            if m:
                cost = float(m.group(1).replace(",", ""))
                break

        # 尝试从 fund_map 或在线查询获取名称
        name_from_map = fund_map.get(code, "")
        if name_from_map:
            name = name_from_map
        else:
            # 用代码前的文字作为名称
            if context_before:
                name = context_before[-1]
            else:
                try:
                    resp = requests.get(
                        f"https://fundgz.1234567.com.cn/js/{code}.js?rt=1",
                        timeout=3,
                        headers={"User-Agent": "Mozilla/5.0"}
                    )
                    m = re.search(r'"name":"([^"]+)"', resp.text)
                    if m:
                        name = m.group(1)
                except Exception:
                    name = "识别中..."

        fund = {"code": code, "name": name}
        if shares > 0:
            fund["shares"] = shares
        if cost > 0:
            fund["cost"] = cost

        funds.append(fund)

    # 策略2: 截图仅展示基金名称时，按名称反查代码并读取持有金额。
    if not funds:
        funds = parse_named_holdings(items, fund_map)

    return funds


# ── API 路由 ──
@app.route("/health", methods=["GET"])
def health():
    if not has_valid_worker_token():
        return jsonify({"error": "unauthorized"}), 403
    return jsonify({"status": "ok"})


@app.route("/api/ocr", methods=["POST"])
def ocr_upload():
    """接收图片, 返回识别的基金持仓数据"""
    if not has_valid_worker_token():
        return jsonify({"error": "unauthorized", "funds": []}), 403

    if "image" not in request.files:
        return jsonify({"error": "请上传图片", "funds": []}), 400

    file = request.files["image"]
    if not file.filename:
        return jsonify({"error": "文件名为空", "funds": []}), 400

    os.makedirs(app.config["UPLOAD_FOLDER"], mode=0o700, exist_ok=True)
    os.chmod(app.config["UPLOAD_FOLDER"], 0o700)
    ext = os.path.splitext(file.filename)[1] or ".png"
    save_path = os.path.join(app.config["UPLOAD_FOLDER"], f"ocr_{os.urandom(4).hex()}{ext}")
    file.save(save_path)
    os.chmod(save_path, 0o600)
    log.info("收到 OCR 图片")

    try:
        # 压缩大图片
        img = Image.open(save_path)
        max_dim = 2000
        w, h = img.size
        if max(w, h) > max_dim:
            ratio = max_dim / max(w, h)
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
            img.save(save_path, quality=85)
            log.info(f"图片已压缩: {w}x{h} -> {img.size}")

        # OCR 识别
        items = ocr_image(save_path)
        log.info(f"OCR 识别到 {len(items)} 个文本块")

        if not items:
            return jsonify({"error": "未能识别到文字, 请检查图片", "funds": []}), 400

        # 解析持仓
        funds = parse_ocr_to_funds(items)
        log.info(f"解析出 {len(funds)} 个基金")

        return jsonify({"funds": funds, "raw_text": [i["text"] for i in items]})

    except Exception as e:
        log.exception("OCR 处理出错")
        return jsonify({"error": f"识别失败: {str(e)}", "funds": []}), 500
    finally:
        # 无论识别成功、无文字或异常都不保留用户上传的图片。
        try:
            os.remove(save_path)
        except FileNotFoundError:
            pass
        except OSError:
            log.warning("临时 OCR 图片清理失败")


@app.route("/api/lookup", methods=["GET"])
def fund_lookup():
    """查询基金代码对应的名称"""
    code = request.args.get("code", "")
    if not code or not is_fund_code(code):
        return jsonify({"error": "无效的基金代码"}), 400

    fund_map = load_fund_map()
    name = fund_map.get(code)

    if not name:
        try:
            resp = requests.get(
                f"https://fundgz.1234567.com.cn/js/{code}.js?rt=1",
                timeout=3,
                headers={"User-Agent": "Mozilla/5.0"}
            )
            m = re.search(r'"name":"([^"]+)"', resp.text)
            if m:
                name = m.group(1)
        except Exception:
            pass

    return jsonify({"code": code, "name": name or ""})



def _normalize_fundgz_payload(payload):
    """Normalize East Money / Tiantian fundgz payloads to a flat dict."""
    if not isinstance(payload, dict):
        return None
    data = payload.get("Data")
    if isinstance(data, list) and data:
        data = data[0]
    elif isinstance(data, dict):
        pass
    else:
        data = payload
    if not isinstance(data, dict):
        return None
    fundcode = data.get("fundcode") or data.get("fundCode") or data.get("code")
    if not fundcode:
        return None
    gsz = data.get("gsz") if data.get("gsz") is not None else data.get("GSZ")
    try:
        gsz_num = float(gsz)
    except (TypeError, ValueError):
        return None
    if gsz_num <= 0:
        return None
    return {
        "fundcode": str(fundcode),
        "name": data.get("name") or data.get("fundName") or "",
        "dwjz": data.get("dwjz") if data.get("dwjz") is not None else data.get("DWJZ"),
        "gsz": gsz,
        "gszzl": data.get("gszzl") if data.get("gszzl") is not None else data.get("GSZZL"),
        "jzrq": data.get("jzrq") or data.get("JZRQ") or data.get("navDate") or "",
        "gztime": data.get("gztime") or data.get("GZTIME") or data.get("time") or "",
    }


def fetch_eastmoney_fundgz(code: str):
    """Primary valuation source: East Money fundgz (requires eastmoney Referer)."""
    url = f"https://api.fund.eastmoney.com/fund/fundgz?fundCode={code}&_={int(time.time() * 1000)}"
    resp = requests.get(
        url,
        timeout=6,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://fund.eastmoney.com/",
            "Accept": "*/*",
        },
    )
    resp.raise_for_status()
    text = resp.text.strip()
    # tolerate optional JSONP wrapper
    m = re.match(r"^[a-zA-Z_][\w.]*\((.*)\)\s*;?\s*$", text, re.S)
    if m:
        text = m.group(1)
    payload = json.loads(text)
    data = _normalize_fundgz_payload(payload)
    if not data:
        raise ValueError(f"eastmoney empty fundgz for {code}: {payload}")
    data["channel"] = "东财"
    return data


def fetch_tiantian_fundgz(code: str):
    """Fallback valuation source: legacy fundgz.1234567.com.cn JSONP."""
    url = f"https://fundgz.1234567.com.cn/js/{code}.js?rt={int(time.time() * 1000)}"
    resp = requests.get(
        url,
        timeout=6,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://fund.eastmoney.com/",
            "Accept": "*/*",
        },
    )
    resp.raise_for_status()
    text = resp.text.strip()
    if "<html" in text.lower():
        raise ValueError("tiantian returned html 404")
    m = re.search(r"jsonpgz\((.*)\)\s*;?\s*$", text, re.S)
    if not m:
        # sometimes bare object
        payload = json.loads(text)
    else:
        payload = json.loads(m.group(1))
    data = _normalize_fundgz_payload(payload)
    if not data:
        raise ValueError(f"tiantian empty fundgz for {code}")
    data["channel"] = "天天"
    return data


@app.route("/api/fundgz/<code>", methods=["GET"])
@app.route("/api/fundgz", methods=["GET"])
def fund_gz(code=None):
    """Public dual-source fund valuation proxy for ji.guguji.icu.

    Primary: East Money fundgz (with proper Referer)
    Fallback: Tiantian fundgz JSONP
    No worker token required (market data only).
    """
    code = (code or request.args.get("code") or "").strip()
    if not re.fullmatch(r"\d{6}", code):
        return jsonify({"ok": False, "error": "invalid_code"}), 400

    errors = []
    for fetcher in (fetch_eastmoney_fundgz, fetch_tiantian_fundgz):
        try:
            data = fetcher(code)
            return jsonify({"ok": True, **data})
        except Exception as e:
            errors.append(f"{fetcher.__name__}: {e}")
            log.warning("fundgz fallback: %s", e)

    return jsonify({"ok": False, "error": "no_valuation", "detail": errors}), 502



# ── QDII 额度监控 API（供 qdii.guguji.icu）──────────────────────────
@app.route("/api/qdii/health", methods=["GET"])
def qdii_health():
    try:
        qdii_service.init_db()
        return jsonify({"ok": True, "service": "qdii", "source": "eastmoney"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/qdii/batch", methods=["GET", "POST"])
def qdii_batch():
    """Batch purchase-limit snapshots. GET ?codes=016664,539002 or POST JSON {codes:[]}."""
    codes = []
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        codes = body.get("codes") or []
    if not codes:
        raw = request.args.get("codes") or request.args.get("code") or ""
        codes = re.split(r"[\s,;|]+", raw)
    codes = [c.strip() for c in codes if c and c.strip()]
    if not codes:
        return jsonify({"ok": False, "error": "codes_required"}), 400
    if len(codes) > qdii_service.BATCH_MAX:
        return jsonify({"ok": False, "error": f"too_many_codes_max_{qdii_service.BATCH_MAX}"}), 400
    refresh = request.args.get("refresh", "").lower() in ("1", "true", "yes")
    items = qdii_service.fetch_qdii_batch(codes, use_cache=not refresh)
    return jsonify({"ok": True, "count": len(items), "items": items})


@app.route("/api/qdii/<code>", methods=["GET"])
def qdii_one(code):
    """Single fund purchase-limit snapshot (East Money SGZT/MAXSG)."""
    code = (code or "").strip()
    if not re.fullmatch(r"\d{6}", code):
        return jsonify({"ok": False, "error": "invalid_code"}), 400
    refresh = request.args.get("refresh", "").lower() in ("1", "true", "yes")
    try:
        data = qdii_service.fetch_qdii(code, use_cache=not refresh)
        return jsonify({"ok": True, **data})
    except Exception as e:
        log.warning("qdii one failed %s: %s", code, e)
        return jsonify({"ok": False, "code": code, "error": str(e)}), 502


@app.route("/api/qdii/<code>/history", methods=["GET"])
def qdii_history(code):
    code = (code or "").strip()
    if not re.fullmatch(r"\d{6}", code):
        return jsonify({"ok": False, "error": "invalid_code"}), 400
    try:
        limit = int(request.args.get("limit", qdii_service.HISTORY_LIMIT_DEFAULT))
    except ValueError:
        limit = qdii_service.HISTORY_LIMIT_DEFAULT
    rows = qdii_service.get_history(code, limit=limit)
    return jsonify({"ok": True, "code": code, "count": len(rows), "items": rows})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"启动 OCR 服务, 端口: {port}")
    log.info("提示: 首次 OCR 请求会加载模型, 耗时约 10-30 秒")
    app.run(host="0.0.0.0", port=port, debug=False)
