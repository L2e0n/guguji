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
from pathlib import Path

from flask import Flask, request, jsonify
from flask_cors import CORS
from PIL import Image
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("guguji-ocr")

# Paddle 3.x CPU runtime can fail on some hosts unless PIR is disabled.
os.environ.setdefault("FLAGS_enable_pir_api", "0")

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ["https://ji.guguji.icu"]}})
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
def lookup_fund(name: str) -> dict | None:
    """通过东财搜索接口按名称查询基金代码和最新净值。"""
    normalized_name = normalize_fund_name(name)
    try:
        resp = requests.get(
            "https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx",
            params={"m": "1", "key": name.strip()},
            timeout=5,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        candidates = resp.json().get("Datas", [])
        if not candidates:
            return None

        # 优先精确名称，避免 A/C 类别基金被模糊搜索匹配到相邻产品。
        candidate = next(
            (item for item in candidates if normalize_fund_name(item.get("NAME", "")) == normalized_name),
            candidates[0],
        )
        base_info = candidate.get("FundBaseInfo") or {}
        nav = base_info.get("DWJZ")
        return {
            "code": str(candidate.get("CODE", "")),
            "name": candidate.get("NAME", name),
            "nav": float(nav) if nav not in (None, "") else 0.0,
        }
    except Exception as e:
        log.warning(f"基金名称查询失败 [{name}]: {e}")
    return None


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
    value = text.strip().replace(",", "").replace("+", "")
    return float(value) if is_numeric(value) else None


def looks_like_fund_name(text: str) -> bool:
    """排除收益列和分类标题，保留可能的基金产品名称。"""
    text = text.strip()
    if len(re.findall(r"[一-鿿]", text)) < 4:
        return False
    if re.search(r"按最近|日收益|持有收益|偏债类|收起", text):
        return False
    return bool(re.search(r"基金|债券|混合|货币|指数|股票|联接|FOF|QDII", text, re.I))


def parse_named_holdings(items: list[dict], fund_map: dict) -> list[dict]:
    """解析不展示代码的持仓截图，按名称查询基金并反推份额和成本。"""
    name_cache: dict[str, dict | None] = {}
    funds = []

    for item in items:
        name = item["text"].strip()
        if not looks_like_fund_name(name):
            continue

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
            if 0 < other["y"] - item["y"] <= 100 and other["x"] < 600
        ]
        amount_values = [parse_number(other["text"]) for other in amounts]
        market_value = next((value for value in amount_values if value is not None and value > 0), 0.0)

        # 右侧“持有收益”可用于由持有金额反推成本价。
        profits = [
            other for other in items
            if abs(other["y"] - item["y"]) <= 35 and other["x"] > 1150
        ]
        profit_values = [parse_number(other["text"]) for other in profits]
        holding_profit = next((value for value in profit_values if value is not None), 0.0)

        day_profits = [
            other for other in items
            if abs(other["y"] - item["y"]) <= 35 and 750 < other["x"] < 1150
        ]
        day_profit_values = [parse_number(other["text"]) for other in day_profits]
        day_profit = next((value for value in day_profit_values if value is not None), 0.0)

        dates = [
            other["text"] for other in items
            if 0 < other["y"] - item["y"] <= 70 and 750 < other["x"] < 1150
            and re.match(r"^\d{2}-\d{2}$", other["text"].strip())
        ]
        snapshot_date = dates[0] if dates else ""

        nav = float(matched.get("nav") or 0)
        shares = market_value / nav if market_value > 0 and nav > 0 else 0.0
        cost = (market_value - holding_profit) / shares if shares > 0 else 0.0
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"启动 OCR 服务, 端口: {port}")
    log.info("提示: 首次 OCR 请求会加载模型, 耗时约 10-30 秒")
    app.run(host="0.0.0.0", port=port, debug=False)
