# -*- coding: utf-8 -*-
"""Minimal local server for sector-flow API only (no OCR/PIL)."""
from flask import Flask, jsonify, request
from flask_cors import CORS
import sector_flow

app = Flask(__name__)
CORS(app)


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "sector-flow-dev"})


@app.get("/api/sector/health")
def sector_health():
    return jsonify(sector_flow.health())


@app.get("/api/sector/flow")
def sector_flow_list():
    board_type = (request.args.get("type") or "industry").strip()
    period = (request.args.get("period") or "1").strip()
    sort = (request.args.get("sort") or "in").strip()
    try:
        limit = int(request.args.get("limit", 50))
    except ValueError:
        limit = 50
    try:
        page = int(request.args.get("page", 1))
    except ValueError:
        page = 1
    primary_only = request.args.get("primary_only", "1").lower() not in ("0", "false", "no")
    refresh = request.args.get("refresh", "").lower() in ("1", "true", "yes")
    try:
        return jsonify(
            sector_flow.fetch_board_flow(
                board_type=board_type,
                period=period,
                sort=sort,
                limit=limit,
                page=page,
                primary_only=primary_only,
                refresh=refresh,
            )
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/sector/dual")
def sector_dual():
    board_type = (request.args.get("type") or "industry").strip()
    period = (request.args.get("period") or "1").strip()
    try:
        top = int(request.args.get("top", 20))
    except ValueError:
        top = 20
    primary_only = request.args.get("primary_only", "1").lower() not in ("0", "false", "no")
    refresh = request.args.get("refresh", "").lower() in ("1", "true", "yes")
    try:
        return jsonify(
            sector_flow.dual_rank(
                board_type=board_type,
                period=period,
                top=top,
                primary_only=primary_only,
                refresh=refresh,
            )
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/sector/<code>/members")
def sector_members(code):
    try:
        limit = int(request.args.get("limit", 30))
    except ValueError:
        limit = 30
    sort = (request.args.get("sort") or "in").strip()
    refresh = request.args.get("refresh", "").lower() in ("1", "true", "yes")
    try:
        return jsonify(
            sector_flow.fetch_board_members(
                board_code=code, limit=limit, sort=sort, refresh=refresh
            )
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
