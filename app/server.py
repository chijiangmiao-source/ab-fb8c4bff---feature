"""Flask 应用：标定谱系页面与真实接口。"""

from __future__ import annotations

import os
from typing import Any

from flask import Flask, jsonify, render_template, request

from .store import CalibrationStore, StoreError


def create_app(db_path: str | None = None) -> Flask:
    app = Flask(
        __name__,
        template_folder=os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "templates"),
        static_folder=os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "static"),
    )
    db_path = db_path or os.environ.get("CALIBRATION_DB") or str(
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data", "calibration.db"))
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    store = CalibrationStore(db_path)
    app.store = store  # type: ignore[attr-defined]

    def parse_json(*, allow_empty: bool = False) -> dict[str, Any]:
        if allow_empty and not request.get_data(cache=True):
            return {}
        if not request.is_json:
            raise StoreError("INVALID_CONTENT_TYPE",
                             "请求体必须是 application/json", status=400)
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            raise StoreError("INVALID_BODY", "请求体必须是 JSON 对象",
                             status=400)
        return data

    # ---------------- 页面 ---------------- #
    @app.get("/")
    def index() -> str:
        return render_template("index.html")

    # ---------------- 健康端点 ---------------- #
    @app.get("/health")
    def health() -> tuple:
        store.list_records()  # 真实探活：读库
        return jsonify({"status": "ok"}), 200

    # ---------------- 记录 ---------------- #
    @app.post("/api/records")
    def create_record():
        data = parse_json()
        kind = data.get("kind")
        payload = data.get("payload", {})
        parent_ids = data.get("parent_ids")
        record_id = data.get("record_id")  # 可选，测试/导入时指定编号
        if not isinstance(payload, dict):
            raise StoreError("INVALID_PAYLOAD", "payload 必须是对象",
                             status=400)
        if parent_ids is not None and not isinstance(parent_ids, list):
            raise StoreError("INVALID_PARENTS",
                             "parent_ids 必须是数组", status=400)
        if record_id is not None and not isinstance(record_id, str):
            raise StoreError("INVALID_RECORD_ID",
                             "record_id 必须是字符串", status=400)
        record = store.create_record(
            kind, payload, parent_ids, record_id=record_id)
        return jsonify(record), 201

    @app.get("/api/records")
    def list_records():
        return jsonify(store.list_records())

    @app.get("/api/records/<record_id>")
    def get_record(record_id: str):
        return jsonify(store.get_record(record_id))

    # ---------------- 失效裁决 ---------------- #
    @app.post("/api/records/<record_id>/invalidate")
    def invalidate(record_id: str):
        data = parse_json(allow_empty=True)
        operation_id = data.get("operation_id")
        if not operation_id or not isinstance(operation_id, str):
            raise StoreError(
                "OPERATION_ID_REQUIRED",
                "失效裁决必须携带非空操作标识 operation_id", status=400)
        result = store.invalidate(operation_id, record_id)
        return jsonify(result), 200

    @app.get("/api/operations/<operation_id>")
    def get_operation(operation_id: str):
        result = store.get_operation(operation_id)
        if result is None:
            raise StoreError("OPERATION_NOT_FOUND",
                             f"操作标识 {operation_id} 无记录", status=404,
                             details={"operation_id": operation_id})
        return jsonify(result)

    # ---------------- 错误处理 ---------------- #
    @app.errorhandler(StoreError)
    def handle_store_error(err: StoreError):
        body, status = err.to_response()
        return jsonify(body), status

    return app
