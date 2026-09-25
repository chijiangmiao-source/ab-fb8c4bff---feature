"""业务错误：携带错误码与可定位信息，供接口层统一序列化。"""

from __future__ import annotations

from typing import Any


class StoreError(Exception):
    """业务校验错误，携带可定位信息。"""

    def __init__(self, code: str, message: str, status: int = 422,
                 details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details or {}

    def to_response(self) -> tuple[dict[str, Any], int]:
        return {"error": {"code": self.code, "message": self.message,
                          "details": self.details}}, self.status
