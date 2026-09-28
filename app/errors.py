"""可区分错误码的异常。

每类失败都有独立、稳定的 ``code``，调用方据此区分：
- 重复提交命中不同请求体     -> IDEMPOTENCY_CONFLICT
- 同流水线并发互相冲突       -> CONCURRENT_MODIFICATION
- 越权                       -> PERMISSION_DENIED
- 回退版本不存在 / 未发布过 / 不兼容 -> VERSION_*
- 当前状态不允许该操作       -> RELEASE_NOT_CANCELLABLE 等
"""
from __future__ import annotations


class ErrorCode:
    VALIDATION_ERROR = "VALIDATION_ERROR"
    NOT_FOUND = "NOT_FOUND"
    RELEASE_NOT_FOUND = "RELEASE_NOT_FOUND"
    VERSION_NOT_FOUND = "VERSION_NOT_FOUND"
    VERSION_NOT_PUBLISHED = "VERSION_NOT_PUBLISHED"
    VERSION_INCOMPATIBLE = "VERSION_INCOMPATIBLE"
    RELEASE_NOT_CANCELLABLE = "RELEASE_NOT_CANCELLABLE"
    RELEASE_NOT_TERMINAL = "RELEASE_NOT_TERMINAL"
    INVALID_STATUS_TRANSITION = "INVALID_STATUS_TRANSITION"
    RELEASE_FAILED = "RELEASE_FAILED"
    VERSION_ALREADY_ACTIVE = "VERSION_ALREADY_ACTIVE"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    CONCURRENT_MODIFICATION = "CONCURRENT_MODIFICATION"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    INTERNAL_ERROR = "INTERNAL_ERROR"


# 错误码 -> HTTP 状态码
HTTP_STATUS: dict[str, int] = {
    ErrorCode.VALIDATION_ERROR: 400,
    ErrorCode.NOT_FOUND: 404,
    ErrorCode.RELEASE_NOT_FOUND: 404,
    ErrorCode.VERSION_NOT_FOUND: 404,
    ErrorCode.VERSION_NOT_PUBLISHED: 409,
    ErrorCode.VERSION_INCOMPATIBLE: 422,
    ErrorCode.RELEASE_NOT_CANCELLABLE: 409,
    ErrorCode.RELEASE_NOT_TERMINAL: 409,
    ErrorCode.INVALID_STATUS_TRANSITION: 409,
    ErrorCode.RELEASE_FAILED: 409,
    ErrorCode.VERSION_ALREADY_ACTIVE: 409,
    ErrorCode.PERMISSION_DENIED: 403,
    ErrorCode.CONCURRENT_MODIFICATION: 409,
    ErrorCode.IDEMPOTENCY_CONFLICT: 409,
    ErrorCode.INTERNAL_ERROR: 500,
}


class ServiceError(Exception):
    """业务错误。

    conflict_with 记录挡下当前请求的那条记录（release_id / 幂等键），
    便于调用方定位冲突来源。
    """

    def __init__(self, code: str, message: str, *, conflict_with: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.conflict_with = conflict_with

    @property
    def http_status(self) -> int:
        return HTTP_STATUS.get(self.code, 500)

    def to_dict(self) -> dict[str, str]:
        body = {"code": self.code, "message": self.message}
        if self.conflict_with:
            body["conflict_with"] = self.conflict_with
        return body
