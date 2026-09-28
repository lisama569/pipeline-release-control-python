"""错误类型与可区分的失败原因码。"""
from __future__ import annotations

import enum


class ReasonCode(str, enum.Enum):
    PERMISSION_DENIED = "permission_denied"
    DUPLICATE_REQUEST = "duplicate_request"
    CONCURRENT_CONFLICT = "concurrent_conflict"
    VERSION_NOT_FOUND = "version_not_found"
    VERSION_INCOMPATIBLE = "version_incompatible"
    INVALID_STATE_TRANSITION = "invalid_state_transition"
    NOT_FOUND = "not_found"


class ReleaseError(Exception):
    def __init__(self, code: ReasonCode, message: str):
        super().__init__(message)
        self.code = code
        self.message = message
