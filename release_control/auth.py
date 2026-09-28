"""角色权限模型。"""
from __future__ import annotations

from .errors import ReasonCode, ReleaseError
from .models import Caller, Role

# 动作 -> 允许的角色集合
PERMISSIONS: dict[str, frozenset[Role]] = {
    "read": frozenset({Role.VIEWER, Role.OPERATOR, Role.ADMIN}),
    "publish": frozenset({Role.OPERATOR, Role.ADMIN}),
    "rollback": frozenset({Role.ADMIN}),
    "cancel": frozenset({Role.OPERATOR, Role.ADMIN}),
}


def require(caller: Caller, action: str) -> None:
    """校验调用方是否有权执行动作，越权直接拒绝。"""
    allowed = PERMISSIONS.get(action, frozenset())
    if caller.role not in allowed:
        raise ReleaseError(
            ReasonCode.PERMISSION_DENIED,
            f"caller={caller.caller_id} role={caller.role.value} 无权执行 {action}",
        )
