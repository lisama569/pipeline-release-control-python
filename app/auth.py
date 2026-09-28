"""角色与权限模型。

动作权限矩阵：

| action             | viewer | operator | admin |
|--------------------|--------|----------|-------|
| read               |  ✓     |  ✓       |  ✓    |
| publish            |        |  ✓       |  ✓    |
| rollback           |        |  ✓       |  ✓    |
| cancel             |        |  ✓       |  ✓    |
| report_failure     |        |  ✓       |  ✓    |
| register_pipeline  |        |          |  ✓    |

越权统一抛 ``PERMISSION_DENIED``（HTTP 403），不泄露资源是否存在：
权限检查先于资源查找。
"""
from __future__ import annotations

from .errors import ErrorCode, ServiceError
from .models import Role

READ = "read"
PUBLISH = "publish"
ROLLBACK = "rollback"
CANCEL = "cancel"
REPORT_FAILURE = "report_failure"
REGISTER_PIPELINE = "register_pipeline"

ROLE_PERMISSIONS: dict[Role, frozenset[str]] = {
    Role.VIEWER: frozenset({READ}),
    Role.OPERATOR: frozenset({READ, PUBLISH, ROLLBACK, CANCEL, REPORT_FAILURE}),
    Role.ADMIN: frozenset({
        READ, PUBLISH, ROLLBACK, CANCEL, REPORT_FAILURE, REGISTER_PIPELINE,
    }),
}


class Principal:
    """调用方身份。"""

    __slots__ = ("username", "role")

    def __init__(self, username: str, role: Role) -> None:
        self.username = username
        self.role = role

    def __repr__(self) -> str:
        return f"Principal(username={self.username!r}, role={self.role.value!r})"


def require_permission(principal: Principal, action: str) -> None:
    """越权抛 PERMISSION_DENIED。"""
    if action not in ROLE_PERMISSIONS.get(principal.role, frozenset()):
        raise ServiceError(
            ErrorCode.PERMISSION_DENIED,
            f"role '{principal.role.value}' is not allowed to perform '{action}'",
        )


def principal_from_headers(username: str | None, role_value: str | None) -> Principal:
    """从请求头解析调用方（演示用的简易鉴权，见 README）。

    缺省身份视为 viewer；未知角色视为认证失败 -> PERMISSION_DENIED。
    """
    name = (username or "anonymous").strip() or "anonymous"
    try:
        role = Role(role_value or "viewer")
    except ValueError:
        raise ServiceError(
            ErrorCode.PERMISSION_DENIED,
            f"unknown role '{role_value}'",
        )
    return Principal(name, role)
