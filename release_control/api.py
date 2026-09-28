"""FastAPI 路由层。"""
from __future__ import annotations

from fastapi import APIRouter

from .service import ReleaseService


def create_router(service: ReleaseService) -> APIRouter:
    router = APIRouter()
    # 端点在后续步骤填充
    return router
