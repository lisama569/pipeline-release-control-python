"""FastAPI 路由层：解析调用方身份与请求标识，把领域错误映射为可区分的 HTTP 响应。"""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Header, Request
from pydantic import BaseModel

from .errors import ReasonCode, ReleaseError
from .models import Build, Caller, Environment, Role
from .service import ReleaseService

STATUS_BY_CODE = {
    ReasonCode.PERMISSION_DENIED: 403,
    ReasonCode.DUPLICATE_REQUEST: 409,
    ReasonCode.CONCURRENT_CONFLICT: 409,
    ReasonCode.INVALID_STATE_TRANSITION: 409,
    ReasonCode.NOT_FOUND: 404,
    ReasonCode.VERSION_NOT_FOUND: 404,
    ReasonCode.VERSION_INCOMPATIBLE: 422,
}


class BuildIn(BaseModel):
    build_id: str
    artifact_uri: str
    config: dict = {}
    schema_version: int = 1


class PublishIn(BaseModel):
    environment: Environment
    build: BuildIn


class RollbackIn(BaseModel):
    environment: Environment
    target_release_id: str


def _release_view(record) -> dict:
    return {
        "release_id": record.release_id,
        "pipeline_id": record.pipeline_id,
        "environment": record.environment.value,
        "status": record.status.value,
        "build_id": record.build.build_id,
        "artifact_uri": record.build.artifact_uri,
        "config": record.build.config,
        "idempotency_key": record.idempotency_key,
        "created_by": record.created_by,
    }


def create_router(service: ReleaseService) -> APIRouter:
    router = APIRouter()

    def resolve_caller(request: Request) -> Caller:
        return Caller(
            caller_id=request.headers["x-caller-id"],
            role=Role(request.headers["x-role"]),
        )

    def resolve_request_id(request: Request) -> str:
        return request.headers.get("x-request-id") or uuid.uuid4().hex

    @router.post("/pipelines/{pipeline_id}/releases", status_code=201)
    def publish(pipeline_id: str, body: PublishIn, request: Request,
                idempotency_key: str = Header(...)):
        caller = resolve_caller(request)
        record = service.publish(
            caller=caller,
            pipeline_id=pipeline_id,
            env=body.environment,
            build=Build(
                build_id=body.build.build_id,
                pipeline_id=pipeline_id,
                artifact_uri=body.build.artifact_uri,
                config=body.build.config,
                schema_version=body.build.schema_version,
            ),
            idempotency_key=idempotency_key,
            request_id=resolve_request_id(request),
        )
        return _release_view(record)

    @router.post("/pipelines/{pipeline_id}/rollbacks", status_code=201)
    def rollback(pipeline_id: str, body: RollbackIn, request: Request,
                 idempotency_key: str = Header(...)):
        caller = resolve_caller(request)
        record = service.rollback(
            caller=caller,
            pipeline_id=pipeline_id,
            env=body.environment,
            target_release_id=body.target_release_id,
            idempotency_key=idempotency_key,
            request_id=resolve_request_id(request),
        )
        return _release_view(record)

    @router.post("/releases/{release_id}/cancel")
    def cancel(release_id: str, request: Request):
        caller = resolve_caller(request)
        return _release_view(service.cancel(caller, release_id, resolve_request_id(request)))

    @router.get("/releases/{release_id}")
    def get_release(release_id: str, request: Request):
        caller = resolve_caller(request)
        return _release_view(service.get_release(caller, release_id))

    return router
