"""FastAPI 路由层。

约定的请求头：
- ``X-Username`` / ``X-Role``：调用方身份（演示用简易鉴权）。
- ``X-Request-Id``：请求标识；缺省自动生成，响应头原样返回，日志/审计携带。
- ``Idempotency-Key``：发布/回滚的幂等键；重试务必复用同一键。

所有业务错误统一为 ``{code,message,conflict_with?}``，HTTP 状态码
见 errors.HTTP_STATUS，调用方可用稳定的 code 区分失败原因。
"""
from __future__ import annotations

import logging
import uuid

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .auth import principal_from_headers
from .errors import ServiceError
from .models import Artifact, Environment
from .service import ReleaseService

logger = logging.getLogger("release_control.api")


# ----------------------------- 请求体 -------------------------------
class RegisterBody(BaseModel):
    config_generation: str = Field(min_length=1)


class ConfigGenerationBody(BaseModel):
    generation: str = Field(min_length=1)


class ArtifactBody(BaseModel):
    artifact_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    checksum: str = Field(min_length=1)
    config_snapshot: dict = Field(default_factory=dict)
    compatible_generations: list[str] = Field(default_factory=list)


class PublishBody(BaseModel):
    artifact: ArtifactBody


class RollbackBody(BaseModel):
    target_version: str = Field(min_length=1)


class FailureBody(BaseModel):
    reason: str = Field(min_length=1)


# ----------------------------- 应用 ---------------------------------
def create_app(service: ReleaseService | None = None) -> FastAPI:
    # 库默认不配置 handler；若调用方未配置日志，这里给出可读的本地默认格式。
    if not logging.getLogger("release_control").handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s"))
        root = logging.getLogger("release_control")
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        root.propagate = False

    app = FastAPI(title="pipeline-release-control", version="0.1.0")
    svc = service or ReleaseService()
    app.state.service = svc

    @app.exception_handler(ServiceError)
    async def service_error_handler(request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request_id = request.headers.get("X-Request-Id") or f"req-{uuid.uuid4().hex[:12]}"
        request.state.request_id = request_id
        logger.info(
            "request_start request_id=%s method=%s path=%s role=%s user=%s "
            "idempotency_key=%s",
            request_id, request.method, request.url.path,
            request.headers.get("X-Role", "viewer"),
            request.headers.get("X-Username", "anonymous"),
            request.headers.get("Idempotency-Key", "-"),
        )
        try:
            response = await call_next(request)
        except Exception:
            logger.exception("request_crash request_id=%s", request_id)
            raise
        logger.info("request_end request_id=%s status=%s",
                    request_id, response.status_code)
        response.headers["X-Request-Id"] = request_id
        return response

    def to_artifact(body: ArtifactBody) -> Artifact:
        return Artifact(
            artifact_id=body.artifact_id,
            version=body.version,
            checksum=body.checksum,
            config_snapshot=body.config_snapshot,
            compatible_generations=tuple(body.compatible_generations),
        )

    @app.post("/pipelines/{pipeline_id}/register")
    def register(request: Request, pipeline_id: str, body: RegisterBody,
                 x_username: str | None = Header(default=None),
                 x_role: str | None = Header(default=None)):
        p = principal_from_headers(x_username, x_role)
        svc.register_pipeline(p, pipeline_id, body.config_generation,
                              request_id=request.state.request_id)
        return {"pipeline_id": pipeline_id,
                "config_generation": body.config_generation}

    @app.post("/pipelines/{pipeline_id}/config-generation")
    def set_generation(request: Request, pipeline_id: str, body: ConfigGenerationBody,
                       x_username: str | None = Header(default=None),
                       x_role: str | None = Header(default=None)):
        p = principal_from_headers(x_username, x_role)
        svc.set_config_generation(p, pipeline_id, body.generation,
                                  request_id=request.state.request_id)
        return {"pipeline_id": pipeline_id, "config_generation": body.generation}

    @app.post("/pipelines/{pipeline_id}/environments/{environment}/releases")
    def publish(request: Request, pipeline_id: str, environment: Environment,
                body: PublishBody,
                x_username: str | None = Header(default=None),
                x_role: str | None = Header(default=None),
                idempotency_key: str | None = Header(default=None,
                                                      alias="Idempotency-Key")):
        p = principal_from_headers(x_username, x_role)
        release = svc.publish(
            p, pipeline_id, environment.value, to_artifact(body.artifact),
            idempotency_key=idempotency_key or "",
            request_id=request.state.request_id,
        )
        return release.to_dict()

    @app.post("/pipelines/{pipeline_id}/environments/{environment}/rollbacks")
    def rollback(request: Request, pipeline_id: str, environment: Environment,
                 body: RollbackBody,
                 x_username: str | None = Header(default=None),
                 x_role: str | None = Header(default=None),
                 idempotency_key: str | None = Header(default=None,
                                                       alias="Idempotency-Key")):
        p = principal_from_headers(x_username, x_role)
        release = svc.rollback(
            p, pipeline_id, environment.value, body.target_version,
            idempotency_key=idempotency_key or "",
            request_id=request.state.request_id,
        )
        return release.to_dict()

    @app.post("/pipelines/{pipeline_id}/releases/{release_id}/cancel")
    def cancel(request: Request, pipeline_id: str, release_id: str,
               x_username: str | None = Header(default=None),
               x_role: str | None = Header(default=None)):
        p = principal_from_headers(x_username, x_role)
        release = svc.cancel(p, pipeline_id, "", release_id,
                             request_id=request.state.request_id)
        return release.to_dict()

    @app.post("/pipelines/{pipeline_id}/releases/{release_id}/failures")
    def report_failure(request: Request, pipeline_id: str, release_id: str,
                       body: FailureBody,
                       x_username: str | None = Header(default=None),
                       x_role: str | None = Header(default=None)):
        p = principal_from_headers(x_username, x_role)
        release = svc.mark_failed(p, pipeline_id, "", release_id, body.reason,
                                  request_id=request.state.request_id)
        return release.to_dict()

    @app.get("/pipelines/{pipeline_id}/environments/{environment}/current")
    def current(pipeline_id: str, environment: Environment,
                x_username: str | None = Header(default=None),
                x_role: str | None = Header(default=None)):
        p = principal_from_headers(x_username, x_role)
        return svc.get_current(p, pipeline_id, environment.value).to_dict()

    @app.get("/pipelines/{pipeline_id}/releases")
    def history(pipeline_id: str,
                x_username: str | None = Header(default=None),
                x_role: str | None = Header(default=None)):
        p = principal_from_headers(x_username, x_role)
        return {"releases": [r.to_dict() for r in svc.history(p, pipeline_id)]}

    @app.get("/pipelines/{pipeline_id}/audits")
    def audits(pipeline_id: str,
               x_username: str | None = Header(default=None),
               x_role: str | None = Header(default=None)):
        p = principal_from_headers(x_username, x_role)
        return {"audits": [a.to_dict() for a in svc.audits(p, pipeline_id)]}

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    return app
