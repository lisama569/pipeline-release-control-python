import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from release_control.api import STATUS_BY_CODE, create_router
from release_control.errors import ReleaseError
from release_control.service import ReleaseService
from release_control.store import InMemoryStore

store = InMemoryStore()
service = ReleaseService(store)
app = FastAPI(title="Pipeline Release Control")
app.include_router(create_router(service))


@app.exception_handler(ReleaseError)
async def release_error_handler(request: Request, exc: ReleaseError):
    return JSONResponse(
        status_code=STATUS_BY_CODE[exc.code],
        content={"error": {"code": exc.code.value, "message": exc.message}},
    )


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
