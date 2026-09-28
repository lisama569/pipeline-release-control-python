import uvicorn
from fastapi import FastAPI

from release_control.api import create_router
from release_control.service import ReleaseService
from release_control.store import InMemoryStore

store = InMemoryStore()
service = ReleaseService(store)
app = FastAPI(title="Pipeline Release Control")
app.include_router(create_router(service))


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
