from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from serving.db_clients import (
    close_connections,
    get_cassandra_session,
    get_mongo_collection,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    close_connections()


app = FastAPI(title="Subway Pulse API", lifespan=lifespan)


@app.get("/health")
def health():
    status: dict[str, Any] = {}
    try:
        get_mongo_collection("speed_layer").database.client.admin.command("ping")
        status["mongo"] = "ok"
    except Exception as exc:
        status["mongo"] = str(exc)

    try:
        get_cassandra_session().execute("SELECT release_version FROM system.local")
        status["cassandra"] = "ok"
    except Exception as exc:
        status["cassandra"] = str(exc)

    ok = all(v == "ok" for v in status.values())
    return JSONResponse(
        content={"status": "ok" if ok else "degraded", **status},
        status_code=200 if ok else 503,
    )
