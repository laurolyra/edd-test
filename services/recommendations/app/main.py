from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from app.api import router as recommendations_router
from app.config import RecommendationsSettings
from app.handlers import handle_event
from edd_common.api import install_error_handlers
from edd_common.runtime import ServiceRuntime

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
settings = RecommendationsSettings()

runtime = ServiceRuntime(
    settings=settings,
    migrations_dir=MIGRATIONS_DIR,
    topics=["orders.events"],
    handler=handle_event,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await runtime.start()
    app.state.runtime = runtime
    try:
        yield
    finally:
        await runtime.stop()


app = FastAPI(
    title="Recommendations Service",
    version="1.0.0",
    description=(
        "A read model built from completed orders. It demonstrates CQRS: the "
        "query side is populated asynchronously by events, never by HTTP calls."
    ),
    lifespan=lifespan,
)
install_error_handlers(app)
app.include_router(recommendations_router)
app.include_router(runtime.health_router())


@app.get("/", tags=["operations"], summary="Service metadata")
async def root() -> dict:
    return {
        "service": settings.service_name,
        "port": settings.http_port,
        "docs": "/docs",
        "consumes": runtime.topics,
        "produces": [],
    }
