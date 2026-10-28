from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from app.api import router as orders_router
from app.config import OrdersSettings
from app.handlers import handle_event
from edd_common.api import install_error_handlers
from edd_common.runtime import ServiceRuntime

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
settings = OrdersSettings()

runtime = ServiceRuntime(
    settings=settings,
    migrations_dir=MIGRATIONS_DIR,
    topics=["stock.events", "invoices.events", "fiscal.events"],
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
    title="Orders Service",
    version="1.0.0",
    description=(
        "Owns the order aggregate. It never calls other services over HTTP: "
        "commands become events in the outbox, and reactions arrive as events."
    ),
    lifespan=lifespan,
)
install_error_handlers(app)
app.include_router(orders_router)
app.include_router(runtime.health_router())


@app.get("/", tags=["operations"], summary="Service metadata")
async def root() -> dict:
    return {
        "service": settings.service_name,
        "port": settings.http_port,
        "docs": "/docs",
        "consumes": runtime.topics,
        "produces": ["orders.events"],
    }
