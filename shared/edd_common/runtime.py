from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from aiokafka import AIOKafkaProducer
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from edd_common.api import DomainError
from edd_common.config import BaseServiceSettings
from edd_common.consumer import EventConsumer, Handler
from edd_common.db import create_pool
from edd_common.kafka import create_producer
from edd_common.logging import configure_logging
from edd_common.migrations import apply_migrations, ensure_platform_tables
from edd_common.outbox import OutboxRelay

if TYPE_CHECKING:
    import asyncpg

logger = logging.getLogger(__name__)


class ServiceRuntime:
    """Wires Postgres, the outbox relay and a Kafka consumer for one service.

    Every service does exactly the same lifecycle, so it lives here instead of
    being copied five times.
    """

    def __init__(
        self,
        *,
        settings: BaseServiceSettings,
        migrations_dir: Path,
        topics: list[str],
        handler: Handler,
    ) -> None:
        self.settings = settings
        self.migrations_dir = migrations_dir
        self.topics = topics
        self.handler = handler
        self.pool: asyncpg.Pool | None = None
        self.producer: AIOKafkaProducer | None = None
        self.consumer: EventConsumer | None = None
        self.relay: OutboxRelay | None = None
        self._relay_task: asyncio.Task[None] | None = None

    @property
    def dlq_topic(self) -> str:
        return f"{self.settings.service_name}.dlq"

    async def start(self) -> None:
        configure_logging(self.settings)
        logger.info("Starting %s", self.settings.service_name)

        self.pool = await self._start_pool_with_retry()
        await ensure_platform_tables(self.pool)
        await apply_migrations(self.pool, self.migrations_dir)

        self.producer = await self._start_producer_with_retry()
        self.relay = OutboxRelay(
            self.pool,
            self.producer,
            poll_interval_seconds=self.settings.outbox_poll_interval_seconds,
            batch_size=self.settings.outbox_batch_size,
        )
        self._relay_task = asyncio.create_task(
            self.relay.run(),
            name=f"{self.settings.service_name}-outbox-relay",
        )

        self.consumer = EventConsumer(
            settings=self.settings,
            topics=self.topics,
            handler=self.handler,
            pool=self.pool,
            producer=self.producer,
            dlq_topic=self.dlq_topic,
        )
        await self._start_consumer_with_retry()
        logger.info("%s is ready", self.settings.service_name)

    async def _start_pool_with_retry(self) -> asyncpg.Pool:
        for attempt in range(1, 31):
            try:
                return await create_pool(self.settings.database_url)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Postgres not ready (attempt %d/30): %s",
                    attempt,
                    exc,
                )
                await asyncio.sleep(1)
        raise RuntimeError("Could not connect to Postgres after 30 attempts")

    async def _start_producer_with_retry(self) -> AIOKafkaProducer:
        assert self.pool is not None
        for attempt in range(1, 31):
            try:
                return await create_producer(self.settings)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Kafka producer not ready (attempt %d/30): %s",
                    attempt,
                    exc,
                )
                await asyncio.sleep(1)
        raise RuntimeError("Could not connect the Kafka producer after 30 attempts")

    async def _start_consumer_with_retry(self) -> None:
        assert self.consumer is not None
        for attempt in range(1, 31):
            try:
                await self.consumer.start()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Kafka consumer not ready (attempt %d/30): %s",
                    attempt,
                    exc,
                )
                await asyncio.sleep(1)
        raise RuntimeError("Could not start the Kafka consumer after 30 attempts")

    async def stop(self) -> None:
        logger.info("Stopping %s", self.settings.service_name)
        if self.consumer is not None:
            await self.consumer.stop()
            self.consumer = None
        if self.relay is not None:
            self.relay.stop()
            self.relay = None
        if self._relay_task is not None:
            self._relay_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._relay_task
            self._relay_task = None
        if self.producer is not None:
            await self.producer.stop()
            self.producer = None
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
        logger.info("%s stopped", self.settings.service_name)

    def health_router(self) -> APIRouter:
        router = APIRouter(tags=["operations"])

        @router.get("/health", summary="Liveness and database readiness")
        async def health() -> JSONResponse:
            if self.pool is None:
                return JSONResponse(
                    status_code=503,
                    content={"status": "starting", "service": self.settings.service_name},
                )
            try:
                await self.pool.fetchval("SELECT 1")
            except Exception:
                logger.exception("Health check failed")
                return JSONResponse(
                    status_code=503,
                    content={"status": "unhealthy", "service": self.settings.service_name},
                )
            return JSONResponse(
                content={
                    "status": "ok",
                    "service": self.settings.service_name,
                    "consumer_group": self.settings.consumer_group,
                    "topics": self.topics,
                }
            )

        return router

    def assert_ready(self) -> asyncpg.Pool:
        if self.pool is None:
            raise DomainError("Service is not ready", status_code=503)
        return self.pool
