from __future__ import annotations

import asyncio
import contextlib
import logging

import asyncpg
from aiokafka import AIOKafkaProducer

from edd_common.events import Event

logger = logging.getLogger(__name__)


async def enqueue_event(
    conn: asyncpg.Connection,
    topic: str,
    event: Event,
) -> None:
    """Write an event to the outbox inside the caller's transaction.

    The business write and the event record commit together. A separate relay
    publishes the outbox rows to Kafka, which is what makes "database updated
    but event lost" impossible. This is the transactional outbox pattern.
    """

    await conn.execute(
        """
        INSERT INTO outbox (id, topic, message_key, event_type, envelope)
        VALUES ($1, $2, $3, $4, $5)
        """,
        event.event_id,
        topic,
        event.partition_key,
        event.event_type,
        event.model_dump(mode="json"),
    )


class OutboxRelay:
    """Polls the outbox and publishes unpublished events to Kafka.

    Delivery is at-least-once: if Kafka accepts a message but the following
    database update fails, the message is published again. Consumers deduplicate
    by ``event_id`` in the inbox table, so duplicates are harmless.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        producer: AIOKafkaProducer,
        *,
        poll_interval_seconds: float,
        batch_size: int,
    ) -> None:
        self._pool = pool
        self._producer = producer
        self._poll_interval = poll_interval_seconds
        self._batch_size = batch_size
        self._stop = asyncio.Event()

    async def run(self) -> None:
        logger.info("Outbox relay started")
        while not self._stop.is_set():
            try:
                published = await self.publish_batch()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Outbox relay iteration failed; will retry")
                published = 0

            if published == 0:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=self._poll_interval,
                    )

        logger.info("Outbox relay stopped")

    async def publish_batch(self) -> int:
        async with self._pool.acquire() as conn, conn.transaction():
            rows = await conn.fetch(
                """
                    SELECT id, topic, message_key, event_type, envelope
                    FROM outbox
                    WHERE published_at IS NULL
                    ORDER BY created_at, id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                    """,
                self._batch_size,
            )

            for row in rows:
                envelope = row["envelope"]
                headers = [
                    ("event_id", str(row["id"]).encode()),
                    ("event_type", row["event_type"].encode()),
                    (
                        "correlation_id",
                        str(envelope.get("correlation_id", "")).encode(),
                    ),
                ]
                await self._producer.send_and_wait(
                    topic=row["topic"],
                    key=row["message_key"],
                    value=envelope,
                    headers=headers,
                )
                await conn.execute(
                    "UPDATE outbox SET published_at = now() WHERE id = $1",
                    row["id"],
                )

            if rows:
                logger.info("Published %d outbox event(s)", len(rows))
            return len(rows)

    def stop(self) -> None:
        self._stop.set()


async def wait_for_outbox_empty(
    pool: asyncpg.Pool,
    *,
    timeout_seconds: float = 10.0,
) -> bool:
    """Test/demo helper: wait until this service has no unpublished events."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while loop.time() < deadline:
        async with pool.acquire() as conn:
            pending = await conn.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL")
        if pending == 0:
            return True
        await asyncio.sleep(0.2)
    return False


async def outbox_stats(pool: asyncpg.Pool) -> dict[str, int]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                count(*) FILTER (WHERE published_at IS NULL) AS pending,
                count(*) FILTER (WHERE published_at IS NOT NULL) AS published
            FROM outbox
            """
        )
    return {"pending": int(row["pending"]), "published": int(row["published"])}


__all__ = [
    "OutboxRelay",
    "enqueue_event",
    "outbox_stats",
    "wait_for_outbox_empty",
]
