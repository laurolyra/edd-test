from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord, OffsetAndMetadata, TopicPartition

from edd_common.config import BaseServiceSettings
from edd_common.events import Event
from edd_common.kafka import consumer_config

logger = logging.getLogger(__name__)

Handler = Callable[[asyncpg.Connection, Event], Awaitable[None]]


class EventConsumer:
    """A manual-commit, idempotent Kafka consumer.

    Processing a message means:

    1. start a Postgres transaction,
    2. insert the event id into ``processed_events`` (the inbox),
    3. run the handler in the same transaction,
    4. commit the offset only after the transaction commits.

    A crash at any point therefore leads to a redelivery, never to a lost
    event. Redeliveries are ignored by the inbox unique key.
    """

    def __init__(
        self,
        *,
        settings: BaseServiceSettings,
        topics: Sequence[str],
        handler: Handler,
        pool: asyncpg.Pool,
        producer: AIOKafkaProducer,
        dlq_topic: str,
    ) -> None:
        self._settings = settings
        self._topics = list(topics)
        self._handler = handler
        self._pool = pool
        self._producer = producer
        self._dlq_topic = dlq_topic
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(*self._topics, **consumer_config(self._settings))
        await self._consumer.start()
        logger.info(
            "Consumer started (group=%s, topics=%s)",
            self._settings.consumer_group,
            ", ".join(self._topics),
        )
        self._task = asyncio.create_task(
            self._run(), name=f"{self._settings.service_name}-consumer"
        )

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        logger.info("Consumer stopped")

    async def _run(self) -> None:
        assert self._consumer is not None
        while not self._stopping:
            try:
                # One record at a time. Offsets are committed per record, so a
                # failure can be rewound precisely without skipping the rest of
                # a prefetched batch.
                message = await self._consumer.getone()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Kafka poll failed; retrying in 2s")
                await asyncio.sleep(2)
                continue

            try:
                await self._process_message(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Usually a DLQ publish failure. The offset was not committed,
                # so rewind to this record and try again after a short pause.
                logger.exception(
                    "Message processing failed; rewinding %s-%s-%s",
                    message.topic,
                    message.partition,
                    message.offset,
                )
                self._consumer.seek(
                    TopicPartition(message.topic, message.partition),
                    message.offset,
                )
                await asyncio.sleep(2)

    async def _process_message(self, message: ConsumerRecord) -> None:
        raw = message.value
        try:
            event = Event.model_validate_json(raw)
        except Exception as exc:
            logger.exception(
                "Invalid event envelope at %s-%s-%s",
                message.topic,
                message.partition,
                message.offset,
            )
            await self._send_to_dlq(
                {
                    "consumer": self._settings.consumer_group,
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "error": f"{type(exc).__name__}: {exc}",
                    "raw": raw.decode("utf-8", errors="replace")
                    if isinstance(raw, bytes)
                    else str(raw),
                }
            )
            await self._commit(message)
            return

        try:
            await self._handle_with_retry(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "Event %s (%s) failed permanently; sending to %s",
                event.event_id,
                event.event_type,
                self._dlq_topic,
            )
            await self._send_to_dlq(
                {
                    "consumer": self._settings.consumer_group,
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "error": f"{type(exc).__name__}: {exc}",
                    "event": event.model_dump(mode="json"),
                }
            )
        await self._commit(message)

    async def _handle_with_retry(self, event: Event) -> None:
        last_error: Exception | None = None
        attempts = max(1, self._settings.consumer_max_retries)

        for attempt in range(1, attempts + 1):
            try:
                async with self._pool.acquire() as conn, conn.transaction():
                    claimed = await conn.fetchval(
                        """
                            INSERT INTO processed_events (event_id, consumer_group, event_type)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (event_id, consumer_group) DO NOTHING
                            RETURNING event_id
                            """,
                        event.event_id,
                        self._settings.consumer_group,
                        event.event_type,
                    )
                    if claimed is None:
                        logger.info(
                            "Skipping duplicate event %s (%s)",
                            event.event_id,
                            event.event_type,
                        )
                        return
                    await self._handler(conn, event)
                logger.info(
                    "Processed %s (event_id=%s, correlation_id=%s)",
                    event.event_type,
                    event.event_id,
                    event.correlation_id,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Handler failed for %s (event_id=%s, attempt %d/%d): %s",
                    event.event_type,
                    event.event_id,
                    attempt,
                    attempts,
                    exc,
                )
                if attempt < attempts:
                    await asyncio.sleep(min(0.25 * (2 ** (attempt - 1)), 2.0))

        assert last_error is not None
        raise last_error

    async def _send_to_dlq(self, payload: dict[str, Any]) -> None:
        await self._producer.send_and_wait(
            self._dlq_topic,
            key=payload.get("event", {}).get("partition_key", "unknown"),
            value=payload,
        )

    async def _commit(self, message: ConsumerRecord) -> None:
        assert self._consumer is not None
        topic_partition = TopicPartition(message.topic, message.partition)
        try:
            await self._consumer.commit(
                {topic_partition: OffsetAndMetadata(message.offset + 1, "")}
            )
        except Exception:
            # A failed commit only causes redelivery; the inbox makes it safe.
            logger.exception(
                "Could not commit offset %s-%s-%s",
                message.topic,
                message.partition,
                message.offset,
            )


__all__ = ["EventConsumer", "Handler"]
