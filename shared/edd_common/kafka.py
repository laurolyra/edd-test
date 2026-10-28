from __future__ import annotations

import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.structs import TopicPartition

from edd_common.config import BaseServiceSettings

logger = logging.getLogger(__name__)


def _json_serializer(value: Any) -> bytes:
    return json.dumps(value, default=str, separators=(",", ":")).encode("utf-8")


def _key_serializer(value: str | bytes | None) -> bytes | None:
    if value is None:
        return None
    return value.encode("utf-8") if isinstance(value, str) else value


def producer_config(settings: BaseServiceSettings) -> dict[str, Any]:
    return {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "client_id": f"{settings.service_name}-producer",
        "acks": "all",
        "enable_idempotence": True,
        "compression_type": "gzip",
        "linger_ms": 5,
        "max_batch_size": 16_384,
        "value_serializer": _json_serializer,
        "key_serializer": _key_serializer,
    }


def consumer_config(settings: BaseServiceSettings) -> dict[str, Any]:
    return {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "client_id": f"{settings.service_name}-consumer",
        "group_id": settings.consumer_group,
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest",
        "session_timeout_ms": 30_000,
        "heartbeat_interval_ms": 3_000,
        "max_poll_interval_ms": 300_000,
    }


async def create_producer(settings: BaseServiceSettings) -> AIOKafkaProducer:
    producer = AIOKafkaProducer(**producer_config(settings))
    await producer.start()
    return producer


__all__ = [
    "TopicPartition",
    "consumer_config",
    "create_producer",
    "logger",
    "producer_config",
]
