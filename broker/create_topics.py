"""Create the Kafka topics required by the shop.

Usage:
    uv run python broker/create_topics.py
    KAFKA_BOOTSTRAP_SERVERS=kafka:29092 python broker/create_topics.py
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
from pathlib import Path

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from topics import TOPICS  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("create-topics")


async def wait_for_broker(admin: AIOKafkaAdminClient, attempts: int = 30) -> None:
    for attempt in range(1, attempts + 1):
        try:
            await admin.list_topics()
            return
        except Exception as exc:
            if attempt == attempts:
                raise
            logger.info("Kafka not ready (%d/%d): %s", attempt, attempts, exc)
            await asyncio.sleep(2)


async def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await wait_for_broker(admin)
        existing = set(await admin.list_topics())
        to_create = [
            NewTopic(
                name=spec.name,
                num_partitions=spec.partitions,
                replication_factor=1,
                topic_configs={
                    "retention.ms": str(spec.retention_ms),
                    "cleanup.policy": "delete",
                },
            )
            for spec in TOPICS
            if spec.name not in existing
        ]

        if not to_create:
            logger.info("All %d topics already exist", len(TOPICS))
            return

        # Another process may have won the race; that is not a problem.
        with contextlib.suppress(TopicAlreadyExistsError):
            await admin.create_topics(to_create)

        for spec in TOPICS:
            status = "created" if spec.name in {t.name for t in to_create} else "exists"
            logger.info(
                "%-24s %-7s partitions=%d retention=%dms  %s",
                spec.name,
                status,
                spec.partitions,
                spec.retention_ms,
                spec.description,
            )
    finally:
        await admin.close()


if __name__ == "__main__":
    asyncio.run(main())
