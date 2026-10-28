from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "broker"))

from topics import TOPICS  # noqa: E402


def test_topic_names_are_unique() -> None:
    names = [spec.name for spec in TOPICS]
    assert len(names) == len(set(names))


def test_all_event_topics_have_multiple_partitions() -> None:
    event_topics = [spec for spec in TOPICS if spec.name.endswith(".events")]
    assert len(event_topics) == 4
    assert all(spec.partitions >= 3 for spec in event_topics)


def test_every_service_has_a_dlq() -> None:
    services = {"orders", "stock", "invoices", "fiscal", "recommendations"}
    dlqs = {spec.name.removesuffix(".dlq") for spec in TOPICS if spec.name.endswith(".dlq")}
    assert services == dlqs
