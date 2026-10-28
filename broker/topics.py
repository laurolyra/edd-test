"""Declarative Kafka topic definitions.

Keeping this in Python (rather than relying on auto-creation) means every
environment gets the same partition count, which is what determines the
maximum consumer parallelism and the ordering domain.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_ms: int
    description: str


WEEK = 7 * 24 * 60 * 60 * 1000
MONTH = 30 * 24 * 60 * 60 * 1000

TOPICS: tuple[TopicSpec, ...] = (
    TopicSpec(
        "orders.events",
        6,
        WEEK,
        "Order lifecycle: created, cancelled, completed. Key = order_id.",
    ),
    TopicSpec(
        "stock.events",
        6,
        WEEK,
        "Reservation results: reserved, rejected, released, committed. Key = order_id.",
    ),
    TopicSpec(
        "invoices.events",
        6,
        WEEK,
        "Invoice lifecycle: issued, voided. Key = order_id.",
    ),
    TopicSpec(
        "fiscal.events",
        6,
        WEEK,
        "Fiscal document lifecycle: issued, cancelled. Key = order_id.",
    ),
    TopicSpec("orders.dlq", 1, MONTH, "Orders consumer poison messages."),
    TopicSpec("stock.dlq", 1, MONTH, "Stock consumer poison messages."),
    TopicSpec("invoices.dlq", 1, MONTH, "Invoices consumer poison messages."),
    TopicSpec("fiscal.dlq", 1, MONTH, "Fiscal consumer poison messages."),
    TopicSpec(
        "recommendations.dlq",
        1,
        MONTH,
        "Recommendations consumer poison messages.",
    ),
)
