from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class Event(BaseModel):
    """The envelope every message on Kafka uses.

    Keeping the envelope stable lets every service apply the same consumer
    mechanics (deduplication, tracing, dead-lettering) even when payloads have
    completely different shapes.
    """

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: str = Field(min_length=1, max_length=120)
    event_version: int = Field(default=1, ge=1)
    occurred_at: datetime
    producer: str
    correlation_id: str
    causation_id: str | None = None
    partition_key: str
    payload: dict[str, Any]


def new_event(
    *,
    event_type: str,
    producer: str,
    payload: dict[str, Any],
    key: str,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    event_version: int = 1,
    occurred_at: datetime | None = None,
) -> Event:
    """Build an envelope.

    ``correlation_id`` should stay stable for the whole business transaction
    (we use the order id). ``causation_id`` points at the event that directly
    caused this one, which makes a trace of a saga easy to reconstruct.
    """

    return Event(
        event_id=uuid4(),
        event_type=event_type,
        event_version=event_version,
        occurred_at=occurred_at or datetime.now(UTC),
        producer=producer,
        correlation_id=correlation_id or key,
        causation_id=causation_id,
        partition_key=key,
        payload=payload,
    )
