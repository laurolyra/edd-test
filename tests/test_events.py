from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from edd_common.events import Event, new_event


def test_event_envelope_round_trips_through_json() -> None:
    event = new_event(
        event_type="order.created",
        producer="orders",
        key="order-1",
        correlation_id="order-1",
        payload={"order_id": "order-1", "total_cents": 4200},
    )

    restored = Event.model_validate_json(event.model_dump_json())

    assert restored == event
    assert restored.event_id.version == 4
    assert restored.payload["total_cents"] == 4200
    assert restored.occurred_at.tzinfo is not None


def test_event_defaults_correlation_id_to_partition_key() -> None:
    event = new_event(
        event_type="stock.reserved",
        producer="stock",
        key="order-9",
        payload={},
    )
    assert event.correlation_id == "order-9"
    assert event.causation_id is None
    assert event.event_version == 1


def test_event_rejects_unknown_envelope_fields() -> None:
    with pytest.raises(ValidationError):
        Event.model_validate(
            {
                "event_id": str(uuid4()),
                "event_type": "order.created",
                "event_version": 1,
                "occurred_at": datetime.now(UTC).isoformat(),
                "producer": "orders",
                "correlation_id": "order-1",
                "partition_key": "order-1",
                "payload": {},
                "unexpected": "field",
            }
        )
