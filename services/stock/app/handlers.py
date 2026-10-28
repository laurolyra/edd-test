from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import uuid4

import asyncpg

from app.events import (
    ORDER_CANCELLED,
    ORDER_COMPLETED,
    ORDER_CREATED,
    STOCK_COMMITTED,
    STOCK_REJECTED,
    STOCK_RELEASED,
    STOCK_RESERVED,
    STOCK_TOPIC,
    OrderCancelledPayload,
    OrderCompletedPayload,
    OrderCreatedPayload,
    OrderItemPayload,
    StockCommittedPayload,
    StockRejectedPayload,
    StockReleasedPayload,
    StockReservedPayload,
)
from edd_common.events import Event, new_event
from edd_common.outbox import enqueue_event

logger = logging.getLogger(__name__)


async def handle_event(conn: asyncpg.Connection, event: Event) -> None:
    if event.event_type == ORDER_CREATED:
        await _on_order_created(conn, event)
    elif event.event_type == ORDER_CANCELLED:
        await _on_order_cancelled(conn, event)
    elif event.event_type == ORDER_COMPLETED:
        await _on_order_completed(conn, event)
    else:
        logger.debug("Ignoring event type %s", event.event_type)


def _aggregate(items: list[OrderItemPayload]) -> list[OrderItemPayload]:
    """Defensive aggregation: one line per product, even if a producer sent duplicates."""

    quantities: dict[str, int] = {}
    prices: dict[str, int] = {}
    order: list[str] = []
    for item in items:
        if item.product_id not in quantities:
            order.append(item.product_id)
            quantities[item.product_id] = 0
            prices[item.product_id] = item.unit_price_cents
        quantities[item.product_id] += item.quantity

    return [
        OrderItemPayload(
            product_id=product_id,
            quantity=quantities[product_id],
            unit_price_cents=prices[product_id],
        )
        for product_id in order
    ]


async def _on_order_created(conn: asyncpg.Connection, event: Event) -> None:
    payload = OrderCreatedPayload.model_validate(event.payload)
    existing = await conn.fetchval(
        "SELECT status FROM reservations WHERE order_id = $1",
        payload.order_id,
    )
    if existing is not None:
        logger.info(
            "Reservation for order %s already exists with status %s",
            payload.order_id,
            existing,
        )
        return

    items = _aggregate(payload.items)
    product_ids = [item.product_id for item in items]

    # Lock all requested rows in a stable order. This prevents two concurrent
    # orders from over-selling the same product and avoids lock-order deadlocks.
    rows = await conn.fetch(
        """
        SELECT product_id, available
        FROM stock_items
        WHERE product_id = ANY($1::text[])
        ORDER BY product_id
        FOR UPDATE
        """,
        product_ids,
    )
    available_by_product = {row["product_id"]: row["available"] for row in rows}

    unavailable = [
        {
            "product_id": item.product_id,
            "requested": item.quantity,
            "available": available_by_product.get(item.product_id, 0),
        }
        for item in items
        if available_by_product.get(item.product_id, 0) < item.quantity
    ]

    reservation_id = uuid4()
    now = datetime.now(UTC)
    item_dicts = [item.model_dump() for item in items]

    if unavailable:
        reason = "Insufficient stock: " + ", ".join(
            (
                f"{entry['product_id']} "
                f"(requested {entry['requested']}, available {entry['available']})"
            )
            for entry in unavailable
        )
        await conn.execute(
            """
            INSERT INTO reservations (id, order_id, status, items, reason, created_at, updated_at)
            VALUES ($1, $2, 'REJECTED', $3, $4, $5, $5)
            """,
            reservation_id,
            payload.order_id,
            item_dicts,
            reason,
            now,
        )
        rejected = StockRejectedPayload(
            order_id=payload.order_id,
            reason=reason,
            unavailable_items=unavailable,
            rejected_at=now,
        )
        outbound = new_event(
            event_type=STOCK_REJECTED,
            producer="stock",
            key=str(payload.order_id),
            correlation_id=str(payload.order_id),
            causation_id=str(event.event_id),
            payload=rejected.model_dump(mode="json"),
        )
        await enqueue_event(conn, STOCK_TOPIC, outbound)
        logger.info("Rejected order %s: %s", payload.order_id, reason)
        return

    for item in items:
        await conn.execute(
            """
            UPDATE stock_items
            SET available = available - $2,
                reserved = reserved + $2,
                updated_at = now()
            WHERE product_id = $1
            """,
            item.product_id,
            item.quantity,
        )

    await conn.execute(
        """
        INSERT INTO reservations (id, order_id, status, items, created_at, updated_at)
        VALUES ($1, $2, 'RESERVED', $3, $4, $4)
        """,
        reservation_id,
        payload.order_id,
        item_dicts,
        now,
    )

    reserved = StockReservedPayload(
        order_id=payload.order_id,
        reservation_id=reservation_id,
        customer_id=payload.customer_id,
        items=items,
        total_cents=payload.total_cents,
        currency=payload.currency,
        reserved_at=now,
    )
    outbound = new_event(
        event_type=STOCK_RESERVED,
        producer="stock",
        key=str(payload.order_id),
        correlation_id=str(payload.order_id),
        causation_id=str(event.event_id),
        payload=reserved.model_dump(mode="json"),
    )
    await enqueue_event(conn, STOCK_TOPIC, outbound)
    logger.info("Reserved stock for order %s", payload.order_id)


async def _on_order_cancelled(conn: asyncpg.Connection, event: Event) -> None:
    payload = OrderCancelledPayload.model_validate(event.payload)
    row = await conn.fetchrow(
        """
        SELECT id, status, items
        FROM reservations
        WHERE order_id = $1
        FOR UPDATE
        """,
        payload.order_id,
    )
    if row is None:
        logger.info("No reservation to release for order %s", payload.order_id)
        return
    if row["status"] != "RESERVED":
        logger.info(
            "Reservation %s for order %s is %s; nothing to release",
            row["id"],
            payload.order_id,
            row["status"],
        )
        return

    items = [OrderItemPayload.model_validate(item) for item in row["items"]]
    for item in items:
        await conn.execute(
            """
            UPDATE stock_items
            SET available = available + $2,
                reserved = GREATEST(reserved - $2, 0),
                updated_at = now()
            WHERE product_id = $1
            """,
            item.product_id,
            item.quantity,
        )

    now = datetime.now(UTC)
    await conn.execute(
        """
        UPDATE reservations
        SET status = 'RELEASED', reason = $2, updated_at = $3
        WHERE id = $1
        """,
        row["id"],
        payload.reason,
        now,
    )
    released = StockReleasedPayload(
        order_id=payload.order_id,
        reservation_id=row["id"],
        items=items,
        released_at=now,
    )
    outbound = new_event(
        event_type=STOCK_RELEASED,
        producer="stock",
        key=str(payload.order_id),
        correlation_id=str(payload.order_id),
        causation_id=str(event.event_id),
        payload=released.model_dump(mode="json"),
    )
    await enqueue_event(conn, STOCK_TOPIC, outbound)
    logger.info("Released stock for cancelled order %s", payload.order_id)


async def _on_order_completed(conn: asyncpg.Connection, event: Event) -> None:
    payload = OrderCompletedPayload.model_validate(event.payload)
    row = await conn.fetchrow(
        """
        SELECT id, status, items
        FROM reservations
        WHERE order_id = $1
        FOR UPDATE
        """,
        payload.order_id,
    )
    if row is None:
        logger.warning("No reservation found for completed order %s", payload.order_id)
        return
    if row["status"] != "RESERVED":
        logger.info(
            "Reservation %s for order %s is %s; nothing to commit",
            row["id"],
            payload.order_id,
            row["status"],
        )
        return

    items = [OrderItemPayload.model_validate(item) for item in row["items"]]
    for item in items:
        # available was already reduced when the stock was reserved.
        await conn.execute(
            """
            UPDATE stock_items
            SET reserved = GREATEST(reserved - $2, 0),
                updated_at = now()
            WHERE product_id = $1
            """,
            item.product_id,
            item.quantity,
        )

    now = datetime.now(UTC)
    await conn.execute(
        """
        UPDATE reservations
        SET status = 'COMMITTED', updated_at = $2
        WHERE id = $1
        """,
        row["id"],
        now,
    )
    committed = StockCommittedPayload(
        order_id=payload.order_id,
        reservation_id=row["id"],
        committed_at=now,
    )
    outbound = new_event(
        event_type=STOCK_COMMITTED,
        producer="stock",
        key=str(payload.order_id),
        correlation_id=str(payload.order_id),
        causation_id=str(event.event_id),
        payload=committed.model_dump(mode="json"),
    )
    await enqueue_event(conn, STOCK_TOPIC, outbound)
    logger.info("Committed stock for completed order %s", payload.order_id)
