from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import UUID

import asyncpg

from app.events import (
    FISCAL_CANCELLED,
    FISCAL_ISSUED,
    INVOICE_ISSUED,
    INVOICE_VOIDED,
    ORDER_CANCELLED,
    ORDER_COMPLETED,
    ORDER_TOPIC,
    STOCK_REJECTED,
    STOCK_RESERVED,
    FiscalIssuedPayload,
    InvoiceIssuedPayload,
    OrderCancelledPayload,
    OrderCompletedPayload,
    StockRejectedPayload,
    StockReservedPayload,
    item_payloads,
)
from edd_common.events import Event, new_event
from edd_common.outbox import enqueue_event

logger = logging.getLogger(__name__)


async def handle_event(conn: asyncpg.Connection, event: Event) -> None:
    """Route consumed events to the order saga.

    Unknown event types are ignored on purpose. A service sharing a topic with
    other event types must not fail just because it does not care about them.
    """

    if event.event_type == STOCK_RESERVED:
        await _on_stock_reserved(conn, event)
    elif event.event_type == STOCK_REJECTED:
        await _on_stock_rejected(conn, event)
    elif event.event_type == INVOICE_ISSUED:
        await _on_invoice_issued(conn, event)
    elif event.event_type == FISCAL_ISSUED:
        await _on_fiscal_issued(conn, event)
    elif event.event_type == INVOICE_VOIDED:
        logger.info("Invoice voided for order %s", _order_id(event))
    elif event.event_type == FISCAL_CANCELLED:
        logger.info("Fiscal document cancelled for order %s", _order_id(event))
    else:
        logger.debug("Ignoring event type %s", event.event_type)


def _order_id(event: Event) -> UUID:
    return UUID(str(event.payload["order_id"]))


async def _on_stock_reserved(conn: asyncpg.Connection, event: Event) -> None:
    payload = StockReservedPayload.model_validate(event.payload)
    await conn.execute(
        """
        UPDATE orders
        SET stock_reserved_at = COALESCE(stock_reserved_at, $2),
            status = CASE WHEN status = 'PENDING' THEN 'STOCK_RESERVED' ELSE status END,
            updated_at = now()
        WHERE id = $1
        """,
        payload.order_id,
        payload.reserved_at,
    )
    await _try_complete(conn, payload.order_id, event)


async def _on_stock_rejected(conn: asyncpg.Connection, event: Event) -> None:
    payload = StockRejectedPayload.model_validate(event.payload)
    cancelled_at = datetime.now(UTC)

    row = await conn.fetchrow(
        """
        UPDATE orders
        SET status = 'CANCELLED',
            cancellation_reason = $2,
            cancelled_at = COALESCE(cancelled_at, $3),
            updated_at = now()
        WHERE id = $1
          AND status NOT IN ('CANCELLED', 'COMPLETED')
        RETURNING customer_id
        """,
        payload.order_id,
        payload.reason,
        cancelled_at,
    )
    if row is None:
        logger.info(
            "Stock rejection for order %s ignored (already cancelled/completed)",
            payload.order_id,
        )
        return

    item_rows = await conn.fetch(
        """
        SELECT product_id, quantity, unit_price_cents
        FROM order_items
        WHERE order_id = $1
        ORDER BY product_id
        """,
        payload.order_id,
    )
    cancelled = OrderCancelledPayload(
        order_id=payload.order_id,
        customer_id=row["customer_id"],
        reason=payload.reason,
        items=item_payloads([dict(item) for item in item_rows]),
        cancelled_at=cancelled_at,
    )
    outbound = new_event(
        event_type=ORDER_CANCELLED,
        producer="orders",
        key=str(payload.order_id),
        correlation_id=str(payload.order_id),
        causation_id=str(event.event_id),
        payload=cancelled.model_dump(mode="json"),
    )
    await enqueue_event(conn, ORDER_TOPIC, outbound)
    logger.info("Order %s cancelled after stock rejection", payload.order_id)


async def _on_invoice_issued(conn: asyncpg.Connection, event: Event) -> None:
    payload = InvoiceIssuedPayload.model_validate(event.payload)
    await conn.execute(
        """
        UPDATE orders
        SET invoice_id = $2,
            invoice_issued_at = COALESCE(invoice_issued_at, $3),
            updated_at = now()
        WHERE id = $1
        """,
        payload.order_id,
        payload.invoice_id,
        payload.issued_at,
    )
    await _try_complete(conn, payload.order_id, event)


async def _on_fiscal_issued(conn: asyncpg.Connection, event: Event) -> None:
    payload = FiscalIssuedPayload.model_validate(event.payload)
    await conn.execute(
        """
        UPDATE orders
        SET fiscal_document_id = $2,
            fiscal_issued_at = COALESCE(fiscal_issued_at, $3),
            updated_at = now()
        WHERE id = $1
        """,
        payload.order_id,
        payload.fiscal_document_id,
        payload.issued_at,
    )
    await _try_complete(conn, payload.order_id, event)


async def _try_complete(
    conn: asyncpg.Connection,
    order_id: UUID,
    event: Event,
) -> None:
    """Complete the order once stock, invoice and fiscal are all confirmed.

    Each of the three events calls this method, so arrival order across topics
    does not matter. The UPDATE is also the idempotency guard: only one caller
    can transition the order to COMPLETED.
    """

    row = await conn.fetchrow(
        """
        UPDATE orders
        SET status = 'COMPLETED',
            completed_at = now(),
            updated_at = now()
        WHERE id = $1
          AND stock_reserved_at IS NOT NULL
          AND invoice_issued_at IS NOT NULL
          AND fiscal_issued_at IS NOT NULL
          AND status IN ('PENDING', 'STOCK_RESERVED')
        RETURNING customer_id, total_cents, currency, invoice_id,
                  fiscal_document_id, completed_at
        """,
        order_id,
    )
    if row is None:
        return

    item_rows = await conn.fetch(
        """
        SELECT product_id, quantity, unit_price_cents
        FROM order_items
        WHERE order_id = $1
        ORDER BY product_id
        """,
        order_id,
    )
    payload = OrderCompletedPayload(
        order_id=order_id,
        customer_id=row["customer_id"],
        items=item_payloads([dict(item) for item in item_rows]),
        total_cents=row["total_cents"],
        currency=row["currency"],
        invoice_id=row["invoice_id"],
        fiscal_document_id=row["fiscal_document_id"],
        completed_at=row["completed_at"],
    )
    outbound = new_event(
        event_type=ORDER_COMPLETED,
        producer="orders",
        key=str(order_id),
        correlation_id=str(order_id),
        causation_id=str(event.event_id),
        payload=payload.model_dump(mode="json"),
    )
    await enqueue_event(conn, ORDER_TOPIC, outbound)
    logger.info("Order %s completed", order_id)


__all__ = ["handle_event"]
