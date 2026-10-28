from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import uuid4

import asyncpg

from app.events import (
    INVOICE_ISSUED,
    INVOICE_TOPIC,
    INVOICE_VOIDED,
    ORDER_CANCELLED,
    STOCK_RESERVED,
    InvoiceIssuedPayload,
    InvoiceVoidedPayload,
    OrderCancelledPayload,
    StockReservedPayload,
)
from edd_common.events import Event, new_event
from edd_common.outbox import enqueue_event

logger = logging.getLogger(__name__)

INVOICE_COLUMNS = (
    "id, order_id, invoice_number, customer_id, total_cents, currency, "
    "status, items, issued_at, voided_at, void_reason"
)


async def handle_event(conn: asyncpg.Connection, event: Event) -> None:
    if event.event_type == STOCK_RESERVED:
        await _on_stock_reserved(conn, event)
    elif event.event_type == ORDER_CANCELLED:
        await _on_order_cancelled(conn, event)
    else:
        logger.debug("Ignoring event type %s", event.event_type)


async def _on_stock_reserved(conn: asyncpg.Connection, event: Event) -> None:
    payload = StockReservedPayload.model_validate(event.payload)

    was_cancelled = await conn.fetchval(
        "SELECT 1 FROM cancelled_orders WHERE order_id = $1",
        payload.order_id,
    )
    if was_cancelled:
        logger.info(
            "Order %s was cancelled before invoicing; skipping invoice",
            payload.order_id,
        )
        return

    existing = await conn.fetchval(
        "SELECT id FROM invoices WHERE order_id = $1",
        payload.order_id,
    )
    if existing is not None:
        logger.info("Invoice for order %s already exists", payload.order_id)
        return

    sequence = await conn.fetchval("SELECT nextval('invoice_number_seq')")
    invoice_id = uuid4()
    issued_at = datetime.now(UTC)
    invoice_number = f"INV-{issued_at.year}-{int(sequence):08d}"
    items = [item.model_dump() for item in payload.items]

    await conn.execute(
        """
        INSERT INTO invoices (
            id, order_id, invoice_number, customer_id, total_cents,
            currency, status, items, issued_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'ISSUED', $7, $8)
        """,
        invoice_id,
        payload.order_id,
        invoice_number,
        payload.customer_id,
        payload.total_cents,
        payload.currency,
        items,
        issued_at,
    )

    issued = InvoiceIssuedPayload(
        invoice_id=invoice_id,
        invoice_number=invoice_number,
        order_id=payload.order_id,
        customer_id=payload.customer_id,
        items=payload.items,
        total_cents=payload.total_cents,
        currency=payload.currency,
        issued_at=issued_at,
    )
    outbound = new_event(
        event_type=INVOICE_ISSUED,
        producer="invoices",
        key=str(payload.order_id),
        correlation_id=str(payload.order_id),
        causation_id=str(event.event_id),
        payload=issued.model_dump(mode="json"),
    )
    await enqueue_event(conn, INVOICE_TOPIC, outbound)
    logger.info("Issued invoice %s for order %s", invoice_number, payload.order_id)


async def _on_order_cancelled(conn: asyncpg.Connection, event: Event) -> None:
    payload = OrderCancelledPayload.model_validate(event.payload)

    # Always remember the cancellation, even if the invoice does not exist yet.
    await conn.execute(
        """
        INSERT INTO cancelled_orders (order_id, reason, cancelled_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (order_id) DO NOTHING
        """,
        payload.order_id,
        payload.reason,
        payload.cancelled_at,
    )

    row = await conn.fetchrow(
        f"SELECT {INVOICE_COLUMNS} FROM invoices WHERE order_id = $1 FOR UPDATE",
        payload.order_id,
    )
    if row is None:
        logger.info("No invoice yet for cancelled order %s", payload.order_id)
        return
    if row["status"] == "VOIDED":
        logger.info("Invoice %s is already voided", row["invoice_number"])
        return

    voided_at = datetime.now(UTC)
    await conn.execute(
        """
        UPDATE invoices
        SET status = 'VOIDED',
            voided_at = $2,
            void_reason = $3
        WHERE id = $1
        """,
        row["id"],
        voided_at,
        payload.reason,
    )

    voided = InvoiceVoidedPayload(
        invoice_id=row["id"],
        invoice_number=row["invoice_number"],
        order_id=payload.order_id,
        reason=payload.reason,
        voided_at=voided_at,
    )
    outbound = new_event(
        event_type=INVOICE_VOIDED,
        producer="invoices",
        key=str(payload.order_id),
        correlation_id=str(payload.order_id),
        causation_id=str(event.event_id),
        payload=voided.model_dump(mode="json"),
    )
    await enqueue_event(conn, INVOICE_TOPIC, outbound)
    logger.info("Voided invoice %s for order %s", row["invoice_number"], payload.order_id)
