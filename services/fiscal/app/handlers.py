from __future__ import annotations

import logging
import secrets
from datetime import UTC, datetime
from uuid import uuid4
from xml.sax.saxutils import escape

import asyncpg

from app.events import (
    FISCAL_CANCELLED,
    FISCAL_ISSUED,
    FISCAL_TOPIC,
    INVOICE_ISSUED,
    INVOICE_VOIDED,
    FiscalCancelledPayload,
    FiscalIssuedPayload,
    InvoiceIssuedPayload,
    InvoiceVoidedPayload,
)
from edd_common.events import Event, new_event
from edd_common.outbox import enqueue_event

logger = logging.getLogger(__name__)


async def handle_event(conn: asyncpg.Connection, event: Event) -> None:
    if event.event_type == INVOICE_ISSUED:
        await _on_invoice_issued(conn, event)
    elif event.event_type == INVOICE_VOIDED:
        await _on_invoice_voided(conn, event)
    else:
        logger.debug("Ignoring event type %s", event.event_type)


def _access_key() -> str:
    """A fake 44-digit fiscal access key (the real format encodes state, date, CNPJ, etc.)."""

    return "".join(secrets.choice("0123456789") for _ in range(44))


def _build_xml(
    *,
    document_number: str,
    access_key: str,
    customer_id: str,
    items: list[dict],
    total_cents: int,
    tax_cents: int,
) -> str:
    item_xml = "\n".join(
        (
            "    <item>"
            f"<codigo>{escape(item['product_id'])}</codigo>"
            f"<quantidade>{item['quantity']}</quantidade>"
            f"<valorUnitarioCents>{item['unit_price_cents']}</valorUnitarioCents>"
            "</item>"
        )
        for item in items
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<notaFiscal>\n"
        f"  <numero>{escape(document_number)}</numero>\n"
        f"  <chaveAcesso>{access_key}</chaveAcesso>\n"
        "  <emitente>Event Driven Shop</emitente>\n"
        f"  <destinatario>{escape(customer_id)}</destinatario>\n"
        "  <itens>\n"
        f"{item_xml}\n"
        "  </itens>\n"
        f"  <totalCents>{total_cents}</totalCents>\n"
        f"  <impostoCents>{tax_cents}</impostoCents>\n"
        "</notaFiscal>\n"
    )


async def _on_invoice_issued(conn: asyncpg.Connection, event: Event) -> None:
    payload = InvoiceIssuedPayload.model_validate(event.payload)

    existing = await conn.fetchval(
        "SELECT id FROM fiscal_documents WHERE invoice_id = $1",
        payload.invoice_id,
    )
    if existing is not None:
        logger.info("Fiscal document for invoice %s already exists", payload.invoice_id)
        return

    sequence = await conn.fetchval("SELECT nextval('fiscal_number_seq')")
    document_id = uuid4()
    issued_at = datetime.now(UTC)
    document_number = f"NF-{issued_at.year}-{int(sequence):06d}"
    access_key = _access_key()
    tax_cents = payload.total_cents // 10
    items = [item.model_dump() for item in payload.items]

    xml_content = _build_xml(
        document_number=document_number,
        access_key=access_key,
        customer_id=payload.customer_id,
        items=items,
        total_cents=payload.total_cents,
        tax_cents=tax_cents,
    )
    fiscal_payload = {
        "issuer": "Event Driven Shop",
        "recipient": payload.customer_id,
        "items": items,
        "total_cents": payload.total_cents,
        "tax_cents": tax_cents,
        "currency": payload.currency,
    }

    await conn.execute(
        """
        INSERT INTO fiscal_documents (
            id, order_id, invoice_id, invoice_number, document_number,
            access_key, status, customer_id, total_cents, tax_cents,
            payload, xml_content, issued_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'ISSUED', $7, $8, $9, $10, $11, $12)
        """,
        document_id,
        payload.order_id,
        payload.invoice_id,
        payload.invoice_number,
        document_number,
        access_key,
        payload.customer_id,
        payload.total_cents,
        tax_cents,
        fiscal_payload,
        xml_content,
        issued_at,
    )

    issued = FiscalIssuedPayload(
        fiscal_document_id=document_id,
        order_id=payload.order_id,
        invoice_id=payload.invoice_id,
        document_number=document_number,
        access_key=access_key,
        issued_at=issued_at,
    )
    outbound = new_event(
        event_type=FISCAL_ISSUED,
        producer="fiscal",
        key=str(payload.order_id),
        correlation_id=str(payload.order_id),
        causation_id=str(event.event_id),
        payload=issued.model_dump(mode="json"),
    )
    await enqueue_event(conn, FISCAL_TOPIC, outbound)
    logger.info("Issued fiscal document %s for order %s", document_number, payload.order_id)


async def _on_invoice_voided(conn: asyncpg.Connection, event: Event) -> None:
    payload = InvoiceVoidedPayload.model_validate(event.payload)
    row = await conn.fetchrow(
        """
        SELECT id, document_number, status
        FROM fiscal_documents
        WHERE invoice_id = $1
        FOR UPDATE
        """,
        payload.invoice_id,
    )
    if row is None:
        logger.info("No fiscal document yet for voided invoice %s", payload.invoice_id)
        return
    if row["status"] == "CANCELLED":
        logger.info("Fiscal document %s is already cancelled", row["document_number"])
        return

    cancelled_at = datetime.now(UTC)
    await conn.execute(
        """
        UPDATE fiscal_documents
        SET status = 'CANCELLED',
            cancelled_at = $2,
            cancel_reason = $3
        WHERE id = $1
        """,
        row["id"],
        cancelled_at,
        payload.reason,
    )

    cancelled = FiscalCancelledPayload(
        fiscal_document_id=row["id"],
        order_id=payload.order_id,
        invoice_id=payload.invoice_id,
        reason=payload.reason,
        cancelled_at=cancelled_at,
    )
    outbound = new_event(
        event_type=FISCAL_CANCELLED,
        producer="fiscal",
        key=str(payload.order_id),
        correlation_id=str(payload.order_id),
        causation_id=str(event.event_id),
        payload=cancelled.model_dump(mode="json"),
    )
    await enqueue_event(conn, FISCAL_TOPIC, outbound)
    logger.info("Cancelled fiscal document %s", row["document_number"])
