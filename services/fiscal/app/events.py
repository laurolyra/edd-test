from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class OrderItemPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    product_id: str
    quantity: int
    unit_price_cents: int


class InvoiceIssuedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    invoice_id: UUID
    invoice_number: str
    order_id: UUID
    customer_id: str
    items: list[OrderItemPayload]
    total_cents: int
    currency: str
    issued_at: datetime


class InvoiceVoidedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    invoice_id: UUID
    invoice_number: str
    order_id: UUID
    reason: str
    voided_at: datetime


class FiscalIssuedPayload(BaseModel):
    fiscal_document_id: UUID
    order_id: UUID
    invoice_id: UUID
    document_number: str
    access_key: str
    issued_at: datetime


class FiscalCancelledPayload(BaseModel):
    fiscal_document_id: UUID
    order_id: UUID
    invoice_id: UUID
    reason: str
    cancelled_at: datetime


INVOICE_ISSUED = "invoice.issued"
INVOICE_VOIDED = "invoice.voided"

FISCAL_TOPIC = "fiscal.events"
FISCAL_ISSUED = "fiscal.issued"
FISCAL_CANCELLED = "fiscal.cancelled"
