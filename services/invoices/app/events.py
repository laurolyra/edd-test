from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class OrderItemPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    product_id: str
    quantity: int
    unit_price_cents: int


class StockReservedPayload(BaseModel):
    """Event-carried state: stock forwards the order snapshot it received.

    This means invoices does not need a synchronous call (or a second event)
    to fetch the order total and items. Events should carry the facts their
    consumers need.
    """

    model_config = ConfigDict(extra="ignore")

    order_id: UUID
    reservation_id: UUID
    customer_id: str
    items: list[OrderItemPayload]
    total_cents: int
    currency: str
    reserved_at: datetime


class OrderCancelledPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: UUID
    reason: str
    cancelled_at: datetime


class InvoiceIssuedPayload(BaseModel):
    invoice_id: UUID
    invoice_number: str
    order_id: UUID
    customer_id: str
    items: list[OrderItemPayload]
    total_cents: int
    currency: str
    issued_at: datetime


class InvoiceVoidedPayload(BaseModel):
    invoice_id: UUID
    invoice_number: str
    order_id: UUID
    reason: str
    voided_at: datetime


ORDER_CANCELLED = "order.cancelled"
STOCK_RESERVED = "stock.reserved"

INVOICE_TOPIC = "invoices.events"
INVOICE_ISSUED = "invoice.issued"
INVOICE_VOIDED = "invoice.voided"
