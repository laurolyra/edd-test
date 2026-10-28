from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

ORDER_TOPIC = "orders.events"

ORDER_CREATED = "order.created"
ORDER_CANCELLED = "order.cancelled"
ORDER_COMPLETED = "order.completed"

STOCK_RESERVED = "stock.reserved"
STOCK_REJECTED = "stock.rejected"

INVOICE_ISSUED = "invoice.issued"
INVOICE_VOIDED = "invoice.voided"

FISCAL_ISSUED = "fiscal.issued"
FISCAL_CANCELLED = "fiscal.cancelled"


class OrderItemPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    product_id: str
    quantity: int
    unit_price_cents: int


class OrderCreatedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: UUID
    customer_id: str
    items: list[OrderItemPayload]
    total_cents: int
    currency: str
    created_at: datetime


class OrderCancelledPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: UUID
    customer_id: str
    reason: str
    items: list[OrderItemPayload]
    cancelled_at: datetime


class OrderCompletedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: UUID
    customer_id: str
    items: list[OrderItemPayload]
    total_cents: int
    currency: str
    invoice_id: UUID
    fiscal_document_id: UUID
    completed_at: datetime


class StockReservedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: UUID
    reservation_id: UUID
    reserved_at: datetime


class UnavailableItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    product_id: str
    requested: int
    available: int


class StockRejectedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: UUID
    reason: str
    unavailable_items: list[UnavailableItem]
    rejected_at: datetime


class InvoiceIssuedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    invoice_id: UUID
    invoice_number: str
    order_id: UUID
    total_cents: int
    currency: str
    issued_at: datetime


class InvoiceVoidedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    invoice_id: UUID
    order_id: UUID
    reason: str
    voided_at: datetime


class FiscalIssuedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    fiscal_document_id: UUID
    order_id: UUID
    invoice_id: UUID
    document_number: str
    access_key: str
    issued_at: datetime


class FiscalCancelledPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    fiscal_document_id: UUID
    order_id: UUID
    invoice_id: UUID
    reason: str
    cancelled_at: datetime


def item_payloads(items: list[dict]) -> list[dict]:
    return [
        {
            "product_id": item["product_id"],
            "quantity": item["quantity"],
            "unit_price_cents": item["unit_price_cents"],
        }
        for item in items
    ]
