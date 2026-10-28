from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel


class InvoiceStatus(StrEnum):
    ISSUED = "ISSUED"
    VOIDED = "VOIDED"


class InvoiceItemOut(BaseModel):
    product_id: str
    quantity: int
    unit_price_cents: int


class InvoiceOut(BaseModel):
    id: UUID
    order_id: UUID
    invoice_number: str
    customer_id: str
    total_cents: int
    currency: str
    status: str
    items: list[InvoiceItemOut]
    issued_at: datetime
    voided_at: datetime | None = None
    void_reason: str | None = None


class InvoiceListOut(BaseModel):
    items: list[InvoiceOut]
    count: int
