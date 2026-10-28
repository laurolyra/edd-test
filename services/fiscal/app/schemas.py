from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel


class FiscalStatus(StrEnum):
    ISSUED = "ISSUED"
    CANCELLED = "CANCELLED"


class FiscalDocumentOut(BaseModel):
    id: UUID
    order_id: UUID
    invoice_id: UUID
    invoice_number: str
    document_number: str
    access_key: str
    status: str
    customer_id: str
    total_cents: int
    tax_cents: int
    issued_at: datetime
    cancelled_at: datetime | None = None
    cancel_reason: str | None = None
