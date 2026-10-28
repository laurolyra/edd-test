from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ReservationStatus(StrEnum):
    RESERVED = "RESERVED"
    REJECTED = "REJECTED"
    RELEASED = "RELEASED"
    COMMITTED = "COMMITTED"


class RestockRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    quantity: int = Field(gt=0, le=100_000)


class StockItemOut(BaseModel):
    product_id: str
    name: str
    price_cents: int
    available: int
    reserved: int
    updated_at: datetime


class ReservationItemOut(BaseModel):
    product_id: str
    quantity: int
    unit_price_cents: int


class ReservationOut(BaseModel):
    id: UUID
    order_id: UUID
    status: str
    items: list[ReservationItemOut]
    reason: str | None = None
    created_at: datetime
    updated_at: datetime


class ReservationListOut(BaseModel):
    items: list[ReservationOut]
    count: int
