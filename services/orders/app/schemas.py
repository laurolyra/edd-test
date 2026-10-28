from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

MAX_ITEM_QUANTITY = 1000


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    STOCK_RESERVED = "STOCK_RESERVED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


class OrderItemIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_id: str = Field(min_length=1, max_length=64)
    quantity: int = Field(gt=0, le=MAX_ITEM_QUANTITY)
    unit_price_cents: int = Field(ge=0)


class OrderCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_id: str = Field(min_length=1, max_length=120)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    items: list[OrderItemIn] = Field(min_length=1, max_length=50)

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def reject_duplicate_products(self) -> OrderCreate:
        product_ids = [item.product_id for item in self.items]
        if len(product_ids) != len(set(product_ids)):
            raise ValueError("each product_id may appear only once; combine quantities")
        return self


class CancelOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(default="customer_request", min_length=1, max_length=500)


class OrderItemOut(BaseModel):
    product_id: str
    quantity: int
    unit_price_cents: int


class OrderOut(BaseModel):
    id: UUID
    customer_id: str
    status: str
    currency: str
    total_cents: int
    items: list[OrderItemOut]
    cancellation_reason: str | None = None
    invoice_id: UUID | None = None
    fiscal_document_id: UUID | None = None
    stock_reserved_at: datetime | None = None
    invoice_issued_at: datetime | None = None
    fiscal_issued_at: datetime | None = None
    completed_at: datetime | None = None
    cancelled_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class OrderListOut(BaseModel):
    items: list[OrderOut]
    count: int
