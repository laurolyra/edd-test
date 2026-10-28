from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


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
    reason: str
    cancelled_at: datetime


class OrderCompletedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: UUID
    completed_at: datetime


class UnavailableItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    product_id: str
    requested: int
    available: int


class StockReservedPayload(BaseModel):
    order_id: UUID
    reservation_id: UUID
    customer_id: str
    items: list[OrderItemPayload]
    total_cents: int
    currency: str
    reserved_at: datetime


class StockRejectedPayload(BaseModel):
    order_id: UUID
    reason: str
    unavailable_items: list[UnavailableItem]
    rejected_at: datetime


class StockReleasedPayload(BaseModel):
    order_id: UUID
    reservation_id: UUID
    items: list[OrderItemPayload]
    released_at: datetime


class StockCommittedPayload(BaseModel):
    order_id: UUID
    reservation_id: UUID
    committed_at: datetime


ORDER_CREATED = "order.created"
ORDER_CANCELLED = "order.cancelled"
ORDER_COMPLETED = "order.completed"

STOCK_TOPIC = "stock.events"
STOCK_RESERVED = "stock.reserved"
STOCK_REJECTED = "stock.rejected"
STOCK_RELEASED = "stock.released"
STOCK_COMMITTED = "stock.committed"
