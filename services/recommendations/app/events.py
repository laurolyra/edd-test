from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class OrderItemPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    product_id: str
    quantity: int
    unit_price_cents: int


class OrderCompletedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: UUID
    customer_id: str
    items: list[OrderItemPayload]
    total_cents: int
    currency: str
    completed_at: datetime


class RecommendationOut(BaseModel):
    product_id: str
    score: int
    reason: str


class RecommendationsResponse(BaseModel):
    user_id: str
    based_on: list[str]
    items: list[RecommendationOut]
    generated_at: datetime


class PurchaseOut(BaseModel):
    order_id: UUID
    product_id: str
    quantity: int
    purchased_at: datetime


class PurchaseListOut(BaseModel):
    user_id: str
    items: list[PurchaseOut]
    count: int


class CooccurrenceOut(BaseModel):
    product_id: str
    related_product_id: str
    score: int


class StatsOut(BaseModel):
    users: int
    purchases: int
    top_pairs: list[CooccurrenceOut]
    top_products: list[dict]


ORDER_COMPLETED = "order.completed"
