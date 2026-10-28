from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


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
