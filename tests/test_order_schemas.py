from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services" / "orders"))

from app.schemas import OrderCreate  # noqa: E402


def test_order_rejects_duplicate_products() -> None:
    with pytest.raises(ValidationError, match="only once"):
        OrderCreate(
            customer_id="alice",
            items=[
                {"product_id": "p-1", "quantity": 1, "unit_price_cents": 100},
                {"product_id": "p-1", "quantity": 2, "unit_price_cents": 100},
            ],
        )


def test_order_normalizes_currency_and_accepts_valid_items() -> None:
    order = OrderCreate(
        customer_id="alice",
        currency="brl",
        items=[{"product_id": "p-1", "quantity": 2, "unit_price_cents": 500}],
    )
    assert order.currency == "BRL"
    assert order.items[0].quantity == 2
