from __future__ import annotations

from uuid import UUID

import asyncpg
from fastapi import APIRouter, Query, Request

from app.events import OrderItemPayload
from app.schemas import (
    ReservationListOut,
    ReservationOut,
    ReservationStatus,
    RestockRequest,
    StockItemOut,
)
from edd_common.api import NotFoundError
from edd_common.runtime import ServiceRuntime

router = APIRouter(tags=["stock"])

STOCK_COLUMNS = "product_id, name, price_cents, available, reserved, updated_at"
RESERVATION_COLUMNS = "id, order_id, status, items, reason, created_at, updated_at"


def _pool(request: Request) -> asyncpg.Pool:
    runtime: ServiceRuntime = request.app.state.runtime
    return runtime.assert_ready()


@router.get("/stock", response_model=list[StockItemOut], summary="List stock levels")
async def list_stock(request: Request) -> list[dict]:
    async with _pool(request).acquire() as conn:
        rows = await conn.fetch(f"SELECT {STOCK_COLUMNS} FROM stock_items ORDER BY product_id")
    return [dict(row) for row in rows]


@router.get("/stock/{product_id}", response_model=StockItemOut, summary="Get one product")
async def get_stock_item(product_id: str, request: Request) -> dict:
    async with _pool(request).acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {STOCK_COLUMNS} FROM stock_items WHERE product_id = $1",
            product_id,
        )
    if row is None:
        raise NotFoundError(f"Product {product_id} was not found")
    return dict(row)


@router.post(
    "/stock/{product_id}/restock",
    response_model=StockItemOut,
    summary="Admin command: add units to available stock",
)
async def restock(product_id: str, payload: RestockRequest, request: Request) -> dict:
    async with _pool(request).acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE stock_items
            SET available = available + $2, updated_at = now()
            WHERE product_id = $1
            RETURNING {STOCK_COLUMNS}
            """,
            product_id,
            payload.quantity,
        )
    if row is None:
        raise NotFoundError(f"Product {product_id} was not found")
    return dict(row)


@router.get(
    "/reservations",
    response_model=ReservationListOut,
    summary="List reservations (optional status filter)",
)
async def list_reservations(
    request: Request,
    reservation_status: ReservationStatus | None = Query(default=None, alias="status"),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict:
    async with _pool(request).acquire() as conn:
        if reservation_status:
            rows = await conn.fetch(
                f"""
                SELECT {RESERVATION_COLUMNS}
                FROM reservations
                WHERE status = $1
                ORDER BY created_at DESC
                LIMIT $2
                """,
                reservation_status.value,
                limit,
            )
        else:
            rows = await conn.fetch(
                f"""
                SELECT {RESERVATION_COLUMNS}
                FROM reservations
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit,
            )
    reservations = [_reservation_dict(row) for row in rows]
    return {"items": reservations, "count": len(reservations)}


@router.get(
    "/reservations/{order_id}",
    response_model=ReservationOut,
    summary="Get the reservation for an order",
)
async def get_reservation(order_id: UUID, request: Request) -> dict:
    async with _pool(request).acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {RESERVATION_COLUMNS} FROM reservations WHERE order_id = $1",
            order_id,
        )
    if row is None:
        raise NotFoundError(f"No reservation for order {order_id}")
    return _reservation_dict(row)


def _reservation_dict(row: asyncpg.Record) -> dict:
    result = dict(row)
    result["items"] = [OrderItemPayload.model_validate(item).model_dump() for item in row["items"]]
    return result
