from __future__ import annotations

from uuid import UUID

import asyncpg
from fastapi import APIRouter, Query, Request, status

from app import repository
from app.schemas import (
    CancelOrderRequest,
    OrderCreate,
    OrderListOut,
    OrderOut,
    OrderStatus,
)
from edd_common.runtime import ServiceRuntime

router = APIRouter(prefix="/orders", tags=["orders"])


def _pool(request: Request) -> asyncpg.Pool:
    runtime: ServiceRuntime = request.app.state.runtime
    return runtime.assert_ready()


@router.post(
    "",
    response_model=OrderOut,
    status_code=status.HTTP_201_CREATED,
    summary="Place an order (writes order + outbox event atomically)",
)
async def create_order(payload: OrderCreate, request: Request) -> dict:
    return await repository.create_order(_pool(request), payload)


@router.get("", response_model=OrderListOut, summary="List orders")
async def list_orders(
    request: Request,
    order_status: OrderStatus | None = Query(default=None, alias="status"),
    customer_id: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict:
    orders, count = await repository.list_orders(
        _pool(request),
        status=order_status.value if order_status else None,
        customer_id=customer_id,
        limit=limit,
        offset=offset,
    )
    return {"items": orders, "count": count}


@router.get("/{order_id}", response_model=OrderOut, summary="Get one order")
async def get_order(order_id: UUID, request: Request) -> dict:
    return await repository.get_order(_pool(request), order_id)


@router.post(
    "/{order_id}/cancel",
    response_model=OrderOut,
    summary="Cancel an order (starts the compensation flow)",
)
async def cancel_order(
    order_id: UUID,
    request: Request,
    payload: CancelOrderRequest | None = None,
) -> dict:
    reason = payload.reason if payload else "customer_request"
    return await repository.cancel_order(_pool(request), order_id, reason)
