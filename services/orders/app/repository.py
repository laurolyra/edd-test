from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import UUID, uuid4

import asyncpg

from app.events import (
    ORDER_CANCELLED,
    ORDER_CREATED,
    ORDER_TOPIC,
    OrderCancelledPayload,
    OrderCreatedPayload,
)
from app.schemas import OrderCreate
from edd_common.api import ConflictError, NotFoundError
from edd_common.events import new_event
from edd_common.outbox import enqueue_event

logger = logging.getLogger(__name__)

ORDER_COLUMNS = """
    id, customer_id, status, currency, total_cents, cancellation_reason,
    invoice_id, fiscal_document_id, stock_reserved_at, invoice_issued_at,
    fiscal_issued_at, completed_at, cancelled_at, created_at, updated_at
"""


async def _fetch_order(conn: asyncpg.Connection, order_id: UUID) -> dict | None:
    row = await conn.fetchrow(
        f"SELECT {ORDER_COLUMNS} FROM orders WHERE id = $1",
        order_id,
    )
    if row is None:
        return None

    item_rows = await conn.fetch(
        """
        SELECT product_id, quantity, unit_price_cents
        FROM order_items
        WHERE order_id = $1
        ORDER BY product_id
        """,
        order_id,
    )
    order = dict(row)
    order["items"] = [dict(item) for item in item_rows]
    return order


async def get_order(pool: asyncpg.Pool, order_id: UUID) -> dict:
    async with pool.acquire() as conn:
        order = await _fetch_order(conn, order_id)
    if order is None:
        raise NotFoundError(f"Order {order_id} was not found")
    return order


async def list_orders(
    pool: asyncpg.Pool,
    *,
    status: str | None,
    customer_id: str | None,
    limit: int,
    offset: int,
) -> tuple[list[dict], int]:
    conditions: list[str] = []
    args: list[object] = []

    if status:
        args.append(status)
        conditions.append(f"status = ${len(args)}")
    if customer_id:
        args.append(customer_id)
        conditions.append(f"customer_id = ${len(args)}")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    async with pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT count(*) FROM orders {where}", *args)
        rows = await conn.fetch(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM orders
            {where}
            ORDER BY created_at DESC
            LIMIT ${len(args) + 1} OFFSET ${len(args) + 2}
            """,
            *args,
            limit,
            offset,
        )

        orders: list[dict] = []
        for row in rows:
            order = dict(row)
            item_rows = await conn.fetch(
                """
                SELECT product_id, quantity, unit_price_cents
                FROM order_items
                WHERE order_id = $1
                ORDER BY product_id
                """,
                row["id"],
            )
            order["items"] = [dict(item) for item in item_rows]
            orders.append(order)

    return orders, int(total)


async def create_order(pool: asyncpg.Pool, request: OrderCreate) -> dict:
    order_id = uuid4()
    created_at = datetime.now(UTC)
    items = [item.model_dump() for item in request.items]
    total_cents = sum(item["quantity"] * item["unit_price_cents"] for item in items)

    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            """
            INSERT INTO orders (
                id, customer_id, status, currency, total_cents, created_at, updated_at
            )
            VALUES ($1, $2, 'PENDING', $3, $4, $5, $5)
            """,
            order_id,
            request.customer_id,
            request.currency,
            total_cents,
            created_at,
        )
        await conn.executemany(
            """
            INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents)
            VALUES ($1, $2, $3, $4)
            """,
            [
                (order_id, item["product_id"], item["quantity"], item["unit_price_cents"])
                for item in items
            ],
        )

        payload = OrderCreatedPayload(
            order_id=order_id,
            customer_id=request.customer_id,
            items=items,
            total_cents=total_cents,
            currency=request.currency,
            created_at=created_at,
        )
        event = new_event(
            event_type=ORDER_CREATED,
            producer="orders",
            key=str(order_id),
            correlation_id=str(order_id),
            payload=payload.model_dump(mode="json"),
        )
        await enqueue_event(conn, ORDER_TOPIC, event)
        order = await _fetch_order(conn, order_id)

    assert order is not None
    logger.info("Created order %s for customer %s", order_id, request.customer_id)
    return order


async def cancel_order(pool: asyncpg.Pool, order_id: UUID, reason: str) -> dict:
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            f"SELECT {ORDER_COLUMNS} FROM orders WHERE id = $1 FOR UPDATE",
            order_id,
        )
        if row is None:
            raise NotFoundError(f"Order {order_id} was not found")
        if row["status"] == "COMPLETED":
            raise ConflictError("Completed orders cannot be cancelled")
        if row["status"] == "CANCELLED":
            order = await _fetch_order(conn, order_id)
            assert order is not None
            return order

        cancelled_at = datetime.now(UTC)
        await conn.execute(
            """
                UPDATE orders
                SET status = 'CANCELLED',
                    cancellation_reason = $2,
                    cancelled_at = $3,
                    updated_at = now()
                WHERE id = $1
                """,
            order_id,
            reason,
            cancelled_at,
        )

        item_rows = await conn.fetch(
            """
                SELECT product_id, quantity, unit_price_cents
                FROM order_items
                WHERE order_id = $1
                ORDER BY product_id
                """,
            order_id,
        )
        items = [dict(item) for item in item_rows]
        payload = OrderCancelledPayload(
            order_id=order_id,
            customer_id=row["customer_id"],
            reason=reason,
            items=items,
            cancelled_at=cancelled_at,
        )
        event = new_event(
            event_type=ORDER_CANCELLED,
            producer="orders",
            key=str(order_id),
            correlation_id=str(order_id),
            payload=payload.model_dump(mode="json"),
        )
        await enqueue_event(conn, ORDER_TOPIC, event)
        order = await _fetch_order(conn, order_id)

    assert order is not None
    logger.info("Cancelled order %s: %s", order_id, reason)
    return order
