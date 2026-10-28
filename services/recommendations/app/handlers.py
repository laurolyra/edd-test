from __future__ import annotations

import logging
from itertools import combinations

import asyncpg

from app.events import ORDER_COMPLETED, OrderCompletedPayload, OrderItemPayload
from edd_common.events import Event

logger = logging.getLogger(__name__)


async def handle_event(conn: asyncpg.Connection, event: Event) -> None:
    if event.event_type == ORDER_COMPLETED:
        await _on_order_completed(conn, event)
    else:
        logger.debug("Ignoring event type %s", event.event_type)


async def _on_order_completed(conn: asyncpg.Connection, event: Event) -> None:
    payload = OrderCompletedPayload.model_validate(event.payload)

    claimed = await conn.fetchval(
        """
        INSERT INTO processed_orders (order_id)
        VALUES ($1)
        ON CONFLICT (order_id) DO NOTHING
        RETURNING order_id
        """,
        payload.order_id,
    )
    if claimed is None:
        logger.info("Order %s already contributed to recommendations", payload.order_id)
        return

    items = [OrderItemPayload.model_validate(item) for item in payload.items]

    for item in items:
        await conn.execute(
            """
            INSERT INTO user_purchases (user_id, order_id, product_id, quantity, purchased_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (user_id, order_id, product_id) DO NOTHING
            """,
            payload.customer_id,
            payload.order_id,
            item.product_id,
            item.quantity,
            payload.completed_at,
        )
        await conn.execute(
            """
            INSERT INTO product_popularity (product_id, units_purchased, orders_count)
            VALUES ($1, $2, 1)
            ON CONFLICT (product_id) DO UPDATE
            SET units_purchased = product_popularity.units_purchased + EXCLUDED.units_purchased,
                orders_count = product_popularity.orders_count + 1,
                updated_at = now()
            """,
            item.product_id,
            item.quantity,
        )

    # Co-occurrence is symmetric: buying A and B together strengthens both
    # "people who bought A also bought B" and the reverse direction.
    for left, right in combinations(items, 2):
        for product_id, related_product_id in (
            (left.product_id, right.product_id),
            (right.product_id, left.product_id),
        ):
            await conn.execute(
                """
                INSERT INTO product_cooccurrence (product_id, related_product_id, score)
                VALUES ($1, $2, 1)
                ON CONFLICT (product_id, related_product_id) DO UPDATE
                SET score = product_cooccurrence.score + 1,
                    updated_at = now()
                """,
                product_id,
                related_product_id,
            )

    logger.info(
        "Updated recommendations from order %s (%d product(s))",
        payload.order_id,
        len(items),
    )
