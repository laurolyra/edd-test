from __future__ import annotations

from datetime import UTC, datetime

import asyncpg
from fastapi import APIRouter, Query, Request

from app.schemas import (
    CooccurrenceOut,
    PurchaseListOut,
    RecommendationsResponse,
    StatsOut,
)
from edd_common.runtime import ServiceRuntime

router = APIRouter(tags=["recommendations"])


def _pool(request: Request) -> asyncpg.Pool:
    runtime: ServiceRuntime = request.app.state.runtime
    return runtime.assert_ready()


@router.get(
    "/recommendations/{user_id}",
    response_model=RecommendationsResponse,
    summary="Recommend products from co-purchase history",
)
async def get_recommendations(
    user_id: str,
    request: Request,
    limit: int = Query(default=5, ge=1, le=20),
) -> dict:
    async with _pool(request).acquire() as conn:
        purchased_rows = await conn.fetch(
            """
            SELECT DISTINCT product_id
            FROM user_purchases
            WHERE user_id = $1
            ORDER BY product_id
            """,
            user_id,
        )
        bought = [row["product_id"] for row in purchased_rows]

        cooccurrence_rows = await conn.fetch(
            """
            SELECT related_product_id AS product_id, SUM(score)::int AS score
            FROM product_cooccurrence
            WHERE product_id = ANY($1::text[])
              AND NOT (related_product_id = ANY($1::text[]))
            GROUP BY related_product_id
            ORDER BY score DESC, product_id
            LIMIT $2
            """,
            bought,
            limit,
        )

        items = [
            {
                "product_id": row["product_id"],
                "score": row["score"],
                "reason": "bought_together",
            }
            for row in cooccurrence_rows
        ]

        if len(items) < limit:
            excluded = bought + [item["product_id"] for item in items]
            popular_rows = await conn.fetch(
                """
                SELECT product_id, units_purchased AS score
                FROM product_popularity
                WHERE NOT (product_id = ANY($1::text[]))
                ORDER BY units_purchased DESC, product_id
                LIMIT $2
                """,
                excluded,
                limit - len(items),
            )
            items.extend(
                {
                    "product_id": row["product_id"],
                    "score": row["score"],
                    "reason": "popular",
                }
                for row in popular_rows
            )

    return {
        "user_id": user_id,
        "based_on": bought,
        "items": items,
        "generated_at": datetime.now(UTC),
    }


@router.get(
    "/users/{user_id}/purchases",
    response_model=PurchaseListOut,
    summary="Purchase history used to build recommendations",
)
async def get_purchases(user_id: str, request: Request) -> dict:
    async with _pool(request).acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT order_id, product_id, quantity, purchased_at
            FROM user_purchases
            WHERE user_id = $1
            ORDER BY purchased_at DESC, product_id
            """,
            user_id,
        )
    purchases = [dict(row) for row in rows]
    return {"user_id": user_id, "items": purchases, "count": len(purchases)}


@router.get("/stats", response_model=StatsOut, summary="Projection statistics and top pairs")
async def get_stats(request: Request) -> dict:
    async with _pool(request).acquire() as conn:
        users = await conn.fetchval("SELECT count(DISTINCT user_id) FROM user_purchases")
        purchases = await conn.fetchval("SELECT count(*) FROM user_purchases")
        pair_rows = await conn.fetch(
            """
            SELECT product_id, related_product_id, score
            FROM product_cooccurrence
            ORDER BY score DESC, product_id
            LIMIT 10
            """
        )
        product_rows = await conn.fetch(
            """
            SELECT product_id, units_purchased, orders_count
            FROM product_popularity
            ORDER BY units_purchased DESC, product_id
            LIMIT 10
            """
        )
    return {
        "users": int(users or 0),
        "purchases": int(purchases or 0),
        "top_pairs": [CooccurrenceOut(**dict(row)).model_dump() for row in pair_rows],
        "top_products": [dict(row) for row in product_rows],
    }
