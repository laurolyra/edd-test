from __future__ import annotations

import json
from typing import Any

import asyncpg


async def _init_connection(conn: asyncpg.Connection) -> None:
    # Let asyncpg exchange Python dict/list values with Postgres JSON/JSONB
    # columns directly. This keeps the outbox code free of json.dumps noise.
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def create_pool(database_url: str, *, min_size: int = 1, max_size: int = 10) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=database_url,
        min_size=min_size,
        max_size=max_size,
        init=_init_connection,
    )


async def fetch_value(pool: asyncpg.Pool, query: str, *args: Any) -> Any:
    async with pool.acquire() as conn:
        return await conn.fetchval(query, *args)
