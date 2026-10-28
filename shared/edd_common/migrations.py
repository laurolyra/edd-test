from __future__ import annotations

import logging
from pathlib import Path

import asyncpg

logger = logging.getLogger(__name__)

PLATFORM_DDL = """
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    message_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    envelope JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (created_at, id)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID NOT NULL,
    consumer_group TEXT NOT NULL,
    event_type TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer_group)
);

CREATE INDEX IF NOT EXISTS processed_events_cleanup_idx
    ON processed_events (processed_at);
"""


async def ensure_platform_tables(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(PLATFORM_DDL)


async def apply_migrations(pool: asyncpg.Pool, migrations_dir: Path) -> None:
    """Apply ``*.sql`` files in lexical order, once per database.

    This is intentionally tiny. In a production service you would reach for
    Alembic (or another migration tool) so migrations can be reviewed and
    rolled back. The important habit is the same: versioned, ordered DDL.
    """

    migrations_dir = Path(migrations_dir)
    if not migrations_dir.exists():
        raise RuntimeError(f"Migrations directory does not exist: {migrations_dir}")

    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )

        files = sorted(migrations_dir.glob("*.sql"))
        for migration in files:
            already_applied = await conn.fetchval(
                "SELECT 1 FROM schema_migrations WHERE version = $1",
                migration.name,
            )
            if already_applied:
                continue

            logger.info("Applying migration %s", migration.name)
            async with conn.transaction():
                await conn.execute(migration.read_text(encoding="utf-8"))
                await conn.execute(
                    "INSERT INTO schema_migrations (version) VALUES ($1)",
                    migration.name,
                )
