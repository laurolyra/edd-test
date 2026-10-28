CREATE TABLE IF NOT EXISTS stock_items (
    product_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    price_cents BIGINT NOT NULL CHECK (price_cents >= 0),
    available INTEGER NOT NULL DEFAULT 0 CHECK (available >= 0),
    reserved INTEGER NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reservations (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    status TEXT NOT NULL
        CHECK (status IN ('RESERVED', 'REJECTED', 'RELEASED', 'COMMITTED')),
    items JSONB NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS reservations_status_idx
    ON reservations (status, created_at DESC);

INSERT INTO stock_items (product_id, name, price_cents, available)
VALUES
    ('p-100', 'Mechanical Keyboard', 12900, 10),
    ('p-101', 'Noise-Cancelling Headphones', 24900, 8),
    ('p-102', '27-inch Monitor', 39900, 5),
    ('p-103', 'USB-C Hub', 7900, 20),
    ('p-104', 'Ergonomic Mouse', 5900, 15),
    ('p-105', 'Laptop Stand', 4900, 12)
ON CONFLICT (product_id) DO NOTHING;
