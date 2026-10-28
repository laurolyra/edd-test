CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    customer_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'STOCK_RESERVED', 'COMPLETED', 'CANCELLED')),
    currency TEXT NOT NULL DEFAULT 'USD',
    total_cents BIGINT NOT NULL CHECK (total_cents >= 0),
    cancellation_reason TEXT,
    invoice_id UUID,
    fiscal_document_id UUID,
    stock_reserved_at TIMESTAMPTZ,
    invoice_issued_at TIMESTAMPTZ,
    fiscal_issued_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents BIGINT NOT NULL CHECK (unit_price_cents >= 0),
    PRIMARY KEY (order_id, product_id)
);

CREATE INDEX IF NOT EXISTS orders_customer_idx
    ON orders (customer_id, created_at DESC);

CREATE INDEX IF NOT EXISTS orders_status_idx
    ON orders (status, created_at DESC);
