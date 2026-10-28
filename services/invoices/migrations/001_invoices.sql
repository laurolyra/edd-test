CREATE SEQUENCE IF NOT EXISTS invoice_number_seq START WITH 1000;

CREATE TABLE IF NOT EXISTS invoices (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    invoice_number TEXT NOT NULL UNIQUE,
    customer_id TEXT NOT NULL,
    total_cents BIGINT NOT NULL CHECK (total_cents >= 0),
    currency TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ISSUED' CHECK (status IN ('ISSUED', 'VOIDED')),
    items JSONB NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    voided_at TIMESTAMPTZ,
    void_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS invoices_status_idx
    ON invoices (status, issued_at DESC);

-- Remembers cancellations that arrived before stock reservation was confirmed.
-- This closes the race between "stock.reserved" and "order.cancelled" arriving
-- from two different topics.
CREATE TABLE IF NOT EXISTS cancelled_orders (
    order_id UUID PRIMARY KEY,
    reason TEXT NOT NULL,
    cancelled_at TIMESTAMPTZ NOT NULL
);
