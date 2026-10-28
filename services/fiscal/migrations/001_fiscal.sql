CREATE SEQUENCE IF NOT EXISTS fiscal_number_seq START WITH 1;

CREATE TABLE IF NOT EXISTS fiscal_documents (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    invoice_id UUID NOT NULL UNIQUE,
    invoice_number TEXT NOT NULL,
    document_number TEXT NOT NULL UNIQUE,
    access_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'ISSUED' CHECK (status IN ('ISSUED', 'CANCELLED')),
    customer_id TEXT NOT NULL,
    total_cents BIGINT NOT NULL CHECK (total_cents >= 0),
    tax_cents BIGINT NOT NULL CHECK (tax_cents >= 0),
    payload JSONB NOT NULL,
    xml_content TEXT NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    cancelled_at TIMESTAMPTZ,
    cancel_reason TEXT
);

CREATE INDEX IF NOT EXISTS fiscal_status_idx
    ON fiscal_documents (status, issued_at DESC);
