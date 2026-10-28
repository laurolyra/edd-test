CREATE TABLE IF NOT EXISTS user_purchases (
    user_id TEXT NOT NULL,
    order_id UUID NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    purchased_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, order_id, product_id)
);

CREATE INDEX IF NOT EXISTS user_purchases_user_idx
    ON user_purchases (user_id, purchased_at DESC);

-- Business-level idempotency: an order should contribute to statistics once,
-- even if an operator republishes an equivalent event with a new event_id.
CREATE TABLE IF NOT EXISTS processed_orders (
    order_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS product_cooccurrence (
    product_id TEXT NOT NULL,
    related_product_id TEXT NOT NULL,
    score INTEGER NOT NULL DEFAULT 0 CHECK (score >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, related_product_id),
    CHECK (product_id <> related_product_id)
);

CREATE INDEX IF NOT EXISTS product_cooccurrence_score_idx
    ON product_cooccurrence (product_id, score DESC);

CREATE TABLE IF NOT EXISTS product_popularity (
    product_id TEXT PRIMARY KEY,
    units_purchased INTEGER NOT NULL DEFAULT 0 CHECK (units_purchased >= 0),
    orders_count INTEGER NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS product_popularity_score_idx
    ON product_popularity (units_purchased DESC);
