-- Clients
CREATE TABLE IF NOT EXISTS customers (
customer_id TEXT PRIMARY KEY,
created_at TIMESTAMPTZ NOT NULL,
country TEXT,
marketing_opt_in BOOLEAN DEFAULT FALSE,
updated_at TIMESTAMPTZ NOT NULL
);

-- Commandes
CREATE TABLE IF NOT EXISTS orders (
order_id TEXT PRIMARY KEY,
customer_id TEXT NOT NULL REFERENCES customers(customer_id),
order_ts TIMESTAMPTZ NOT NULL,
status TEXT NOT NULL, -- CREATED, PAID, CANCELLED, SHIPPED...
currency TEXT NOT NULL,
total_amount NUMERIC(12,2) NOT NULL,
updated_at TIMESTAMPTZ NOT NULL
);

-- Lignes de commande
CREATE TABLE IF NOT EXISTS order_items (
order_item_id TEXT PRIMARY KEY,
order_id TEXT NOT NULL REFERENCES orders(order_id),
sku TEXT NOT NULL,
quantity INT NOT NULL,
unit_price NUMERIC(12,2) NOT NULL,
updated_at TIMESTAMPTZ NOT NULL
);

-- Paiements
CREATE TABLE IF NOT EXISTS payments (
payment_id TEXT PRIMARY KEY,
order_id TEXT NOT NULL REFERENCES orders(order_id),
provider TEXT NOT NULL, -- stripe, adyen...
status TEXT NOT NULL, -- AUTHORIZED, SETTLED, FAILED, REFUNDED
authorized_ts TIMESTAMPTZ,
settled_ts TIMESTAMPTZ,
amount NUMERIC(12,2) NOT NULL,
updated_at TIMESTAMPTZ NOT NULL
);

-- Index pour incrémental (IMPORTANT)
CREATE INDEX IF NOT EXISTS idx_customers_updated_at ON customers(updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders(updated_at);
CREATE INDEX IF NOT EXISTS idx_order_items_updated_at ON order_items(updated_at);
CREATE INDEX IF NOT EXISTS idx_payments_updated_at ON payments(updated_at);

-- Index utiles pour joins
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);
