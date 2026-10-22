CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${BQ_RAW_DS}.customers` (
    customer_id STRING NOT NULL,
    created_at TIMESTAMP,
    country STRING,
    marketing_opt_in BOOL,
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMP
)
CLUSTER BY customer_id;

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${BQ_RAW_DS}.orders` (
    order_id STRING NOT NULL,
    customer_id STRING,
    order_ts TIMESTAMP,
    status STRING,
    currency STRING,
    total_amount NUMERIC,
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMP
)
PARTITION BY DATE(order_ts)
CLUSTER BY order_id, customer_id;

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${BQ_RAW_DS}.order_items` (
    order_item_id STRING NOT NULL,
    order_id STRING,
    sku STRING,
    quantity INT64,
    unit_price NUMERIC,
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMP
)
CLUSTER BY order_id, sku;

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${BQ_RAW_DS}.payments` (
    payment_id STRING NOT NULL,
    order_id STRING,
    provider STRING,
    status STRING,
    authorized_ts TIMESTAMP,
    settled_ts TIMESTAMP,
    amount NUMERIC,
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMP
)
PARTITION BY DATE(authorized_ts)
CLUSTER BY payment_id, order_id;
