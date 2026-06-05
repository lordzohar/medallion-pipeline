CREATE TABLE IF NOT EXISTS customers_cleaned (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone_normalized VARCHAR(32),
    country VARCHAR(8),
    order_amount NUMERIC(12, 2),
    duplicate_count INTEGER,
    processed_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS enriched_orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id VARCHAR(32),
    product_name VARCHAR(255),
    category VARCHAR(64),
    status VARCHAR(32),
    amount NUMERIC(12, 2),
    amount_with_tax NUMERIC(12, 2),
    discount_amount NUMERIC(12, 2),
    net_amount NUMERIC(12, 2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stream_analytics (
    customer_id INTEGER,
    order_count INTEGER,
    total_amount NUMERIC(12, 2),
    average_amount NUMERIC(12, 2),
    window_closed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hop_processing_log (
    log_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255),
    execution_status VARCHAR(50),
    records_processed INTEGER,
    records_error INTEGER,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    error_message TEXT
);
