CREATE TABLE IF NOT EXISTS products_sales (
    transaction_id   INTEGER,
    purchase_date    TEXT,
    user_id          INTEGER,
    product_id       INTEGER,
    unit_price       REAL,
    quantity        INTEGER,
    total_revenue    REAL,
    product_name     TEXT,
    product_category TEXT
)