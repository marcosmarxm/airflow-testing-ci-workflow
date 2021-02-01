CREATE TABLE IF NOT EXISTS transactions (
    transaction_id   INTEGER,
    purchase_date    TEXT,
    user_id          INTEGER,
    product_id       INTEGER,
    unit_price       REAL,
    quantity        INTEGER,
    total_revenue    REAL
)