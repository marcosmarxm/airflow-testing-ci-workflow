CREATE TABLE IF NOT EXISTS transactions(
    orderId         INTEGER,
    purchaseDate    TEXT,
    userId          INTEGER,
    product         TEXT,
    category        TEXT,
    price           REAL
)