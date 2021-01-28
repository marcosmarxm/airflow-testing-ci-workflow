CREATE TABLE IF NOT EXISTS {tablename} (
    transactionId   INTEGER,
    purchaseDate    TEXT,
    userId          INTEGER,
    productId       INTEGER,
    unitPrice       REAL,
    quantity        INTEGER,
    totalRevenue    REAL,
    productName     TEXT,
    productCategory TEXT
)