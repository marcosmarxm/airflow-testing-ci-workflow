GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO root;

CREATE TABLE transactions(
    orderId         INTEGER,
    purchaseDate    TEXT,
    userId          INTEGER,
    product         TEXT,
    category        TEXT,
    price           REAL
);  
