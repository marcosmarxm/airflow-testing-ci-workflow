GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO root;

CREATE TABLE test(
    transaction_id  INTEGER,
    purchase_date   TEXT,
    user            TEXT,
    product         TEXT,
    category        TEXT,
    price           REAL);  
