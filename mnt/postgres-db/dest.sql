DROP USER IF EXISTS airflow;
CREATE USER airflow PASSWORD airflow;

DROP DATABASE IF EXISTS dest;
CREATE DATABASE dest;
\c dest;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA PUBLIC TO airflow;

CREATE TABLE table_dest (
    year_month               INTEGER NOT NULL,
    some_id                  INTEGER NOT NULL,
    user_name                STRING,
    value                    REAL 
);  
