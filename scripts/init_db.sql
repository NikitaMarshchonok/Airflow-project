CREATE DATABASE pipeline_db;
\c pipeline_db;

CREATE TABLE IF NOT EXISTS exchange_rates (
    id         SERIAL PRIMARY KEY,
    base       VARCHAR(3) NOT NULL,
    target     VARCHAR(3) NOT NULL,
    rate       NUMERIC(18, 6) NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);