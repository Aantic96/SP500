CREATE TABLE IF NOT EXISTS stock_name (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS price_data (
    stock_id INT REFERENCES stock_name(id),
    date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    CONSTRAINT price_data_unique UNIQUE (stock_id, date)
);