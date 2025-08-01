CREATE SCHEMA IF NOT EXISTS ods;
CREATE TABLE IF NOT EXISTS ods.sales_clean (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE,
    hour_of_day INT,
    cash_type TEXT,
    card TEXT,
    money NUMERIC(10,2),
    coffee_name TEXT,
    time_of_day TEXT,
    weekday TEXT,
    month_name TEXT,
    weekdaysort INT,
    monthsort INT,
    source TEXT,
    load_dttm TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sales_clean_date ON ods.sales_clean(load_dttm);