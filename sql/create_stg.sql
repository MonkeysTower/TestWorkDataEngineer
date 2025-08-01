CREATE SCHEMA IF NOT EXISTS stg;
CREATE TABLE IF NOT EXISTS  stg.sales_raw (
    index_1 TEXT,
    date TEXT,
    datetime TEXT,
    hour_of_day TEXT,
    cash_type TEXT,
    card TEXT,
    money TEXT,
    coffee_name TEXT,
    "Time_of_Day" TEXT,
    "Weekday" TEXT,
    "Month_name" TEXT,
    "Weekdaysort" TEXT,
    "Monthsort" TEXT,
    load_dttm TIMESTAMP,
    source TEXT
);
CREATE INDEX IF NOT EXISTS idx_sales_clean_date ON stg.sales_raw(load_dttm);