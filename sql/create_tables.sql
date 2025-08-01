CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS stg.sales_raw (
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
    load_dttm TIMESTAMP DEFAULT NOW(),
    source TEXT
);

CREATE TABLE IF NOT EXISTS ods.sales_clean (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE,
    hour_of_day INT,
    cash_type TEXT,
    card TEXT,
    money NUMERIC(10,2),
    coffee_name TEXT,
    time_of_day TEXT,
    weekdaysort INT,
    monthsort INT,
    source TEXT,
    load_dttm TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dm.daily_sales (
    report_date DATE PRIMARY KEY,
    revenue NUMERIC(12,2),
    avg_check NUMERIC(10,2),
    units_sold INT,
    top_hour INT,
    load_dttm TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sales_clean_date ON stg.sales_raw(load_dttm);
CREATE INDEX IF NOT EXISTS idx_sales_clean_date ON ods.sales_clean(load_dttm);