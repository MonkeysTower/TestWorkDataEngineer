CREATE SCHEMA IF NOT EXISTS dm;
DROP TABLE IF EXISTS dm.daily_sales;
CREATE TABLE dm.daily_sales (
    report_date DATE PRIMARY KEY,
    revenue NUMERIC(12,2),
    avg_check NUMERIC(10,2),
    units_sold INT,
    top_hour INT,
    load_dttm TIMESTAMP DEFAULT NOW()
);