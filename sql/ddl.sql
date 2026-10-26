-- Athena external table definitions.
--
-- Declared rather than crawled. A crawler infers types from whatever files it
-- happens to see, which means a column can change type between runs; explicit
-- DDL keeps the schema in version control and fails loudly when the data stops
-- matching it. Crawlers are the better fit for data you do not control.
--
-- __BUCKET__ is substituted by src/athena.py.

CREATE DATABASE IF NOT EXISTS payment_lakehouse;

-- Bronze: raw JSON. `raw` stays a string because bronze does not interpret
-- anything.
CREATE EXTERNAL TABLE IF NOT EXISTS payment_lakehouse.bronze_events (
    batch_id       string,
    ingest_ts      string,
    source         string,
    partition_key  string,
    raw            string
)
PARTITIONED BY (ingest_dt string, hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://__BUCKET__/bronze/';

CREATE EXTERNAL TABLE IF NOT EXISTS payment_lakehouse.fct_payment_state (
    payment_id         string,
    customer_id        string,
    region             string,
    currency           string,
    amount             double,
    payment_method     string,
    payment_ts         timestamp,
    payment_dt         string,
    invoice_id         string,
    invoice_amount     double,
    invoice_ts         string,
    amount_variance    double,
    days_waiting       bigint,
    retry_window_days  bigint,
    state              string,
    as_of              string
)
STORED AS PARQUET
LOCATION 's3://__BUCKET__/gold/fct_payment_state/';

CREATE EXTERNAL TABLE IF NOT EXISTS payment_lakehouse.dim_region (
    region                 string,
    currency               string,
    retry_window_days      bigint,
    expected_volume_share  double
)
STORED AS PARQUET
LOCATION 's3://__BUCKET__/gold/dim_region/';

CREATE EXTERNAL TABLE IF NOT EXISTS payment_lakehouse.reconciliation_daily (
    payment_dt       string,
    region           string,
    amount_mismatch  bigint,
    expired          bigint,
    matched          bigint,
    pending          bigint,
    total_payments   bigint,
    total_amount     double
)
STORED AS PARQUET
LOCATION 's3://__BUCKET__/gold/reconciliation_daily/';

-- Partitions are not discovered automatically. Without this the table parses
-- but returns nothing.
MSCK REPAIR TABLE payment_lakehouse.bronze_events;
