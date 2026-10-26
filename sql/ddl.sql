-- Athena DDL for the payment lakehouse.
--
-- Explicit DDL, not a Glue Crawler. The difference matters and gets asked:
--
--   Crawler       scans the data, INFERS types, creates/updates the table.
--                 Costs DPU-minutes, runs on a schedule, and can silently
--                 change a column type when it meets a new file.
--   Explicit DDL  you declare the contract. Free, deterministic, reviewable
--                 in git. If the data stops matching, the query fails loudly
--                 instead of the schema quietly moving.
--
-- For a pipeline you own end to end, declare the schema. Use a crawler for
-- data you do not control.
--
-- __BUCKET__ is substituted by infra/athena.sh.

CREATE DATABASE IF NOT EXISTS payment_lakehouse;

-- ---------------------------------------------------------------------
-- BRONZE — raw JSON, partitioned by ingest date/hour.
-- `raw` stays a string: bronze does not interpret anything.
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- GOLD — the business tables
-- ---------------------------------------------------------------------
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

-- Bronze is PARTITIONED, so Athena does not know its partitions exist until
-- told. Without this, the table returns zero rows and looks broken.
MSCK REPAIR TABLE payment_lakehouse.bronze_events;
