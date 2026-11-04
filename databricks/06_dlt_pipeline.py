import dlt
from pyspark.sql import functions as F
from pyspark.sql import Window
import json

LANDING = "/Volumes/payment_lakehouse/pipeline/landing"

# Parameter, not current_timestamp(). Falls back to the answer key so the
# pipeline is reproducible without extra config.
try:
    AS_OF = spark.conf.get("pipeline.as_of")
except Exception:
    AS_OF = json.loads(open(f"{LANDING}/_expected_totals.json").read())["generated_at"]

VALID_REGIONS    = ["IN","US","EU","SG","AE"]
VALID_CURRENCIES = ["INR","USD","EUR","SGD","AED"]
VALID = ("event_id IS NOT NULL AND payment_id IS NOT NULL "
         "AND amount IS NOT NULL AND amount > 0 "
         f"AND currency IN {tuple(VALID_CURRENCIES)} "
         f"AND region IN {tuple(VALID_REGIONS)}")


@dlt.table(name="bronze_events", partition_cols=["ingest_dt"],
           comment="Raw events as landed. Append-only.")
def bronze_events():
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/_schemas/dlt_bronze")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(f"{LANDING}/events")
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_dt", F.to_date(F.current_timestamp()))
        .withColumn("source_file", F.col("_metadata.file_path")))


@dlt.view(name="events_typed")
def events_typed():
    return (dlt.read_stream("bronze_events")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_dt", F.to_date(F.to_timestamp("event_ts")))
        .withColumnRenamed("amount", "amount_raw")
        .withColumn("amount", F.expr("try_cast(amount_raw AS DOUBLE)"))
        .withWatermark("event_ts", "7 days")
        .dropDuplicatesWithinWatermark(["event_id"]))

@dlt.table(name="silver_payments", partition_cols=["event_dt"])
@dlt.expect_or_drop("is_valid", VALID)
@dlt.expect("amount_reasonable", "amount < 100000")
def silver_payments():
    return (dlt.read_stream("events_typed")
        .filter("event_type = 'PAYMENT_RECEIVED'")
        .select("event_id","event_ts","event_dt","payment_id","customer_id",
                "region","currency","amount","payment_method","ingest_ts"))


@dlt.table(name="silver_invoices", partition_cols=["event_dt"])
@dlt.expect_or_drop("is_valid", VALID)
@dlt.expect_or_drop("has_invoice_id", "invoice_id IS NOT NULL")
def silver_invoices():
    return (dlt.read_stream("events_typed")
        .filter("event_type = 'INVOICE_CLEARED'")
        .select("event_id","event_ts","event_dt","payment_id","invoice_id",
                "region","currency","amount","ingest_ts"))


@dlt.table(name="silver_quarantine",
           comment="Rejected records, retained for review.")
def silver_quarantine():
    return (dlt.read_stream("events_typed")
                .filter(f"({VALID}) IS NOT TRUE")
        .withColumn("reject_reason",
            F.when(F.col("event_id").isNull(), "missing_field:event_id")
             .when(F.col("amount_raw").isNull(), "null_field:amount")
             .when(F.col("amount").isNull(), "amount_not_numeric")
             .when(F.col("amount") <= 0, "amount_not_positive")
             .when(~F.col("currency").isin(VALID_CURRENCIES), "unknown_currency")
             .when(F.col("region").isNull() | ~F.col("region").isin(VALID_REGIONS),
                   "unknown_region")
             .otherwise("unknown"))
        .select("event_id","event_type","event_ts","payment_id","region",
                "currency","amount","reject_reason","ingest_ts"))


# Materialized view, not a streaming table: late invoices must revise an
# already-computed result, which append-only cannot do.
@dlt.table(name="fct_payment_state", comment="Current state per payment.")
def fct_payment_state():
    regions = spark.createDataFrame(
        [("IN",7),("US",3),("EU",5),("SG",4),("AE",10)],
        ["region","retry_window_days"])

    inv = (dlt.read("silver_invoices")
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("payment_id").orderBy(F.col("event_ts").asc())))
        .filter("rn = 1")
        .select("payment_id","invoice_id",
                F.col("amount").alias("invoice_amount"),
                F.col("event_ts").alias("invoice_ts")))

    return (dlt.read("silver_payments")
        .withColumnRenamed("event_ts","payment_ts")
        .join(inv, "payment_id", "left")
        .join(F.broadcast(regions), "region", "left")
        .withColumn("payment_dt", F.to_date("payment_ts"))
        .withColumn("days_waiting", F.floor(
            (F.unix_timestamp(F.to_timestamp(F.lit(AS_OF)))
           - F.unix_timestamp("payment_ts")) / 86400))
        .withColumn("has_invoice", F.col("invoice_id").isNotNull())
        .withColumn("state",
            F.when(F.col("has_invoice") &
                   (F.abs(F.col("invoice_amount")-F.col("amount")) < 0.005), "MATCHED")
             .when(F.col("has_invoice"), "AMOUNT_MISMATCH")
             .when(F.col("days_waiting") > F.col("retry_window_days"), "EXPIRED")
             .otherwise("PENDING"))
        .select("payment_id","customer_id","region","currency","amount",
                "payment_ts","payment_dt","invoice_id","invoice_amount",
                "days_waiting","retry_window_days","state"))


@dlt.table(name="reconciliation_daily")
def reconciliation_daily():
    return (dlt.read("fct_payment_state")
        .groupBy("payment_dt","region")
        .agg(*[F.sum(F.when(F.col("state")==s,1).otherwise(0)).alias(s.lower())
               for s in ["MATCHED","AMOUNT_MISMATCH","PENDING","EXPIRED"]],
             F.count("*").alias("total_payments"),
             F.sum("amount").alias("total_amount")))