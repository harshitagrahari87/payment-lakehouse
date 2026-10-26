"""
glue_gold_job.py — silver -> gold as a real Glue (PySpark) job.

Same contract as src/gold_job.py:
    match payments to invoices -> state machine -> reconciliation_daily


THE FOUR SPARK DECISIONS IN THIS JOB
------------------------------------

1. BROADCAST THE DIMENSION
   dim_region is 5 rows. A shuffle-hash or sort-merge join would move the
   ENTIRE fact table across the network to co-locate keys. broadcast() ships
   the 5-row side to every executor instead, and the join becomes a local
   map-side lookup with zero shuffle.

   Spark auto-broadcasts under spark.sql.autoBroadcastJoinThreshold (10 MB
   default), but only when it can ESTIMATE the size. On a fresh Parquet path
   with no statistics it often cannot, and silently does a sort-merge join
   instead. The hint makes it deterministic.

2. DEDUPLICATE THE SOURCE BEFORE THE JOIN
   A payment with two invoices would fan out into two fact rows and double
   count. Delta raises "multiple source rows matched" on MERGE for exactly
   this reason. The fix is to dedupe the SOURCE, not to clean up afterwards.

3. THE SKEW IS IN THE AGGREGATION, NOT THE JOIN
   payments JOIN invoices is on payment_id — effectively unique, so it
   spreads evenly. But reconciliation_daily groups by (payment_dt, region),
   and India is 39% of volume. THAT shuffle is skewed: one task gets ~4x the
   rows of the others and becomes a straggler.

   Handled here by AQE skew-join/partition splitting. At larger scale the
   next lever is salting the hot key, then aggregating twice (per-salt, then
   combined) — but salting is a last resort because it complicates the code
   and is unnecessary once AQE can split the partition.

4. as_of IS A PARAMETER, NEVER current_date()
   Whether a payment is PENDING or EXPIRED depends on the date you ask.
   current_date() makes a backfill silently rewrite history. The DAG passes
   data_interval_end.


Job parameters:
    --BUCKET   target bucket
    --AS_OF    ISO timestamp the report is computed as of
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "AS_OF"])
BUCKET = args["BUCKET"]
AS_OF = args["AS_OF"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

REGIONS = [
    ("IN", "INR", 7, 0.40),
    ("US", "USD", 3, 0.20),
    ("EU", "EUR", 5, 0.15),
    ("SG", "SGD", 4, 0.15),
    ("AE", "AED", 10, 0.10),
]
STATES = ["MATCHED", "AMOUNT_MISMATCH", "PENDING", "EXPIRED"]

as_of_col = F.to_timestamp(F.lit(AS_OF))

# ---------------------------------------------------------------------
# READ
# ---------------------------------------------------------------------
payments = spark.read.parquet(f"s3://{BUCKET}/silver/payments/")
invoices = spark.read.parquet(f"s3://{BUCKET}/silver/invoices/")
quarantine = spark.read.parquet(f"s3://{BUCKET}/silver/quarantine/")

dim_region = spark.createDataFrame(
    REGIONS, ["region", "currency_std", "retry_window_days", "expected_volume_share"]
)

print(f"as_of    : {AS_OF}")
print(f"payments : {payments.count():,}")
print(f"invoices : {invoices.count():,}")

# ---------------------------------------------------------------------
# 1. One invoice per payment — BEFORE the join. See docstring note 2.
# ---------------------------------------------------------------------
first_invoice = Window.partitionBy("payment_id").orderBy(F.col("event_ts").asc())

inv = (
    invoices.withColumn("rn", F.row_number().over(first_invoice))
            .filter(F.col("rn") == 1)
            .select(
                "payment_id",
                "invoice_id",
                F.col("amount").alias("invoice_amount"),
                F.col("event_ts").alias("invoice_ts"),
            )
)

# ---------------------------------------------------------------------
# 2. The match. Columns selected EXPLICITLY — silver emits payments and
#    invoices from the same union of columns, so payments carries an
#    all-null invoice_id. Inheriting it produces duplicate column names
#    after the join. (failures.md: the gold merge bug.)
# ---------------------------------------------------------------------
pay = payments.select(
    "payment_id", "customer_id", "region", "currency",
    "amount", "payment_method", F.col("event_ts").alias("payment_ts_raw"),
)

matched = pay.join(inv, on="payment_id", how="left")

# dim_region is 5 rows — broadcast it. See docstring note 1.
matched = matched.join(F.broadcast(dim_region), on="region", how="left")

# ---------------------------------------------------------------------
# 3. The state machine.
# ---------------------------------------------------------------------
fct = (
    matched
    .withColumn("payment_ts", F.to_timestamp("payment_ts_raw"))
    .withColumn("payment_dt", F.to_date("payment_ts"))
    .withColumn("days_waiting", F.datediff(as_of_col, F.col("payment_ts")))
    .withColumn("has_invoice", F.col("invoice_id").isNotNull())
    # Floating point: never compare amounts with ==. A tolerance of half a
    # minor unit is the correct test for money stored as a double.
    .withColumn(
        "amounts_agree",
        F.col("has_invoice") & (F.abs(F.col("invoice_amount") - F.col("amount")) < F.lit(0.005)),
    )
    .withColumn(
        "state",
        F.when(F.col("amounts_agree"), F.lit("MATCHED"))
         .when(F.col("has_invoice"), F.lit("AMOUNT_MISMATCH"))
         .when(F.col("days_waiting") > F.col("retry_window_days"), F.lit("EXPIRED"))
         .otherwise(F.lit("PENDING")),
    )
    .withColumn(
        "amount_variance",
        F.when(F.col("has_invoice"), F.col("invoice_amount") - F.col("amount")),
    )
    .withColumn("as_of", F.lit(AS_OF))
    .select(
        "payment_id", "customer_id", "region", "currency", "amount",
        "payment_method", "payment_ts", "payment_dt",
        "invoice_id", "invoice_amount", "invoice_ts",
        "amount_variance", "days_waiting", "retry_window_days",
        "state", "as_of",
    )
    .cache()
)

# ---------------------------------------------------------------------
# 4. reconciliation_daily — grain: one row per payment_dt per region.
#
#    THIS is the skewed shuffle. India is 39% of volume, so its task gets
#    roughly 4x the rows. AQE splits that partition at runtime; without AQE
#    it is a classic straggler — 199 tasks finishing in seconds and one
#    running for minutes while everything waits at the barrier.
# ---------------------------------------------------------------------
recon = (
    fct.groupBy("payment_dt", "region")
       .agg(
           *[F.sum(F.when(F.col("state") == s, 1).otherwise(0)).alias(s.lower())
             for s in STATES],
           F.count("*").alias("total_payments"),
           F.sum("amount").alias("total_amount"),
       )
)

# ---------------------------------------------------------------------
# WRITE. coalesce(1) because these outputs are small — a few thousand rows
# across 200 files would be the small-files problem in miniature.
# ---------------------------------------------------------------------
fct.coalesce(1).write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/fct_payment_state/")
dim_region.coalesce(1).write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/dim_region/")
recon.coalesce(1).write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/reconciliation_daily/")

# ---------------------------------------------------------------------
# CONTROL TOTAL — the gate. Fail loudly rather than publish wrong numbers.
# ---------------------------------------------------------------------
counts = {r["state"]: r["n"] for r in fct.groupBy("state").agg(F.count("*").alias("n")).collect()}
rejected = quarantine.count()
received = fct.count() + rejected
accounted = sum(counts.get(s, 0) for s in STATES) + rejected

print(f"\n--- payment state as of {AS_OF} ---")
for state in STATES:
    print(f"  {state:<16}: {counts.get(state, 0):>6,}")
print(f"  {'REJECTED':<16}: {rejected:>6,}")
print(f"  {'received':<16}: {received:>6,}")

if accounted != received:
    raise RuntimeError(f"CONTROL TOTAL BROKEN: {accounted} != {received}")
print("  control total balances : True")

unexpected = set(counts) - set(STATES)
if unexpected:
    raise RuntimeError(f"unexpected states in gold: {unexpected}")

fct.unpersist()
job.commit()
