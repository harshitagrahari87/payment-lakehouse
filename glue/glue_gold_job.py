"""
silver -> gold, as a Glue (PySpark) job.

Matches payments to invoices, applies the per-region retry window, and writes
fct_payment_state, dim_region and reconciliation_daily. Mirrors
src/gold_job.py, which is the pandas implementation used locally.

Job parameters:
    --BUCKET
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

# Passed in rather than derived from current_date(), so a rerun of an earlier
# interval reproduces that interval's numbers instead of today's.
as_of_col = F.to_timestamp(F.lit(AS_OF))

payments = spark.read.parquet(f"s3://{BUCKET}/silver/payments/")
invoices = spark.read.parquet(f"s3://{BUCKET}/silver/invoices/")
quarantine = spark.read.parquet(f"s3://{BUCKET}/silver/quarantine/")

dim_region = spark.createDataFrame(
    REGIONS, ["region", "currency_std", "retry_window_days", "expected_volume_share"]
)

print(f"as_of    : {AS_OF}")
print(f"payments : {payments.count():,}")
print(f"invoices : {invoices.count():,}")

# One invoice per payment, enforced before the join. Two invoices for the same
# payment would otherwise fan it into two fact rows and double-count it.
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

# Columns selected explicitly. Silver emits payments and invoices from the same
# union of fields, so the payments frame carries an all-null invoice_id that
# collides with the joined one.
pay = payments.select(
    "payment_id", "customer_id", "region", "currency",
    "amount", "payment_method", F.col("event_ts").alias("payment_ts_raw"),
)

matched = pay.join(inv, on="payment_id", how="left")

# dim_region is five rows. Broadcasting avoids shuffling the fact table, and
# the hint is explicit because Spark cannot always estimate the size of a
# freshly written Parquet path.
matched = matched.join(F.broadcast(dim_region), on="region", how="left")

fct = (
    matched
    .withColumn("payment_ts", F.to_timestamp("payment_ts_raw"))
    .withColumn("payment_dt", F.to_date("payment_ts"))
    .withColumn("days_waiting", F.datediff(as_of_col, F.col("payment_ts")))
    .withColumn("has_invoice", F.col("invoice_id").isNotNull())
    # Tolerance rather than equality: these are doubles.
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

# Grain: one row per payment date per region. This grouping is the skewed one —
# the join key is near-unique, but one region carries ~40% of volume. AQE splits
# the oversized partition at runtime.
recon = (
    fct.groupBy("payment_dt", "region")
       .agg(
           *[F.sum(F.when(F.col("state") == s, 1).otherwise(0)).alias(s.lower())
             for s in STATES],
           F.count("*").alias("total_payments"),
           F.sum("amount").alias("total_amount"),
       )
)

# Outputs are small enough that a single file each is the right shape.
fct.coalesce(1).write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/fct_payment_state/")
dim_region.coalesce(1).write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/dim_region/")
recon.coalesce(1).write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/reconciliation_daily/")

counts = {r["state"]: r["n"] for r in fct.groupBy("state").agg(F.count("*").alias("n")).collect()}
rejected = quarantine.count()
received = fct.count() + rejected
accounted = sum(counts.get(s, 0) for s in STATES) + rejected

print(f"\n--- payment state as of {AS_OF} ---")
for state in STATES:
    print(f"  {state:<16}: {counts.get(state, 0):>6,}")
print(f"  {'REJECTED':<16}: {rejected:>6,}")
print(f"  {'received':<16}: {received:>6,}")

# Fail the job rather than publish numbers that do not add up.
if accounted != received:
    raise RuntimeError(f"CONTROL TOTAL BROKEN: {accounted} != {received}")
print("  control total balances : True")

unexpected = set(counts) - set(STATES)
if unexpected:
    raise RuntimeError(f"unexpected states in gold: {unexpected}")

fct.unpersist()
job.commit()
