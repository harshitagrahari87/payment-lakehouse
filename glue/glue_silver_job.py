"""
glue_silver_job.py — bronze -> silver as a real Glue (PySpark) job.

Same contract as src/silver_job.py, which is the pandas version:
    parse -> validate -> quarantine -> dedup on event_id -> partition by event_dt

The logic is identical. What changes is that every step becomes a distributed
operation, and three of them have costs that do not exist in pandas.


THE THREE THINGS THAT ARE DIFFERENT IN SPARK
--------------------------------------------

1. NARROW vs WIDE
       read · from_json · validate · filter     NARROW — no data movement
       dedup on event_id                        WIDE   — full shuffle
       write partitionBy(event_dt)              WIDE   — repartition

   The parallelism is free. The shuffles are what you tune.

2. THE DEDUP KEY DETERMINES THE SKEW
   event_id is a UUID, so it hashes uniformly and the dedup shuffle spreads
   evenly. There is NO skew in this job, even though the dataset is 39% one
   region — because this job never shuffles on region.

   Skew comes from the key you shuffle on, not from the dataset.

3. PARTITION OVERWRITE MODE
       spark.sql.sources.partitionOverwriteMode = dynamic

   Without this, mode("overwrite") deletes the ENTIRE table directory and
   writes back only the partitions in this batch. Process one day and you
   have just destroyed the other 364. This single setting is the difference
   between an incremental load and an outage.


WHEN THIS JOB IS WORTH IT
-------------------------
At 9,446 records it is SLOWER than pandas — JVM startup and shuffle overhead
exceed the actual work. It earns its keep at the stated production volume of
2M payments/day (~3 GB/day), which does not fit one machine's memory.

Justify Spark by volume, never by preference. Glue's Python-shell job type
exists for small data and costs a fraction of a Spark job.


Job parameters:
    --BUCKET   target bucket
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
BUCKET = args["BUCKET"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Adaptive Query Execution coalesces small post-shuffle partitions and splits
# skewed ones at RUNTIME, from real statistics rather than the optimizer's
# estimates. Default-on in Spark 3.2+, set explicitly so the intent is visible.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# See docstring note 3. Without this, overwrite destroys the whole table.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ---------------------------------------------------------------------
# Schemas declared, never inferred.
#
# inferSchema costs a FULL EXTRA PASS over the data, and worse, the type it
# picks depends on which values it happened to see. A column that is long on
# Monday becomes string on Tuesday because one record arrived with a null.
# Declare the contract.
# ---------------------------------------------------------------------
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("schema_version", IntegerType()),
    StructField("payment_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("invoice_id", StringType()),
    StructField("region", StringType()),
    StructField("currency", StringType()),
    StructField("amount", DoubleType()),
    StructField("payment_method", StringType()),
])

BRONZE_SCHEMA = StructType([
    StructField("batch_id", StringType()),
    StructField("ingest_ts", StringType()),
    StructField("source", StringType()),
    StructField("partition_key", StringType()),
    StructField("sequence_number", StringType()),
    StructField("raw", StringType()),
])

VALID_REGIONS = ["IN", "US", "EU", "SG", "AE"]
VALID_CURRENCIES = ["INR", "USD", "EUR", "SGD", "AED"]

# ---------------------------------------------------------------------
# READ — narrow. One task per file, no shuffle.
# ---------------------------------------------------------------------
bronze = spark.read.schema(BRONZE_SCHEMA).json(f"s3://{BUCKET}/bronze/")
print(f"bronze records read : {bronze.count():,}")

# ---------------------------------------------------------------------
# PARSE — from_json returns NULL for an unparseable string rather than
# throwing. That is what lets a bad record be quarantined instead of
# killing the job.
# ---------------------------------------------------------------------
parsed = bronze.withColumn("e", F.from_json(F.col("raw"), EVENT_SCHEMA))

# ---------------------------------------------------------------------
# VALIDATE — one when/otherwise chain producing a reject_reason.
# The FIRST matching condition wins, so checks run most-fundamental first.
# ---------------------------------------------------------------------
reject_reason = (
    F.when(F.col("e").isNull(), F.lit("unparseable_json"))
     .when(F.col("e.event_id").isNull(), F.lit("missing_field:event_id"))
     .when(~F.col("e.event_type").isin("PAYMENT_RECEIVED", "INVOICE_CLEARED"),
           F.concat(F.lit("unknown_event_type:"), F.col("e.event_type")))
     .when(F.col("e.amount").isNull(), F.lit("null_field:amount"))
     .when(F.col("e.amount") <= 0, F.lit("amount_not_positive"))
     .when(F.col("e.currency").isNull() | ~F.col("e.currency").isin(VALID_CURRENCIES),
           F.concat(F.lit("unknown_currency:"),
                    F.coalesce(F.col("e.currency"), F.lit("null"))))
     .when(F.col("e.region").isNull(), F.lit("missing_field:region"))
     .when(~F.col("e.region").isin(VALID_REGIONS),
           F.concat(F.lit("unknown_region:"), F.col("e.region")))
     .when(F.col("e.payment_id").isNull(), F.lit("missing_field:payment_id"))
     .when((F.col("e.event_type") == "INVOICE_CLEARED") & F.col("e.invoice_id").isNull(),
           F.lit("missing_field:invoice_id"))
     .otherwise(F.lit(None))
)

# cache() because `flagged` is consumed twice (good and bad branches).
# Without it the entire read + parse + validate chain runs twice — Spark is
# lazy, so a DataFrame used N times is COMPUTED N times unless persisted.
flagged = parsed.withColumn("reject_reason", reject_reason).cache()

# NOTE: a string "abc" in a DoubleType field becomes NULL during from_json,
# so it surfaces as null_field:amount rather than amount_not_numeric. The
# pandas version distinguishes them because Python keeps the original type.
# Same records quarantined, different label.

good = flagged.filter(F.col("reject_reason").isNull())
bad = flagged.filter(F.col("reject_reason").isNotNull())

# ---------------------------------------------------------------------
# DEDUP — the only genuinely expensive step. WIDE: a full shuffle on
# event_id so every copy of an id lands on the same executor.
#
# row_number() over a window, keep 1 — the canonical Spark dedup.
# dropDuplicates() exists but gives no control over WHICH row survives;
# here the first arrival must win, so the ordering is explicit.
# ---------------------------------------------------------------------
dedup_window = Window.partitionBy("e.event_id").orderBy(F.col("ingest_ts").asc())

deduped = (
    good.withColumn("rn", F.row_number().over(dedup_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
)

# Quarantine gets the same treatment. A redelivered bad record is still a
# duplicate, and double-counting it corrupts the rejected total.
# Regression guard for notes/failures.md #3.
quarantine_key = F.coalesce(F.col("e.event_id"), F.col("raw"))
quarantine_window = Window.partitionBy(quarantine_key).orderBy(F.col("ingest_ts").asc())

bad_deduped = (
    bad.withColumn("rn", F.row_number().over(quarantine_window))
       .filter(F.col("rn") == 1)
       .drop("rn")
)

# ---------------------------------------------------------------------
# PROJECT — flatten, and derive event_dt as the WRITE partition.
# Bronze was partitioned by ingest date; silver is partitioned by when the
# event actually happened.
# ---------------------------------------------------------------------
silver = (
    deduped.select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.event_type").alias("event_type"),
        F.col("e.event_ts").alias("event_ts"),
        F.col("e.payment_id").alias("payment_id"),
        F.col("e.customer_id").alias("customer_id"),
        F.col("e.invoice_id").alias("invoice_id"),
        F.col("e.region").alias("region"),
        F.col("e.currency").alias("currency"),
        F.col("e.amount").alias("amount"),
        F.col("e.payment_method").alias("payment_method"),
        F.col("batch_id"),
        F.col("ingest_ts"),
    )
    .withColumn("event_dt", F.to_date(F.to_timestamp("event_ts")))
)

payments = silver.filter(F.col("event_type") == "PAYMENT_RECEIVED")
invoices = silver.filter(F.col("event_type") == "INVOICE_CLEARED")

# ---------------------------------------------------------------------
# WRITE.
#
# repartition("event_dt") BEFORE partitionBy avoids the small-files problem.
# Without it every task writes a file into every partition — 200 tasks x 13
# dates = 2,600 tiny files. Shuffling by the partition column first gives one
# writer per date.
#
# Small files are the most common self-inflicted lakehouse problem: per-file
# open cost dominates once files fall well below ~128 MB.
# ---------------------------------------------------------------------
for name, frame in (("payments", payments), ("invoices", invoices)):
    (frame.repartition("event_dt")
          .write.mode("overwrite")
          .partitionBy("event_dt")
          .parquet(f"s3://{BUCKET}/silver/{name}/"))
    print(f"wrote silver/{name}")

(bad_deduped.select(
        F.col("e.event_id").alias("event_id"),
        "batch_id", "ingest_ts", "reject_reason", "raw",
    )
    .coalesce(1)                      # quarantine is tiny; one file is right
    .write.mode("overwrite")
    .parquet(f"s3://{BUCKET}/silver/quarantine/"))

print(f"silver records     : {deduped.count():,}")
print(f"quarantine records : {bad_deduped.count():,}")

flagged.unpersist()
job.commit()
