"""
bronze -> silver, as a Glue (PySpark) job.

Same contract as src/silver_job.py: parse, validate, quarantine bad rows,
deduplicate on event_id, and repartition by event date. That version runs on
pandas and is the one used for local development; this one is what runs at
production volume.

At the current data size this job is slower than the pandas version — JVM
startup and shuffle cost exceed the work. It is sized for the stated target of
~2M payments/day.

Job parameters:
    --BUCKET
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

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Without dynamic mode, overwrite replaces the entire table directory rather
# than only the partitions present in this batch.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Schemas are declared, not inferred: inference costs an extra pass and the
# type it picks depends on which values that pass happened to see.
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

bronze = spark.read.schema(BRONZE_SCHEMA).json(f"s3://{BUCKET}/bronze/")
print(f"bronze records read : {bronze.count():,}")

# from_json yields NULL on unparseable input rather than raising, which is what
# allows a bad record to be quarantined instead of failing the job.
parsed = bronze.withColumn("e", F.from_json(F.col("raw"), EVENT_SCHEMA))

# First matching condition wins, so the cheapest and most fundamental checks
# come first.
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

# Cached because the good and bad branches each consume it; without this the
# read, parse and validation chain is evaluated twice.
flagged = parsed.withColumn("reject_reason", reject_reason).cache()

# Note: a non-numeric string in a DoubleType field becomes NULL during
# from_json, so it is labelled null_field:amount here where the pandas version
# reports amount_not_numeric. Same records rejected, different reason string.

good = flagged.filter(F.col("reject_reason").isNull())
bad = flagged.filter(F.col("reject_reason").isNotNull())

# row_number rather than dropDuplicates: the first arrival has to win, and
# dropDuplicates gives no control over which row survives.
dedup_window = Window.partitionBy("e.event_id").orderBy(F.col("ingest_ts").asc())
deduped = (
    good.withColumn("rn", F.row_number().over(dedup_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
)

# Quarantine needs the same guarantee; a redelivered bad record would otherwise
# inflate the rejected count and break the daily reconciliation.
quarantine_key = F.coalesce(F.col("e.event_id"), F.col("raw"))
quarantine_window = Window.partitionBy(quarantine_key).orderBy(F.col("ingest_ts").asc())
bad_deduped = (
    bad.withColumn("rn", F.row_number().over(quarantine_window))
       .filter(F.col("rn") == 1)
       .drop("rn")
)

# Bronze is partitioned by arrival; silver is partitioned by when the event
# actually happened.
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

# repartition by the partition column first, otherwise every task writes a file
# into every partition and the output is thousands of small files.
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
    .coalesce(1)
    .write.mode("overwrite")
    .parquet(f"s3://{BUCKET}/silver/quarantine/"))

print(f"silver records     : {deduped.count():,}")
print(f"quarantine records : {bad_deduped.count():,}")

flagged.unpersist()
job.commit()
