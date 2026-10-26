"""
silver_job.py — bronze -> silver.

This is the Glue job's logic, written in pandas so it runs without a cluster.
Every operation here has a one-to-one Spark equivalent, noted inline.


WHAT SILVER IS
--------------
    bronze   raw strings, append-only, partitioned by INGEST date
    silver   parsed, typed, validated, DEDUPED, partitioned by EVENT date
    gold     business state and aggregates

Silver is where a bad record is allowed to fail, because failure here is
contained to one row. In bronze it would break ingestion itself.


THE THREE OPERATIONS
--------------------
1. PARSE + VALIDATE   bad rows go to quarantine, never dropped.
                      In finance you do not silently discard a transaction.

2. DEDUP on event_id  keep the FIRST arrival by ingest_ts.
                      Dedup is on event_id, NOT payment_id. A redelivery
                      repeats event_id; a correction reuses payment_id with
                      a new event_id and must survive.

   Spark:
       w = Window.partitionBy("event_id").orderBy("ingest_ts")
       df.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")

3. PARTITION by event_dt, not ingest_dt.
   Bronze is read forward-only by arrival; silver is written by when the
   event actually happened. That is what lets a 6-day-late invoice land in
   its true partition without reprocessing all of bronze.

   Spark:
       df.write.partitionBy("event_dt").mode("append").format("delta").save(...)


Usage:
    python src/silver_job.py
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CONF_PATH = ROOT / "conf" / "config.json"
DATA_DIR = ROOT / "data"

REQUIRED_COMMON = ["event_id", "event_type", "event_ts", "region", "currency", "amount"]
VALID_EVENT_TYPES = {"PAYMENT_RECEIVED", "INVOICE_CLEARED"}


def load_config(path: Path = CONF_PATH) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# --------------------------------------------------------------------------
# read bronze
# --------------------------------------------------------------------------

def read_bronze(bronze_root: Path) -> pd.DataFrame:
    """
    Reads every bronze object. Each line is the envelope written by the
    consumer: provenance columns plus the untouched original message.

    Spark: spark.read.json(f"{bronze}/ingest_dt=*/hour=*/")
    """
    rows = []
    for path in sorted(bronze_root.rglob("*.json")):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    if not rows:
        raise SystemExit(f"no bronze files under {bronze_root} — run bronze_writer.py first")
    return pd.DataFrame(rows)


def read_bronze_s3(cfg: dict, prefix: str = "bronze/") -> pd.DataFrame:
    """
    Same read, against S3. A paginator is required because list_objects_v2
    returns at most 1000 keys per call — a lakehouse outgrows that quickly
    and the silent truncation is a classic bug.
    """
    import boto3

    s3 = boto3.client("s3", region_name=cfg["region"])
    bucket = cfg["bucket"]

    rows = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".json"):
                continue
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode("utf-8")
            for line in body.splitlines():
                line = line.strip()
                if line:
                    rows.append(json.loads(line))

    if not rows:
        raise SystemExit(f"no bronze objects under s3://{bucket}/{prefix}")
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# parse + validate
# --------------------------------------------------------------------------

def validate(payload: dict, valid_regions: set[str], valid_currencies: set[str]) -> str | None:
    """Returns a rejection reason, or None if the record is good."""
    for field in REQUIRED_COMMON:
        if field not in payload:
            return f"missing_field:{field}"
        if payload[field] is None:
            return f"null_field:{field}"

    if payload["event_type"] not in VALID_EVENT_TYPES:
        return f"unknown_event_type:{payload['event_type']}"

    amount = payload["amount"]
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        return "amount_not_numeric"
    if amount <= 0:
        return "amount_not_positive"

    if payload["currency"] not in valid_currencies:
        return f"unknown_currency:{payload['currency']}"
    if payload["region"] not in valid_regions:
        return f"unknown_region:{payload['region']}"

    if payload["event_type"] == "PAYMENT_RECEIVED" and not payload.get("payment_id"):
        return "missing_field:payment_id"
    if payload["event_type"] == "INVOICE_CLEARED":
        if not payload.get("payment_id"):
            return "missing_field:payment_id"
        if not payload.get("invoice_id"):
            return "missing_field:invoice_id"

    return None


def parse_and_validate(bronze: pd.DataFrame, cfg: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid_regions = {r["code"] for r in cfg["regions"]}
    valid_currencies = {r["currency"] for r in cfg["regions"]}

    good, bad = [], []

    for row in bronze.to_dict("records"):
        try:
            payload = json.loads(row["raw"])
        except json.JSONDecodeError:
            bad.append({**row, "reject_reason": "unparseable_json"})
            continue

        reason = validate(payload, valid_regions, valid_currencies)
        if reason:
            # event_id is kept where available so a quarantined row can be
            # traced back to its bronze object and reprocessed after a fix.
            bad.append({
                "event_id": payload.get("event_id"),
                "batch_id": row["batch_id"],
                "ingest_ts": row["ingest_ts"],
                "reject_reason": reason,
                "raw": row["raw"],
            })
            continue

        good.append({
            **payload,
            "batch_id": row["batch_id"],
            "ingest_ts": row["ingest_ts"],
        })

    return pd.DataFrame(good), pd.DataFrame(bad)


# --------------------------------------------------------------------------
# dedup
# --------------------------------------------------------------------------

def dedup(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Keep the first arrival per event_id.

    Spark:
        w = Window.partitionBy("event_id").orderBy("ingest_ts")
        df.withColumn("rn", row_number().over(w)).filter("rn = 1")
    """
    before = len(df)
    deduped = (
        df.sort_values(["event_id", "ingest_ts"])
          .drop_duplicates(subset=["event_id"], keep="first")
          .reset_index(drop=True)
    )
    return deduped, before - len(deduped)


def dedup_quarantine(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Quarantine needs the SAME idempotency guarantee as the main path.
    A redelivered bad record is still a duplicate, and double-counting it
    corrupts the rejected total in the daily reconciliation.

    Unparseable records have no event_id, so fall back to the raw bytes.
    """
    if df.empty:
        return df, 0

    before = len(df)
    key = df["event_id"].fillna("")
    key = key.where(key != "", df["raw"])

    deduped = (
        df.assign(_dedup_key=key)
          .sort_values(["_dedup_key", "ingest_ts"])
          .drop_duplicates(subset=["_dedup_key"], keep="first")
          .drop(columns=["_dedup_key"])
          .reset_index(drop=True)
    )
    return deduped, before - len(deduped)


# --------------------------------------------------------------------------
# write
# --------------------------------------------------------------------------

def write_partitioned(df: pd.DataFrame, out_root: Path, name: str) -> int:
    """Hive-style partition layout: the same paths Glue/Athena expect."""
    if df.empty:
        return 0
    written = 0
    for event_dt, part in df.groupby("event_dt"):
        target = out_root / name / f"event_dt={event_dt}"
        target.mkdir(parents=True, exist_ok=True)
        part.drop(columns=["event_dt"]).to_parquet(
            target / "part-000.parquet", index=False
        )
        written += 1
    return written


def write_partitioned_s3(df: pd.DataFrame, cfg: dict, name: str) -> int:
    """
    Same layout on S3. The key is deterministic (silver/<name>/event_dt=X/
    part-000.parquet) so a re-run REPLACES the partition rather than adding
    a second file — the same idempotency rule bronze uses.
    """
    import io

    import boto3

    if df.empty:
        return 0

    s3 = boto3.client("s3", region_name=cfg["region"])
    written = 0

    for event_dt, part in df.groupby("event_dt"):
        buf = io.BytesIO()
        part.drop(columns=["event_dt"]).to_parquet(buf, index=False)
        s3.put_object(
            Bucket=cfg["bucket"],
            Key=f"silver/{name}/event_dt={event_dt}/part-000.parquet",
            Body=buf.getvalue(),
        )
        written += 1
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="bronze -> silver")
    parser.add_argument("--bronze", type=Path, default=DATA_DIR / "bronze")
    parser.add_argument("--out", type=Path, default=DATA_DIR / "silver")
    parser.add_argument("--source", choices=["local", "s3"], default="local")
    parser.add_argument("--sink", choices=["local", "s3"], default="local")
    args = parser.parse_args()

    cfg = load_config()

    if args.source == "s3":
        bronze = read_bronze_s3(cfg)
        print(f"source                     : s3://{cfg['bucket']}/bronze/")
    else:
        bronze = read_bronze(args.bronze)
        print(f"source                     : {args.bronze}")
    print(f"bronze records read        : {len(bronze):,}")

    good, bad = parse_and_validate(bronze, cfg)
    print(f"parsed ok                  : {len(good):,}")
    print(f"quarantined (raw)          : {len(bad):,}")

    good, dup_good = dedup(good)
    bad, dup_bad = dedup_quarantine(bad)
    print(f"duplicates removed (good)  : {dup_good:,}")
    print(f"duplicates removed (bad)   : {dup_bad:,}")
    print(f"silver records             : {len(good):,}")
    print(f"quarantine records         : {len(bad):,}")

    good["event_dt"] = pd.to_datetime(good["event_ts"]).dt.strftime("%Y-%m-%d")

    payments = good[good["event_type"] == "PAYMENT_RECEIVED"].copy()
    invoices = good[good["event_type"] == "INVOICE_CLEARED"].copy()

    if args.sink == "s3":
        p_parts = write_partitioned_s3(payments, cfg, "payments")
        i_parts = write_partitioned_s3(invoices, cfg, "invoices")
        if not bad.empty:
            import io

            import boto3

            buf = io.BytesIO()
            bad.to_parquet(buf, index=False)
            boto3.client("s3", region_name=cfg["region"]).put_object(
                Bucket=cfg["bucket"],
                Key="silver/quarantine/part-000.parquet",
                Body=buf.getvalue(),
            )
        target = f"s3://{cfg['bucket']}/silver/"
    else:
        p_parts = write_partitioned(payments, args.out, "payments")
        i_parts = write_partitioned(invoices, args.out, "invoices")
        if not bad.empty:
            q_dir = args.out / "quarantine"
            q_dir.mkdir(parents=True, exist_ok=True)
            bad.to_parquet(q_dir / "part-000.parquet", index=False)
        target = str(args.out)

    print(f"\nsink      : {target}")
    print(f"payments  : {len(payments):,} rows across {p_parts} event_dt partitions")
    print(f"invoices  : {len(invoices):,} rows across {i_parts} event_dt partitions")

    if not bad.empty:
        print("\nquarantine by reason:")
        for reason, n in bad["reject_reason"].value_counts().items():
            print(f"  {reason}: {n}")

    # ---- reconcile against the producer's answer key -------------------
    truth_path = DATA_DIR / "_expected_totals.json"
    if truth_path.exists():
        truth = json.loads(truth_path.read_text(encoding="utf-8"))
        expected_unique = truth["events_emitted"] - truth["duplicate_events_injected"]
        actual_unique = len(good) + len(bad)

        print("\n--- reconciliation vs producer answer key ---")
        print(f"  expected unique events : {expected_unique:,}")
        print(f"  actual   unique events : {actual_unique:,}")
        print(f"  match                  : {expected_unique == actual_unique}")
        print(f"  expected quarantined   : {truth['REJECTED']:,}")
        print(f"  actual   quarantined   : {len(bad):,}")
        print(f"  match                  : {truth['REJECTED'] == len(bad)}")


if __name__ == "__main__":
    main()
