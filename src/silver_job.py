"""
bronze -> silver.

Parses the raw messages, validates them, routes failures to a quarantine
table, deduplicates on event_id and repartitions by event date.

Validation lives here rather than in bronze so that a malformed record is
contained to one row instead of breaking ingestion. Nothing is dropped:
rejected records keep their original payload and a reason, so they can be
reprocessed once the cause is fixed.

Spark equivalents for the two non-obvious steps:

    dedup
        w = Window.partitionBy("event_id").orderBy("ingest_ts")
        df.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")

    write
        df.write.partitionBy("event_dt").mode("overwrite").parquet(path)

Usage:
    python src/silver_job.py
    python src/silver_job.py --source s3 --sink s3
"""

from __future__ import annotations

import argparse
import io
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
    Writes the partitioned layout to S3, clearing the prefix first.

    This job and the Spark version write different filenames
    (part-000.parquet vs part-00000-<uuid>-c000.snappy.parquet). A Hive-style
    table is defined by whatever files sit under the prefix, so without
    clearing, both sets survive and every downstream count is inflated.
    """
    import boto3

    if df.empty:
        return 0

    s3 = boto3.client("s3", region_name=cfg["region"])
    prefix = f"silver/{name}/"

    removed = 0
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=cfg["bucket"], Prefix=prefix
    ):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=cfg["bucket"], Delete={"Objects": keys})
            removed += len(keys)
    if removed:
        print(f"  cleared {removed} stale object(s) under {prefix}")

    written = 0
    for event_dt, part in df.groupby("event_dt"):
        buf = io.BytesIO()
        part.drop(columns=["event_dt"]).to_parquet(buf, index=False)
        s3.put_object(
            Bucket=cfg["bucket"],
            Key=f"{prefix}event_dt={event_dt}/part-000.parquet",
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
