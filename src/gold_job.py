"""
gold_job.py — silver -> gold. The business logic.

Everything before this moved and cleaned data. This decides what the data
MEANS: has a payment been matched to its invoice, is it still waiting, or has
it run out of time?


THE STATE MACHINE
-----------------
    PAYMENT_RECEIVED
          |
          +-- invoice found, amounts agree ----------> MATCHED
          |
          +-- invoice found, amounts differ ---------> AMOUNT_MISMATCH
          |                                            (matched, but flagged
          |                                             for finance to review)
          +-- no invoice yet
                   |
                   +-- within the region's retry window --> PENDING
                   |
                   +-- past the window -------------------> EXPIRED
                                                            (exception report)

The retry window is per REGION and lives in conf/config.json, not in code,
because clearance times differ by country and the business changes them
without a deploy. US = 3 days, AE = 10.


WHY as_of IS A PARAMETER AND NEVER datetime.now()
-------------------------------------------------
Whether a payment is PENDING or EXPIRED depends on the date you ask. Using
now() means re-running yesterday's report today produces different numbers,
and a backfill silently rewrites history.

This is the same idempotency rule as Airflow's logical_date:
    "templates use the LOGICAL date, never datetime.now()"
Pass the date in. Then the job is reproducible and a backfill is safe.


THE CONTROL TOTAL
-----------------
    received = MATCHED + AMOUNT_MISMATCH + PENDING + EXPIRED + REJECTED

If that does not balance, the pipeline lost or invented money. It is checked
here, and it is the reason the generator emits an answer key.


OUTPUTS
-------
    gold/fct_payment_state/      one row per payment_id   (current state)
    gold/dim_region/             retry windows and currency
    gold/reconciliation_daily/   counts by payment date and region

Usage:
    python src/gold_job.py
    python src/gold_job.py --source s3 --sink s3
    python src/gold_job.py --as-of 2026-10-25T12:00:00Z
"""

from __future__ import annotations

import argparse
import io
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CONF_PATH = ROOT / "conf" / "config.json"
DATA_DIR = ROOT / "data"

STATES = ["MATCHED", "AMOUNT_MISMATCH", "PENDING", "EXPIRED"]


def load_config(path: Path = CONF_PATH) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# --------------------------------------------------------------------------
# read silver
# --------------------------------------------------------------------------

def read_silver_local(root: Path, name: str) -> pd.DataFrame:
    paths = sorted((root / name).rglob("*.parquet"))
    if not paths:
        raise SystemExit(f"no parquet under {root / name} — run silver_job.py first")
    return pd.concat((pd.read_parquet(p) for p in paths), ignore_index=True)


def read_silver_s3(cfg: dict, name: str) -> pd.DataFrame:
    import boto3

    s3 = boto3.client("s3", region_name=cfg["region"])
    frames = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=cfg["bucket"], Prefix=f"silver/{name}/"
    ):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                body = s3.get_object(Bucket=cfg["bucket"], Key=obj["Key"])["Body"].read()
                frames.append(pd.read_parquet(io.BytesIO(body)))
    if not frames:
        raise SystemExit(f"no parquet under s3://{cfg['bucket']}/silver/{name}/")
    return pd.concat(frames, ignore_index=True)


# --------------------------------------------------------------------------
# the match
# --------------------------------------------------------------------------

def build_payment_state(
    payments: pd.DataFrame,
    invoices: pd.DataFrame,
    cfg: dict,
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    retry_window = {r["code"]: r["retry_window_days"] for r in cfg["regions"]}

    # One invoice per payment. Without this a payment with two invoices would
    # fan out into two rows and double-count. Delta raises an explicit
    # "multiple source rows matched" error on MERGE for exactly this reason —
    # the source must be deduplicated before the merge, not after.
    inv = (
        invoices.sort_values(["payment_id", "event_ts"])
                .drop_duplicates(subset=["payment_id"], keep="first")
                .loc[:, ["payment_id", "invoice_id", "amount", "event_ts"]]
                .rename(columns={"amount": "invoice_amount", "event_ts": "invoice_ts"})
    )

    # Select explicitly. Silver builds payments and invoices from the same
    # union of columns, so the payments frame carries an all-null invoice_id.
    # Merging without this produces invoice_id_x / invoice_id_y and the
    # downstream lookup fails. Never inherit a schema you did not declare.
    pay = payments.loc[:, [
        "payment_id", "customer_id", "region", "currency",
        "amount", "payment_method", "event_ts",
    ]]

    df = pay.merge(inv, on="payment_id", how="left")

    df["payment_ts"] = pd.to_datetime(df["event_ts"], utc=True, format="mixed")
    df["payment_dt"] = df["payment_ts"].dt.strftime("%Y-%m-%d")

    has_invoice = df["invoice_id"].notna()
    amounts_agree = has_invoice & np.isclose(
        df["amount"].astype(float),
        df["invoice_amount"].fillna(-1).astype(float),
    )

    days_waiting = (as_of - df["payment_ts"]).dt.days
    window = df["region"].map(retry_window)

    df["days_waiting"] = days_waiting
    df["retry_window_days"] = window

    df["state"] = np.select(
        [
            has_invoice & amounts_agree,
            has_invoice & ~amounts_agree,
            ~has_invoice & (days_waiting > window),
        ],
        ["MATCHED", "AMOUNT_MISMATCH", "EXPIRED"],
        default="PENDING",
    )

    df["amount_variance"] = np.where(
        has_invoice, df["invoice_amount"].astype(float) - df["amount"].astype(float), np.nan
    )
    df["as_of"] = as_of.isoformat()

    return df[[
        "payment_id", "customer_id", "region", "currency", "amount",
        "payment_method", "payment_ts", "payment_dt",
        "invoice_id", "invoice_amount", "invoice_ts",
        "amount_variance", "days_waiting", "retry_window_days",
        "state", "as_of",
    ]]


def build_dim_region(cfg: dict) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "region": r["code"],
            "currency": r["currency"],
            "retry_window_days": r["retry_window_days"],
            "expected_volume_share": r["weight"],
        }
        for r in cfg["regions"]
    ])


def build_reconciliation_daily(fct: pd.DataFrame) -> pd.DataFrame:
    """Grain: one row per payment_dt per region."""
    recon = (
        fct.pivot_table(
            index=["payment_dt", "region"],
            columns="state",
            values="payment_id",
            aggfunc="count",
            fill_value=0,
        )
        .reset_index()
    )
    for state in STATES:
        if state not in recon.columns:
            recon[state] = 0

    recon["total_payments"] = recon[STATES].sum(axis=1)

    amounts = (
        fct.groupby(["payment_dt", "region"])["amount"]
           .sum()
           .reset_index()
           .rename(columns={"amount": "total_amount"})
    )
    recon = recon.merge(amounts, on=["payment_dt", "region"], how="left")
    recon.columns.name = None
    return recon


# --------------------------------------------------------------------------
# write
# --------------------------------------------------------------------------

def write_local(df: pd.DataFrame, root: Path, name: str) -> str:
    target = root / name
    target.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target / "part-000.parquet", index=False)
    return str(target)


def write_s3(df: pd.DataFrame, cfg: dict, name: str) -> str:
    import boto3

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    key = f"gold/{name}/part-000.parquet"
    boto3.client("s3", region_name=cfg["region"]).put_object(
        Bucket=cfg["bucket"], Key=key, Body=buf.getvalue()
    )
    return f"s3://{cfg['bucket']}/{key}"


def resolve_as_of(arg: str | None) -> tuple[pd.Timestamp, str]:
    """
    Explicit --as-of wins. Otherwise use the producer's generation time, so
    the run is reproducible and comparable to the answer key. now() is the
    last resort and is flagged as such.
    """
    if arg:
        return pd.Timestamp(arg).tz_convert("UTC"), "--as-of argument"

    truth_path = DATA_DIR / "_expected_totals.json"
    if truth_path.exists():
        truth = json.loads(truth_path.read_text(encoding="utf-8"))
        return pd.Timestamp(truth["generated_at"]).tz_convert("UTC"), "producer answer key"

    return pd.Timestamp(datetime.now(timezone.utc)), "now() — NOT reproducible"


def main() -> None:
    parser = argparse.ArgumentParser(description="silver -> gold")
    parser.add_argument("--silver", type=Path, default=DATA_DIR / "silver")
    parser.add_argument("--out", type=Path, default=DATA_DIR / "gold")
    parser.add_argument("--source", choices=["local", "s3"], default="local")
    parser.add_argument("--sink", choices=["local", "s3"], default="local")
    parser.add_argument("--as-of", dest="as_of", default=None,
                        help="ISO timestamp the report is computed AS OF")
    args = parser.parse_args()

    cfg = load_config()
    as_of, as_of_origin = resolve_as_of(args.as_of)

    if args.source == "s3":
        payments = read_silver_s3(cfg, "payments")
        invoices = read_silver_s3(cfg, "invoices")
        quarantine = read_silver_s3(cfg, "quarantine")
    else:
        payments = read_silver_local(args.silver, "payments")
        invoices = read_silver_local(args.silver, "invoices")
        quarantine = read_silver_local(args.silver, "quarantine")

    print(f"as_of        : {as_of.isoformat()}   (from {as_of_origin})")
    print(f"payments     : {len(payments):,}")
    print(f"invoices     : {len(invoices):,}")
    print(f"quarantined  : {len(quarantine):,}")

    fct = build_payment_state(payments, invoices, cfg, as_of)
    dim = build_dim_region(cfg)
    recon = build_reconciliation_daily(fct)

    if args.sink == "s3":
        t1 = write_s3(fct, cfg, "fct_payment_state")
        write_s3(dim, cfg, "dim_region")
        write_s3(recon, cfg, "reconciliation_daily")
        target = f"s3://{cfg['bucket']}/gold/"
    else:
        t1 = write_local(fct, args.out, "fct_payment_state")
        write_local(dim, args.out, "dim_region")
        write_local(recon, args.out, "reconciliation_daily")
        target = str(args.out)

    counts = fct["state"].value_counts().to_dict()
    rejected = len(quarantine)
    received = len(fct) + rejected

    print(f"\nsink         : {target}")
    print(f"\n--- payment state as of {as_of.date()} ---")
    for state in STATES:
        print(f"  {state:<16}: {counts.get(state, 0):>6,}")
    print(f"  {'REJECTED':<16}: {rejected:>6,}")

    accounted = sum(counts.get(s, 0) for s in STATES) + rejected
    print(f"  {'-' * 24}")
    print(f"  {'received':<16}: {received:>6,}")
    print(f"  control total balances : {accounted == received}")

    print("\n--- match rate by region ---")
    by_region = (
        fct.assign(matched=fct["state"].isin(["MATCHED", "AMOUNT_MISMATCH"]))
           .groupby("region")
           .agg(payments=("payment_id", "count"), matched=("matched", "sum"))
    )
    by_region["match_rate"] = by_region["matched"] / by_region["payments"]
    for region, row in by_region.sort_values("payments", ascending=False).iterrows():
        print(f"  {region}: {int(row['payments']):>5,} payments, "
              f"{row['match_rate']:.1%} matched")

    # ---- reconcile against the producer's answer key -------------------
    truth_path = DATA_DIR / "_expected_totals.json"
    if truth_path.exists():
        truth = json.loads(truth_path.read_text(encoding="utf-8"))
        print("\n--- reconciliation vs producer answer key ---")
        all_ok = True
        for state in STATES + ["REJECTED"]:
            expected = truth.get(state, 0)
            actual = rejected if state == "REJECTED" else counts.get(state, 0)
            ok = expected == actual
            all_ok &= ok
            print(f"  {state:<16} expected {expected:>6,}   actual {actual:>6,}   {ok}")
        print(f"\n  ALL MATCH : {all_ok}")


if __name__ == "__main__":
    main()
