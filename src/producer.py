"""
Synthetic payment and invoice event generator.

Emits events with deliberate defects — duplicates, late invoices, malformed
records, an uneven regional distribution and amounts that do not match — and
records what it injected so downstream output can be checked against a known
answer rather than eyeballed.

Two identifiers, two jobs:
    event_id    unique per message. The deduplication key.
    payment_id  unique per payment. The business key.
A redelivery repeats event_id; a correction reuses payment_id with a new
event_id and must survive deduplication.

Two clocks:
    event_ts    when it happened. Set here.
    ingest_ts   when the pipeline saw it. Set by the consumer.
A late invoice has an old event_ts and a current ingest_ts.

Output:
    data/events.jsonl            one JSON object per line, in arrival order
    data/_expected_totals.json   expected counts for reconciliation

Usage:
    python src/producer.py --count 5000 --seed 42
    python src/producer.py --count 5000 --seed 42 --sink kinesis
"""

from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONF_PATH = ROOT / "conf" / "config.json"
DATA_DIR = ROOT / "data"

PAYMENT_METHODS = ["WIRE", "ACH", "CARD", "SEPA"]


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def load_config(path: Path = CONF_PATH) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def iso(ts: datetime) -> str:
    """UTC, ISO-8601, Z-suffixed. One timestamp format everywhere, always UTC."""
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def pick_region(rng: random.Random, regions: list[dict]) -> dict:
    return rng.choices(regions, weights=[r["weight"] for r in regions], k=1)[0]


def envelope(event_type: str, event_ts: datetime, payload: dict) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": iso(event_ts),
        "schema_version": 1,
        **payload,
    }


# --------------------------------------------------------------------------
# defect injection
# --------------------------------------------------------------------------

CORRUPTIONS = [
    ("null_amount", lambda e: e.update(amount=None)),
    ("string_amount", lambda e: e.update(amount="not-a-number")),
    ("bad_currency", lambda e: e.update(currency="XXX")),
    ("missing_region", lambda e: e.pop("region", None)),
    ("negative_amount", lambda e: e.update(amount=-abs(e.get("amount") or 1))),
]


def corrupt(rng: random.Random, event: dict) -> str:
    """Mutate the event in place. Returns the corruption name for the answer key."""
    name, mutate = rng.choice(CORRUPTIONS)
    mutate(event)
    return name


# --------------------------------------------------------------------------
# generation
# --------------------------------------------------------------------------

def generate(cfg: dict, n_payments: int, seed: int) -> tuple[list[dict], dict]:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)

    regions = cfg["regions"]
    d = cfg["defects"]
    g = cfg["generation"]

    retry_window = {r["code"]: r["retry_window_days"] for r in regions}

    events: list[dict] = []
    truth = {
        "payments_generated": 0,
        "MATCHED": 0,
        "AMOUNT_MISMATCH": 0,
        "PENDING": 0,
        "EXPIRED": 0,
        "REJECTED": 0,
    }
    by_region: dict[str, int] = {}
    corruption_counts: dict[str, int] = {}
    duplicate_count = 0

    for i in range(n_payments):
        region = pick_region(rng, regions)
        code = region["code"]
        by_region[code] = by_region.get(code, 0) + 1

        age_days = rng.uniform(0, g["window_days"])
        p_ts = now - timedelta(days=age_days)
        payment_id = f"PAY-{i:07d}"
        amount = round(rng.uniform(50, 5000), 2)

        payment = envelope("PAYMENT_RECEIVED", p_ts, {
            "payment_id": payment_id,
            "customer_id": f"CUST-{rng.randint(1, 20000):06d}",
            "region": code,
            "currency": region["currency"],
            "amount": amount,
            "payment_method": rng.choice(PAYMENT_METHODS),
        })

        truth["payments_generated"] += 1

        # --- malformed payment: quarantined downstream, never matched ---
        if rng.random() < d["malformed_rate"]:
            name = corrupt(rng, payment)
            corruption_counts[name] = corruption_counts.get(name, 0) + 1
            truth["REJECTED"] += 1
            events.append(payment)
            continue

        events.append(payment)

        # --- does an invoice ever exist for this payment? ---
        if rng.random() < d["no_invoice_rate"]:
            invoice_ts = None
        elif rng.random() < d["late_invoice_rate"]:
            invoice_ts = p_ts + timedelta(
                days=rng.uniform(g["late_min_days"], g["late_max_days"])
            )
        else:
            invoice_ts = p_ts + timedelta(hours=rng.uniform(0, 24))

        # An invoice dated in the future has not arrived yet. This is what
        # creates a natural mix of PENDING and EXPIRED at run time.
        if invoice_ts is not None and invoice_ts <= now:
            mismatched = rng.random() < d["amount_mismatch_rate"]
            inv_amount = round(amount * rng.uniform(0.80, 0.98), 2) if mismatched else amount

            events.append(envelope("INVOICE_CLEARED", invoice_ts, {
                "invoice_id": f"INV-{i:07d}",
                "payment_id": payment_id,
                "region": code,
                "currency": region["currency"],
                "amount": inv_amount,
            }))
            truth["AMOUNT_MISMATCH" if mismatched else "MATCHED"] += 1
        else:
            days_waiting = (now - p_ts).days
            if days_waiting > retry_window[code]:
                truth["EXPIRED"] += 1
            else:
                truth["PENDING"] += 1

    # --- duplicate DELIVERIES: same event_id, emitted twice ---
    for event in list(events):
        if rng.random() < d["duplicate_rate"]:
            events.append(dict(event))
            duplicate_count += 1

    # arrival order
    events.sort(key=lambda e: e["event_ts"])

    truth["duplicate_events_injected"] = duplicate_count
    truth["corruptions"] = corruption_counts
    truth["events_emitted"] = len(events)
    truth["payments_by_region"] = by_region
    truth["generated_at"] = iso(now)
    truth["seed"] = seed

    return events, truth


# --------------------------------------------------------------------------
# sink
# --------------------------------------------------------------------------

def write_local(events: list[dict], truth: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    events_path = out_dir / "events.jsonl"
    with open(events_path, "w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")

    truth_path = out_dir / "_expected_totals.json"
    with open(truth_path, "w", encoding="utf-8") as fh:
        json.dump(truth, fh, indent=2)

    print(f"wrote {len(events):,} events -> {events_path}")
    print(f"wrote answer key      -> {truth_path}")


def write_kinesis(events: list[dict], cfg: dict) -> None:
    """
    PartitionKey is payment_id so a payment and its invoice land on the same
    shard, since ordering is only guaranteed within a shard.

    PutRecords accepts at most 500 records per call and can partially fail —
    a 200 response with FailedRecordCount > 0. Only the failed subset is
    resent; resending the whole chunk would duplicate what already succeeded.
    """
    import boto3

    client = boto3.client("kinesis", region_name=cfg["region"])
    stream = cfg["stream_name"]

    sent = retried = 0
    for start in range(0, len(events), 500):
        chunk = [
            {"Data": json.dumps(e).encode("utf-8"),
             "PartitionKey": str(e.get("payment_id") or e.get("event_id"))}
            for e in events[start:start + 500]
        ]

        for attempt in range(5):
            response = client.put_records(StreamName=stream, Records=chunk)
            failed = response.get("FailedRecordCount", 0)
            if not failed:
                break
            # Only the failed records.
            chunk = [
                rec for rec, result in zip(chunk, response["Records"])
                if result.get("ErrorCode")
            ]
            retried += failed
            time.sleep(0.5 * (2 ** attempt))
        else:
            raise SystemExit(f"{len(chunk)} records still failing after 5 attempts")

        sent += len(events[start:start + 500])
        print(f"  sent {sent:,}/{len(events):,}", end="\r")

    print(f"\nsent {sent:,} records -> kinesis:{stream} ({cfg['region']})")
    if retried:
        print(f"  {retried} records were throttled and resent")


def summarise(truth: dict) -> None:
    t = truth
    received = t["payments_generated"]
    accounted = t["MATCHED"] + t["AMOUNT_MISMATCH"] + t["PENDING"] + t["EXPIRED"] + t["REJECTED"]

    print("\n--- ground truth ---")
    print(f"  payments generated : {received:,}")
    print(f"  MATCHED            : {t['MATCHED']:,}")
    print(f"  AMOUNT_MISMATCH    : {t['AMOUNT_MISMATCH']:,}")
    print(f"  PENDING            : {t['PENDING']:,}")
    print(f"  EXPIRED            : {t['EXPIRED']:,}")
    print(f"  REJECTED           : {t['REJECTED']:,}")
    print(f"  {'-' * 30}")
    print(f"  accounted for      : {accounted:,}   balances: {accounted == received}")

    print(f"\n  duplicate deliveries injected : {t['duplicate_events_injected']:,}")
    print(f"  total events emitted          : {t['events_emitted']:,}")

    print("\n  region skew:")
    total = sum(t["payments_by_region"].values())
    for code, n in sorted(t["payments_by_region"].items(), key=lambda kv: -kv[1]):
        print(f"    {code}: {n:>6,}  ({n / total:.1%})")

    if t["corruptions"]:
        print("\n  corruptions injected:")
        for name, n in sorted(t["corruptions"].items()):
            print(f"    {name}: {n}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic payment events.")
    parser.add_argument("--count", type=int, default=5000, help="number of payments")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed; same seed = same data")
    parser.add_argument("--out", type=Path, default=DATA_DIR, help="output directory")
    parser.add_argument("--sink", choices=["local", "kinesis"], default="local")
    args = parser.parse_args()

    cfg = load_config()
    events, truth = generate(cfg, args.count, args.seed)

    # The answer key is always written locally — it is what every downstream
    # reconciliation is checked against.
    write_local(events, truth, args.out)
    if args.sink == "kinesis":
        write_kinesis(events, cfg)

    summarise(truth)


if __name__ == "__main__":
    main()
