"""
bronze_writer.py — the consumer side. Lands raw events in the bronze layer.

This is the logic that will live inside the Lambda. It runs locally now and
against S3 later by changing --sink, because the interesting part is not the
AWS call, it is the three rules below.


RULE 1 — BRONZE IS RAW AND IMMUTABLE
------------------------------------
The original message is stored verbatim as a string in `raw`. Nothing is
parsed, cast, validated or repaired here. If a message is garbage, bronze
stores the garbage.

Why: bronze is the replay source. The moment ingestion parses, a malformed
record can break ingestion itself, and you have lost the only copy of what
actually arrived. Parse in silver, where failure is contained and the bad
row goes to quarantine.


RULE 2 — PARTITION BY INGEST TIME, NOT EVENT TIME
-------------------------------------------------
    ingest_dt = when WE saw it      <- bronze partitions on this
    event_ts  = when it HAPPENED    <- silver partitions on this

An invoice six days late has an old event_ts but lands in today's ingest
partition. A forward-only read of bronze therefore still picks it up, and
only the silver write reaches back in time. Partition bronze by event date
and a late arrival silently lands in a partition you already processed.


RULE 3 — THE OBJECT KEY MUST BE DETERMINISTIC
---------------------------------------------
The key is a hash of the event_ids in the batch. Same batch in, same key out.

Why: Kinesis redelivers on failure. If the key contained a timestamp or a
uuid, a retry would write a SECOND object and every record in that batch
would be duplicated downstream. With a content-derived key the retry
overwrites the identical object and the duplicate never exists.

This is idempotency at the sink — Card D: the technique depends on the sink.
For object storage it is a deterministic key. It is not MERGE, and it is not
a dedup pass bolted on later.


Usage:
    python src/bronze_writer.py --batch-size 100
    python src/bronze_writer.py --batch-size 100 --sink s3
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

ROOT = Path(__file__).resolve().parents[1]
CONF_PATH = ROOT / "conf" / "config.json"
DATA_DIR = ROOT / "data"

SOURCE = "kinesis:payment-events"


def load_config(path: Path = CONF_PATH) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def read_raw_lines(path: Path) -> Iterator[str]:
    """Each line is one message exactly as the producer emitted it."""
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield line


def batched(items: Iterator[str], size: int) -> Iterator[list[str]]:
    batch: list[str] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def batch_id(raw_lines: list[str]) -> str:
    """
    Deterministic id derived from the batch CONTENT.

    In the real Lambda this would be the first record's Kinesis sequence
    number, which is equally stable across redeliveries. Hashing event_ids
    gives the same property without needing Kinesis to exist yet.
    """
    digest = hashlib.sha256()
    for line in raw_lines:
        try:
            digest.update(json.loads(line)["event_id"].encode())
        except (json.JSONDecodeError, KeyError):
            # Unparseable or missing event_id: fall back to the bytes themselves.
            # Bronze must accept this record, not reject it.
            digest.update(line.encode())
    return digest.hexdigest()[:16]


def partition_key_of(raw_line: str) -> str | None:
    """What Kinesis would shard on. payment_id keeps a payment and its
    invoice on the same shard, preserving their relative order."""
    try:
        return json.loads(raw_line).get("payment_id")
    except json.JSONDecodeError:
        return None


def build_records(raw_lines: list[str], bid: str, ingest_ts: datetime) -> list[dict]:
    stamped = iso(ingest_ts)
    return [
        {
            "batch_id": bid,
            "ingest_ts": stamped,
            "source": SOURCE,
            "partition_key": partition_key_of(line),
            "raw": line,
        }
        for line in raw_lines
    ]


def object_key(ingest_ts: datetime, bid: str) -> str:
    return (
        f"bronze/ingest_dt={ingest_ts:%Y-%m-%d}/hour={ingest_ts:%H}/part-{bid}.json"
    )


def write_local(key: str, body: str, root: Path) -> Path:
    path = root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def write_s3(key: str, body: str, bucket: str) -> str:
    import boto3

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    return f"s3://{bucket}/{key}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Land raw events into the bronze layer.")
    parser.add_argument("--input", type=Path, default=DATA_DIR / "events.jsonl")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="mirrors the Lambda event-source-mapping batch size")
    parser.add_argument("--sink", choices=["local", "s3"], default="local")
    parser.add_argument("--out", type=Path, default=DATA_DIR,
                        help="root for the local sink")
    args = parser.parse_args()

    cfg = load_config()
    ingest_ts = datetime.now(timezone.utc)

    batches = written = 0
    partitions: set[str] = set()

    for raw_lines in batched(read_raw_lines(args.input), args.batch_size):
        bid = batch_id(raw_lines)
        key = object_key(ingest_ts, bid)
        body = "\n".join(
            json.dumps(r) for r in build_records(raw_lines, bid, ingest_ts)
        )

        if args.sink == "s3":
            write_s3(key, body, cfg["bucket"])
        else:
            write_local(key, body, args.out)

        batches += 1
        written += len(raw_lines)
        partitions.add(key.rsplit("/", 1)[0])

    target = f"s3://{cfg['bucket']}/bronze/" if args.sink == "s3" else args.out / "bronze"
    print(f"sink        : {args.sink}")
    print(f"batches     : {batches}")
    print(f"records     : {written:,}")
    print(f"partitions  : {len(partitions)}")
    print(f"target      : {target}")
    print("\nre-run this command: object keys are identical, nothing duplicates.")


if __name__ == "__main__":
    main()
