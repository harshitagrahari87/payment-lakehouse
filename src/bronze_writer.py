"""
Lands raw events in the bronze layer.

This is the same logic the Lambda runs; it exists separately so the pipeline
can be exercised end to end without deploying anything. The sink is selectable
so the AWS path and the local path share one implementation.

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
    Content-derived id. The same batch always produces the same id, so a
    retry overwrites its object instead of writing a second copy.

    The Lambda uses the first Kinesis sequence number, which has the same
    property. Hashing event_ids gives it without needing a stream.
    """
    digest = hashlib.sha256()
    for line in raw_lines:
        try:
            digest.update(json.loads(line)["event_id"].encode())
        except (json.JSONDecodeError, KeyError):
            digest.update(line.encode())
    return digest.hexdigest()[:16]


def partition_key_of(raw_line: str) -> str | None:
    """Keying on payment_id keeps a payment and its invoice on one shard."""
    try:
        return json.loads(raw_line).get("payment_id")
    except json.JSONDecodeError:
        return None


def build_records(raw_lines: list[str], bid: str, ingest_ts: datetime) -> list[dict]:
    """Original message preserved verbatim in `raw`, with provenance around it."""
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
    # Partitioned by arrival time. A late event lands in a current partition,
    # so a forward-only read still picks it up.
    return f"bronze/ingest_dt={ingest_ts:%Y-%m-%d}/hour={ingest_ts:%H}/part-{bid}.json"


def write_local(key: str, body: str, root: Path) -> Path:
    path = root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def write_s3(key: str, body: str, bucket: str, region: str) -> str:
    import boto3

    boto3.client("s3", region_name=region).put_object(
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
                        help="mirrors the Lambda event source mapping batch size")
    parser.add_argument("--sink", choices=["local", "s3"], default="local")
    parser.add_argument("--out", type=Path, default=DATA_DIR)
    args = parser.parse_args()

    cfg = load_config()
    ingest_ts = datetime.now(timezone.utc)

    batches = written = 0
    partitions: set[str] = set()

    for raw_lines in batched(read_raw_lines(args.input), args.batch_size):
        bid = batch_id(raw_lines)
        key = object_key(ingest_ts, bid)
        body = "\n".join(json.dumps(r) for r in build_records(raw_lines, bid, ingest_ts))

        if args.sink == "s3":
            write_s3(key, body, cfg["bucket"], cfg["region"])
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


if __name__ == "__main__":
    main()
