"""
lambda_handler.py — the Kinesis -> S3 bronze consumer.

This is the function EPAM asked for and I could not write: "can Airflow /
Lambda land events from a source into S3 — write it."


WHAT LAMBDA ACTUALLY DOES HERE
------------------------------
Lambda does NOT poll Kinesis. The **event source mapping** polls on your
behalf, accumulates up to `BatchSize` records (or until
`MaximumBatchingWindowInSeconds` elapses), and invokes this function with
that batch. You never write a poll loop, and you do not control when it runs.

That distinction is reread #69 in aws.md.


THE THREE THINGS THAT MATTER
----------------------------

1. BASE64. Kinesis delivers the payload base64-encoded. Forgetting to decode
   is the most common first bug.

2. DETERMINISTIC KEY. On failure the mapping does not advance its checkpoint
   and REDELIVERS the batch. If the S3 key contained a timestamp or uuid, the
   retry would write a SECOND object and duplicate every record in it.
   The key here is derived from the batch's first Kinesis sequence number,
   which is stable across redeliveries of the same batch.

   Honest limitation: a retry is not guaranteed to be batched identically.
   Sink-side idempotency REDUCES duplicates; it does not eliminate them —
   which is why silver still dedupes on event_id. Two independent defences.

3. PARTIAL BATCH RESPONSE. Returning `batchItemFailures` tells the mapping to
   retry ONLY the failed records instead of the whole batch. Without it, one
   poison record fails all 100 forever and the shard stalls.
   That is reread #70.


Environment variables (set by infra/deploy.sh):
    BUCKET   target bucket
"""

from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]
SOURCE = "kinesis:payment-events"


def _object_key(ingest_ts: datetime, batch_id: str) -> str:
    """Partitioned by INGEST time — when we received it, not when it happened."""
    return f"bronze/ingest_dt={ingest_ts:%Y-%m-%d}/hour={ingest_ts:%H}/part-{batch_id}.json"


def lambda_handler(event, context):
    records = event.get("Records", [])
    if not records:
        return {"batchItemFailures": []}

    ingest_ts = datetime.now(timezone.utc)
    stamped = ingest_ts.isoformat().replace("+00:00", "Z")

    # Stable across redeliveries of this batch.
    batch_id = records[0]["kinesis"]["sequenceNumber"][-16:]

    lines: list[str] = []
    failures: list[dict] = []

    for record in records:
        kinesis = record["kinesis"]
        sequence = kinesis["sequenceNumber"]
        try:
            raw = base64.b64decode(kinesis["data"]).decode("utf-8")
        except Exception as exc:  # undecodable bytes — cannot be retried into health
            print(f"decode failed seq={sequence}: {exc}")
            failures.append({"itemIdentifier": sequence})
            continue

        # NOTE: `raw` is stored verbatim and is NOT parsed or validated here.
        # Bronze must accept malformed input; validation happens in silver,
        # where a bad row is contained to a quarantine table instead of
        # breaking ingestion.
        lines.append(json.dumps({
            "batch_id": batch_id,
            "ingest_ts": stamped,
            "source": SOURCE,
            "partition_key": kinesis.get("partitionKey"),
            "sequence_number": sequence,
            "raw": raw,
        }))

    if lines:
        try:
            s3.put_object(
                Bucket=BUCKET,
                Key=_object_key(ingest_ts, batch_id),
                Body=("\n".join(lines)).encode("utf-8"),
                ContentType="application/x-ndjson",
            )
        except Exception as exc:
            # The write failed, so NOTHING in this batch landed. Fail every
            # record so the mapping redelivers the whole batch. Reporting
            # partial success here would silently lose data.
            print(f"s3 put failed: {exc}")
            return {
                "batchItemFailures": [
                    {"itemIdentifier": r["kinesis"]["sequenceNumber"]} for r in records
                ]
            }

    print(f"wrote {len(lines)} records, batch={batch_id}, failures={len(failures)}")
    return {"batchItemFailures": failures}
