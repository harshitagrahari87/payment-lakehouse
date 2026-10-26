"""
Kinesis -> S3 bronze consumer.

Deployed as a Lambda function behind an event source mapping. The mapping
polls the stream, batches records, and invokes this handler; the handler does
not poll.

Environment:
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
    return f"bronze/ingest_dt={ingest_ts:%Y-%m-%d}/hour={ingest_ts:%H}/part-{batch_id}.json"


def lambda_handler(event, context):
    records = event.get("Records", [])
    if not records:
        return {"batchItemFailures": []}

    ingest_ts = datetime.now(timezone.utc)
    stamped = ingest_ts.isoformat().replace("+00:00", "Z")

    # Derived from the batch's first sequence number so a redelivery writes to
    # the same key and overwrites rather than duplicating.
    batch_id = records[0]["kinesis"]["sequenceNumber"][-16:]

    lines: list[str] = []
    failures: list[dict] = []

    for record in records:
        kinesis = record["kinesis"]
        sequence = kinesis["sequenceNumber"]
        try:
            raw = base64.b64decode(kinesis["data"]).decode("utf-8")
        except Exception as exc:
            print(f"decode failed seq={sequence}: {exc}")
            failures.append({"itemIdentifier": sequence})
            continue

        # Stored verbatim. Validation happens in silver, where a bad record is
        # contained to the quarantine table instead of breaking ingestion.
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
            # Nothing in this batch landed, so fail every record and let the
            # mapping redeliver the whole batch. Partial success here would
            # advance the checkpoint past records that were never written.
            print(f"s3 put failed: {exc}")
            return {
                "batchItemFailures": [
                    {"itemIdentifier": r["kinesis"]["sequenceNumber"]} for r in records
                ]
            }

    print(f"wrote {len(lines)} records, batch={batch_id}, failures={len(failures)}")

    # Requires FunctionResponseTypes=["ReportBatchItemFailures"] on the mapping.
    # Without it a single bad record fails the whole batch indefinitely and
    # stalls the shard.
    return {"batchItemFailures": failures}
