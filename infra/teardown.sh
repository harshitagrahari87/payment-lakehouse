#!/usr/bin/env bash
#
# teardown.sh — delete every billable resource in this project.
#
# A Kinesis shard bills ~$0.36/day whether or not data flows through it, so
# this runs at the end of every session.
#
#     ./infra/teardown.sh
#
# S3 objects are kept by default. Pass --all to remove the bucket too.
#
set -uo pipefail

cd "$(dirname "$0")/.."

BUCKET=$(python3 -c "import json;print(json.load(open('conf/config.json'))['bucket'])")
REGION=$(python3 -c "import json;print(json.load(open('conf/config.json'))['region'])")
STREAM=$(python3 -c "import json;print(json.load(open('conf/config.json'))['stream_name'])")

FUNCTION="payment-bronze-consumer"
ROLE="payment-lambda-role"

echo "region : $REGION"
echo "bucket : $BUCKET"
echo

# --- event source mapping ------------------------------------------------
# Mapping first: removing the stream while a mapping still points at it
# leaves the mapping in an error state.
echo "[1/5] event source mappings"
MAPPINGS=$(aws lambda list-event-source-mappings \
             --function-name "$FUNCTION" --region "$REGION" \
             --query 'EventSourceMappings[].UUID' --output text 2>/dev/null || true)
for uuid in $MAPPINGS; do
    [ "$uuid" = "None" ] && continue
    aws lambda delete-event-source-mapping --uuid "$uuid" --region "$REGION" >/dev/null 2>&1 \
        && echo "      deleted mapping $uuid"
done

# --- lambda --------------------------------------------------------------
echo "[2/5] lambda function"
aws lambda delete-function --function-name "$FUNCTION" --region "$REGION" >/dev/null 2>&1 \
    && echo "      deleted $FUNCTION" || echo "      none"

# --- kinesis -------------------------------------------------------------
echo "[3/5] kinesis stream"
aws kinesis delete-stream --stream-name "$STREAM" \
    --enforce-consumer-deletion --region "$REGION" >/dev/null 2>&1 \
    && echo "      deleted $STREAM" || echo "      none"

# --- iam -----------------------------------------------------------------
echo "[4/5] iam role"
for arn in $(aws iam list-attached-role-policies --role-name "$ROLE" \
               --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null || true); do
    [ "$arn" = "None" ] && continue
    aws iam detach-role-policy --role-name "$ROLE" --policy-arn "$arn" >/dev/null 2>&1
done
for name in $(aws iam list-role-policies --role-name "$ROLE" \
                --query 'PolicyNames[]' --output text 2>/dev/null || true); do
    [ "$name" = "None" ] && continue
    aws iam delete-role-policy --role-name "$ROLE" --policy-name "$name" >/dev/null 2>&1
done
aws iam delete-role --role-name "$ROLE" >/dev/null 2>&1 \
    && echo "      deleted $ROLE" || echo "      none"

# --- s3 (optional) -------------------------------------------------------
echo "[5/5] s3"
if [ "${1:-}" = "--all" ]; then
    aws s3 rm "s3://$BUCKET" --recursive --region "$REGION" >/dev/null
    aws s3api delete-bucket --bucket "$BUCKET" --region "$REGION" >/dev/null
    echo "      bucket DELETED"
else
    echo "      kept (pass --all to delete the bucket and its contents)"
fi

echo
echo "remaining billable compute:"
aws kinesis list-streams --region "$REGION" --query 'StreamNames' --output text
aws lambda list-functions --region "$REGION" --query 'Functions[].FunctionName' --output text
echo "(empty above means nothing is running)"
