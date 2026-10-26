#!/usr/bin/env bash
#
# deploy.sh — create the streaming half of the pipeline.
#
#     Kinesis stream (1 shard)
#       -> IAM execution role (least privilege, not AdministratorAccess)
#       -> Lambda function
#       -> event source mapping
#
# Idempotent: safe to re-run. Every create is guarded.
#
#     ./infra/deploy.sh
#     ./infra/teardown.sh      <- run this when finished
#
set -euo pipefail

cd "$(dirname "$0")/.."

BUCKET=$(python3 -c "import json;print(json.load(open('conf/config.json'))['bucket'])")
REGION=$(python3 -c "import json;print(json.load(open('conf/config.json'))['region'])")
STREAM=$(python3 -c "import json;print(json.load(open('conf/config.json'))['stream_name'])")

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
FUNCTION="payment-bronze-consumer"
ROLE="payment-lambda-role"

echo "account : $ACCOUNT"
echo "region  : $REGION"
echo "bucket  : $BUCKET"
echo

# --------------------------------------------------------------------
# 1. Kinesis — 1 shard.
#    Sizing: 2M payments/day = ~23/s average, ~116/s peak, ~0.17 MB/s.
#    A shard handles 1 MB/s OR 1000 rec/s. One shard, comfortably.
#    Size for BOTH ceilings and take the higher — here neither binds.
# --------------------------------------------------------------------
echo "[1/5] kinesis stream"
if aws kinesis describe-stream-summary --stream-name "$STREAM" --region "$REGION" >/dev/null 2>&1; then
    echo "      exists"
else
    aws kinesis create-stream --stream-name "$STREAM" --shard-count 1 --region "$REGION"
    aws kinesis wait stream-exists --stream-name "$STREAM" --region "$REGION"
    echo "      created (BILLING ~\$0.36/day — run teardown.sh when done)"
fi
STREAM_ARN=$(aws kinesis describe-stream-summary --stream-name "$STREAM" --region "$REGION" \
             --query 'StreamDescriptionSummary.StreamARN' --output text)

# --------------------------------------------------------------------
# 2. IAM role.
#    Scoped to this stream and this bucket prefix only. The project runs
#    under AdministratorAccess, but the Lambda does not have to.
# --------------------------------------------------------------------
echo "[2/5] iam role"
if aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
    echo "      exists"
else
    aws iam create-role --role-name "$ROLE" \
        --assume-role-policy-document '{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }' >/dev/null
    echo "      created"
fi

# CloudWatch Logs
aws iam attach-role-policy --role-name "$ROLE" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Kinesis read — scoped to this stream, not kinesis:* on *
aws iam put-role-policy --role-name "$ROLE" --policy-name kinesis-read \
    --policy-document "{
        \"Version\": \"2012-10-17\",
        \"Statement\": [{
            \"Effect\": \"Allow\",
            \"Action\": [
                \"kinesis:DescribeStream\",
                \"kinesis:DescribeStreamSummary\",
                \"kinesis:GetRecords\",
                \"kinesis:GetShardIterator\",
                \"kinesis:ListShards\",
                \"kinesis:ListStreams\"
            ],
            \"Resource\": \"$STREAM_ARN\"
        }]
    }"

# S3 write — bronze prefix only. Note the single ARN: PutObject is an
# OBJECT-level action, so it needs /bronze/*, not the bucket ARN.
aws iam put-role-policy --role-name "$ROLE" --policy-name s3-bronze-write \
    --policy-document "{
        \"Version\": \"2012-10-17\",
        \"Statement\": [{
            \"Effect\": \"Allow\",
            \"Action\": \"s3:PutObject\",
            \"Resource\": \"arn:aws:s3:::$BUCKET/bronze/*\"
        }]
    }"
ROLE_ARN="arn:aws:iam::$ACCOUNT:role/$ROLE"
echo "      policies attached"

# --------------------------------------------------------------------
# 3. Package. boto3 ships in the Lambda runtime, so the zip is one file.
# --------------------------------------------------------------------
echo "[3/5] package"
rm -f /tmp/lambda.zip
(cd src && zip -q /tmp/lambda.zip lambda_handler.py)
echo "      /tmp/lambda.zip"

# --------------------------------------------------------------------
# 4. Lambda function
# --------------------------------------------------------------------
echo "[4/5] lambda function"
if aws lambda get-function --function-name "$FUNCTION" --region "$REGION" >/dev/null 2>&1; then
    aws lambda update-function-code --function-name "$FUNCTION" \
        --zip-file fileb:///tmp/lambda.zip --region "$REGION" >/dev/null
    echo "      code updated"
else
    # IAM role propagation lags role creation; retry rather than fail.
    for attempt in 1 2 3 4 5; do
        if aws lambda create-function \
            --function-name "$FUNCTION" \
            --runtime python3.12 \
            --role "$ROLE_ARN" \
            --handler lambda_handler.lambda_handler \
            --zip-file fileb:///tmp/lambda.zip \
            --timeout 60 \
            --memory-size 256 \
            --environment "Variables={BUCKET=$BUCKET}" \
            --region "$REGION" >/dev/null 2>&1; then
            echo "      created"
            break
        fi
        echo "      waiting for IAM propagation (attempt $attempt)"
        sleep 10
    done
fi
aws lambda wait function-active --function-name "$FUNCTION" --region "$REGION"

# --------------------------------------------------------------------
# 5. Event source mapping — this is the component that POLLS Kinesis.
#
#    BatchSize 100                 up to 100 records per invocation
#    MaximumBatchingWindow 10s     or every 10s, whichever comes first
#    StartingPosition TRIM_HORIZON oldest record in the stream (vs LATEST,
#                                  which skips everything already there)
#    FunctionResponseTypes         enables the partial batch response
#    BisectBatchOnFunctionError    halve the batch on failure to isolate
#                                  the poison record instead of stalling
# --------------------------------------------------------------------
echo "[5/5] event source mapping"
EXISTING=$(aws lambda list-event-source-mappings --function-name "$FUNCTION" --region "$REGION" \
           --query 'EventSourceMappings[0].UUID' --output text 2>/dev/null || echo "None")
if [ "$EXISTING" != "None" ] && [ -n "$EXISTING" ]; then
    echo "      exists ($EXISTING)"
else
    aws lambda create-event-source-mapping \
        --function-name "$FUNCTION" \
        --event-source-arn "$STREAM_ARN" \
        --batch-size 100 \
        --maximum-batching-window-in-seconds 10 \
        --starting-position TRIM_HORIZON \
        --maximum-retry-attempts 3 \
        --bisect-batch-on-function-error \
        --function-response-types ReportBatchItemFailures \
        --region "$REGION" >/dev/null
    echo "      created"
fi

echo
echo "ready. send events with:"
echo "    python3 src/producer.py --count 2000 --seed 7 --sink kinesis"
echo
echo "WHEN FINISHED:  ./infra/teardown.sh"
