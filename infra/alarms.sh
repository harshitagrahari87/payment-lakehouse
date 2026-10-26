#!/usr/bin/env bash
#
# alarms.sh — CloudWatch alarms and an SNS topic for notifications.
#
# Four alarms, each tied to a failure this pipeline can actually have.
# Note what is NOT alarmed: job success. A job that completes with wrong
# numbers is the dangerous case, so the alarms watch OUTPUTS, not exit codes.
#
#     ./infra/alarms.sh you@example.com
#     ./infra/alarms.sh --delete
#
set -euo pipefail

cd "$(dirname "$0")/.."

REGION=$(python3 -c "import json;print(json.load(open('conf/config.json'))['region'])")
STREAM=$(python3 -c "import json;print(json.load(open('conf/config.json'))['stream_name'])")

FUNCTION="payment-bronze-consumer"
TOPIC="payment-lakehouse-alerts"
NS="PaymentLakehouse"

ALARMS=(
    "plh-lambda-errors"
    "plh-kinesis-iterator-age"
    "plh-match-rate-low"
    "plh-volume-anomaly"
)

if [ "${1:-}" = "--delete" ]; then
    aws cloudwatch delete-alarms --alarm-names "${ALARMS[@]}" --region "$REGION" 2>/dev/null || true
    ARN=$(aws sns list-topics --region "$REGION" \
          --query "Topics[?ends_with(TopicArn,':$TOPIC')].TopicArn" --output text)
    [ -n "$ARN" ] && aws sns delete-topic --topic-arn "$ARN" --region "$REGION"
    echo "alarms and topic deleted"
    exit 0
fi

EMAIL="${1:-}"

TOPIC_ARN=$(aws sns create-topic --name "$TOPIC" --region "$REGION" \
            --query TopicArn --output text)
echo "topic: $TOPIC_ARN"

if [ -n "$EMAIL" ]; then
    aws sns subscribe --topic-arn "$TOPIC_ARN" --protocol email \
        --notification-endpoint "$EMAIL" --region "$REGION" >/dev/null
    echo "       confirm the subscription email sent to $EMAIL"
fi

# ---------------------------------------------------------------------
# 1. Lambda errors.
#    Any error means records were not written. The event source mapping
#    will retry, but a sustained error stalls the shard — Kinesis will not
#    advance past a batch that keeps failing.
# ---------------------------------------------------------------------
aws cloudwatch put-metric-alarm \
    --alarm-name plh-lambda-errors \
    --alarm-description "Bronze consumer threw an error" \
    --namespace AWS/Lambda --metric-name Errors \
    --dimensions Name=FunctionName,Value="$FUNCTION" \
    --statistic Sum --period 300 --evaluation-periods 1 \
    --threshold 1 --comparison-operator GreaterThanOrEqualToThreshold \
    --treat-missing-data notBreaching \
    --alarm-actions "$TOPIC_ARN" --region "$REGION"
echo "  [1/4] plh-lambda-errors"

# ---------------------------------------------------------------------
# 2. Iterator age — the single most important streaming metric.
#    It is how far BEHIND the consumer is, in milliseconds of data. A
#    steady value is fine; a RISING value means the consumer cannot keep
#    up and will eventually breach the 24h retention and lose data.
#
#    Lag is only meaningful as a derivative. The absolute number tells you
#    nothing without knowing whether it is growing.
# ---------------------------------------------------------------------
aws cloudwatch put-metric-alarm \
    --alarm-name plh-kinesis-iterator-age \
    --alarm-description "Consumer falling behind the stream" \
    --namespace AWS/Lambda --metric-name IteratorAge \
    --dimensions Name=FunctionName,Value="$FUNCTION" \
    --statistic Maximum --period 300 --evaluation-periods 2 \
    --threshold 60000 --comparison-operator GreaterThanThreshold \
    --treat-missing-data notBreaching \
    --alarm-actions "$TOPIC_ARN" --region "$REGION"
echo "  [2/4] plh-kinesis-iterator-age  (>60s behind)"

# ---------------------------------------------------------------------
# 3. Match rate floor — a DATA QUALITY alarm, not an infrastructure one.
#    Every job can succeed while the match rate collapses because an
#    upstream system changed an ID format. Nothing errors. Nothing retries.
#    This is the alarm that catches a silently-wrong pipeline.
#    Metric is published by the Airflow DAG's publish_metrics task.
# ---------------------------------------------------------------------
aws cloudwatch put-metric-alarm \
    --alarm-name plh-match-rate-low \
    --alarm-description "Payment/invoice match rate below floor" \
    --namespace "$NS" --metric-name MatchRate \
    --statistic Average --period 86400 --evaluation-periods 1 \
    --threshold 50 --comparison-operator LessThanThreshold \
    --treat-missing-data breaching \
    --alarm-actions "$TOPIC_ARN" --region "$REGION"
echo "  [3/4] plh-match-rate-low        (<50%)"

# ---------------------------------------------------------------------
# 4. Volume anomaly / freshness.
#    treat-missing-data=breaching is the point: if the pipeline does not
#    run at all, no metric is published, and THAT fires the alarm.
#    An alarm that only fires on bad data cannot detect a dead pipeline.
# ---------------------------------------------------------------------
aws cloudwatch put-metric-alarm \
    --alarm-name plh-volume-anomaly \
    --alarm-description "Daily volume collapsed or pipeline did not run" \
    --namespace "$NS" --metric-name PaymentsProcessed \
    --statistic Sum --period 86400 --evaluation-periods 1 \
    --threshold 100 --comparison-operator LessThanThreshold \
    --treat-missing-data breaching \
    --alarm-actions "$TOPIC_ARN" --region "$REGION"
echo "  [4/4] plh-volume-anomaly        (<100/day OR no data at all)"

echo
aws cloudwatch describe-alarms --alarm-names "${ALARMS[@]}" --region "$REGION" \
    --query 'MetricAlarms[].[AlarmName,StateValue]' --output table

echo
echo "first 10 alarms are free; these 4 cost nothing."
echo "remove with: ./infra/alarms.sh --delete"
