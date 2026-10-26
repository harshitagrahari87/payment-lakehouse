#!/usr/bin/env bash
#
# deploy_glue.sh — upload the PySpark scripts and create the two Glue jobs.
#
#     ./infra/deploy_glue.sh          create/update jobs
#     ./infra/deploy_glue.sh run      create/update, then run both and wait
#     ./infra/deploy_glue.sh delete   remove jobs and the role
#
# COST: Glue bills per DPU-hour with a 1-minute minimum and a 2-DPU floor.
# A short job here is roughly $0.01-0.03. Not free, but not Kinesis either.
# Glue has no idle cost — unlike a cluster, nothing bills between runs.
#
set -euo pipefail

cd "$(dirname "$0")/.."

BUCKET=$(python3 -c "import json;print(json.load(open('conf/config.json'))['bucket'])")
REGION=$(python3 -c "import json;print(json.load(open('conf/config.json'))['region'])")
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

ROLE="payment-glue-role"
SILVER_JOB="payment-bronze-to-silver"
GOLD_JOB="payment-silver-to-gold"

if [ "${1:-}" = "delete" ]; then
    aws glue delete-job --job-name "$SILVER_JOB" --region "$REGION" 2>/dev/null || true
    aws glue delete-job --job-name "$GOLD_JOB"   --region "$REGION" 2>/dev/null || true
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
    aws iam delete-role --role-name "$ROLE" >/dev/null 2>&1 || true
    echo "glue jobs and role deleted"
    exit 0
fi

# --------------------------------------------------------------------
# 1. IAM role for Glue
# --------------------------------------------------------------------
echo "[1/3] iam role"
if ! aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
    aws iam create-role --role-name "$ROLE" \
        --assume-role-policy-document '{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }' >/dev/null
    echo "      created"
else
    echo "      exists"
fi

aws iam attach-role-policy --role-name "$ROLE" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# S3 access needs two statements: ListBucket is a bucket-level action and
# GetObject/PutObject are object-level, so they take different ARNs.
aws iam put-role-policy --role-name "$ROLE" --policy-name s3-lakehouse \
    --policy-document "{
        \"Version\": \"2012-10-17\",
        \"Statement\": [
            {
                \"Effect\": \"Allow\",
                \"Action\": [\"s3:ListBucket\", \"s3:GetBucketLocation\"],
                \"Resource\": \"arn:aws:s3:::$BUCKET\"
            },
            {
                \"Effect\": \"Allow\",
                \"Action\": [\"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\"],
                \"Resource\": \"arn:aws:s3:::$BUCKET/*\"
            }
        ]
    }"
ROLE_ARN="arn:aws:iam::$ACCOUNT:role/$ROLE"
echo "      policies attached"

# --------------------------------------------------------------------
# 2. Upload the scripts
# --------------------------------------------------------------------
echo "[2/3] upload scripts"
aws s3 cp glue/glue_silver_job.py "s3://$BUCKET/scripts/" --region "$REGION" --only-show-errors
aws s3 cp glue/glue_gold_job.py   "s3://$BUCKET/scripts/" --region "$REGION" --only-show-errors
echo "      s3://$BUCKET/scripts/"

# --------------------------------------------------------------------
# 3. Create or update the jobs
#
#    NumberOfWorkers 2, G.1X = the minimum. Sizing notes:
#      worker COUNT = parallelism  (fixes "too slow")
#      worker SIZE  = memory       (fixes OOM)
#    Different levers for different problems — neither fixes skew.
#
#    --job-bookmark-option: disabled here because the jobs are written to be
#    fully idempotent (overwrite by partition). Bookmarks are for append-only
#    incremental reads, and they make backfills confusing.
# --------------------------------------------------------------------
create_or_update () {
    local name="$1" script="$2" extra="$3"
    local cmd="{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET/scripts/$script\",\"PythonVersion\":\"3\"}"
    local defaults="{\"--BUCKET\":\"$BUCKET\",\"--job-language\":\"python\",\"--enable-metrics\":\"true\",\"--enable-spark-ui\":\"true\",\"--spark-event-logs-path\":\"s3://$BUCKET/spark-logs/\",\"--job-bookmark-option\":\"job-bookmark-disable\"$extra}"

    if aws glue get-job --job-name "$name" --region "$REGION" >/dev/null 2>&1; then
        aws glue update-job --job-name "$name" --region "$REGION" \
            --job-update "{\"Role\":\"$ROLE_ARN\",\"Command\":$cmd,\"DefaultArguments\":$defaults,\"GlueVersion\":\"4.0\",\"WorkerType\":\"G.1X\",\"NumberOfWorkers\":2,\"Timeout\":20}" >/dev/null
        echo "      updated $name"
    else
        aws glue create-job --name "$name" --region "$REGION" \
            --role "$ROLE_ARN" --command "$cmd" --default-arguments "$defaults" \
            --glue-version 4.0 --worker-type G.1X --number-of-workers 2 --timeout 20 >/dev/null
        echo "      created $name"
    fi
}

echo "[3/3] glue jobs"
create_or_update "$SILVER_JOB" glue_silver_job.py ""
create_or_update "$GOLD_JOB"   glue_gold_job.py   ",\"--AS_OF\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\""

if [ "${1:-}" = "run" ]; then
    echo
    for name in "$SILVER_JOB" "$GOLD_JOB"; do
        RUN_ID=$(aws glue start-job-run --job-name "$name" --region "$REGION" \
                 --query JobRunId --output text)
        echo "running $name ($RUN_ID)"
        while true; do
            STATE=$(aws glue get-job-run --job-name "$name" --run-id "$RUN_ID" \
                    --region "$REGION" --query 'JobRun.JobRunState' --output text)
            case "$STATE" in
                SUCCEEDED)
                    SECS=$(aws glue get-job-run --job-name "$name" --run-id "$RUN_ID" \
                           --region "$REGION" --query 'JobRun.ExecutionTime' --output text)
                    echo "  SUCCEEDED in ${SECS}s"
                    break ;;
                FAILED|TIMEOUT|STOPPED)
                    aws glue get-job-run --job-name "$name" --run-id "$RUN_ID" \
                        --region "$REGION" --query 'JobRun.ErrorMessage' --output text
                    echo "  $STATE — logs: /aws-glue/jobs/output"
                    exit 1 ;;
            esac
            sleep 10
        done
    done
fi

echo
echo "run with   : ./infra/deploy_glue.sh run"
echo "logs       : aws logs tail /aws-glue/jobs/output --since 10m --region $REGION"
echo "remove     : ./infra/deploy_glue.sh delete"
