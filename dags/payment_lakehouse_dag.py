"""
Orchestrates bronze -> silver -> gold with a reconciliation gate.

The transforms run as PythonOperator here because the jobs are pandas. The
Glue variant swaps them for GlueJobOperator; dependencies and the gate are
unchanged:

    silver = GlueJobOperator(
        task_id="bronze_to_silver",
        job_name="payment-bronze-to-silver",
        script_args={"--AS_OF": "{{ data_interval_end }}"},
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

PROJECT = Path(__file__).resolve().parents[1]
STATES = ["MATCHED", "AMOUNT_MISMATCH", "PENDING", "EXPIRED"]


def _config() -> dict:
    with open(PROJECT / "conf" / "config.json", encoding="utf-8") as fh:
        return json.load(fh)


def alert_on_failure(context) -> None:
    ti = context["task_instance"]
    print(
        f"FAILED {context['dag'].dag_id}.{ti.task_id} "
        f"@ {context['logical_date']}: {context.get('exception')}\n{ti.log_url}"
    )


def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    # SLA is measured from the DAG run start, not from when the task started.
    print(f"SLA MISS {dag.dag_id}: {task_list}")


default_args = {
    "owner": "harshit",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": alert_on_failure,
}


@dag(
    dag_id="payment_lakehouse",
    description="bronze -> silver -> gold with a reconciliation gate",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 10, 20, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,              # set explicitly; the default is True
    max_active_runs=1,          # concurrent runs would race the same partitions
    dagrun_timeout=timedelta(hours=2),
    sla_miss_callback=sla_miss_alert,
    params={
        "source": Param(default="s3", enum=["s3", "local"]),
        "sink": Param(default="s3", enum=["s3", "local"]),
    },
    tags=["payments", "lakehouse", "medallion"],
    doc_md=__doc__,
)
def payment_lakehouse():

    def _bronze_has_data(**context) -> bool:
        import boto3

        cfg = _config()
        response = boto3.client("s3", region_name=cfg["region"]).list_objects_v2(
            Bucket=cfg["bucket"], Prefix="bronze/", MaxKeys=1
        )
        found = response.get("KeyCount", 0) > 0
        print(f"bronze objects present: {found}")
        return found

    # reschedule, not poke: poke holds a worker slot for the whole wait, and
    # enough waiting sensors will deadlock the pool.
    wait_for_bronze = PythonSensor(
        task_id="wait_for_bronze",
        python_callable=_bronze_has_data,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
        soft_fail=False,
    )

    def _run(script: str, *args) -> None:
        cmd = [sys.executable, str(PROJECT / "src" / script), *args]
        print("running:", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise AirflowFailException(f"{script} exited {result.returncode}")

    @task(task_id="bronze_to_silver")
    def bronze_to_silver(**context):
        params = context["params"]
        _run("silver_job.py", "--source", params["source"], "--sink", params["sink"])

    @task(task_id="silver_to_gold")
    def silver_to_gold(**context):
        params = context["params"]
        # data_interval_end, not now(): PENDING vs EXPIRED depends on the date
        # asked, so using wall-clock time would make backfills rewrite history.
        as_of = context["data_interval_end"].isoformat()
        _run("gold_job.py", "--source", params["source"], "--sink", params["sink"],
             "--as-of", as_of)

    @task(task_id="reconciliation_check")
    def reconciliation_check(**context):
        """
        Reads gold back and fails the run if the totals do not hold. A pipeline
        that completes with wrong numbers is worse than one that fails, because
        a green run does not get investigated.
        """
        import io

        import boto3
        import pandas as pd

        cfg = _config()
        s3 = boto3.client("s3", region_name=cfg["region"])
        body = s3.get_object(
            Bucket=cfg["bucket"], Key="gold/fct_payment_state/part-000.parquet"
        )["Body"].read()
        fct = pd.read_parquet(io.BytesIO(body))

        counts = fct["state"].value_counts().to_dict()
        accounted = sum(counts.get(s, 0) for s in STATES)
        print({s: counts.get(s, 0) for s in STATES})

        if accounted != len(fct):
            raise AirflowFailException(
                f"control total broken: {accounted} states != {len(fct)} payments"
            )

        unknown = set(counts) - set(STATES)
        if unknown:
            raise AirflowFailException(f"unexpected states in gold: {unknown}")

        # A collapsed match rate usually means a broken join or an upstream
        # ID format change, neither of which raises on its own.
        match_rate = (counts.get("MATCHED", 0) + counts.get("AMOUNT_MISMATCH", 0)) / len(fct)
        print(f"match rate: {match_rate:.1%}")
        if match_rate < 0.50:
            raise AirflowFailException(f"match rate {match_rate:.1%} below 50% floor")

        return {"payments": len(fct), "match_rate": round(match_rate, 4)}

    @task(task_id="publish_metrics")
    def publish_metrics(summary: dict, **context):
        import boto3

        cfg = _config()
        boto3.client("cloudwatch", region_name=cfg["region"]).put_metric_data(
            Namespace="PaymentLakehouse",
            MetricData=[
                {"MetricName": "PaymentsProcessed", "Value": summary["payments"], "Unit": "Count"},
                {"MetricName": "MatchRate", "Value": summary["match_rate"] * 100, "Unit": "Percent"},
            ],
        )
        print(f"published: {summary}")

    # ALL_DONE so cleanup runs after a failure too, which is when it matters.
    cleanup = BashOperator(
        task_id="cleanup",
        bash_command="rm -rf /tmp/payment-lakehouse/{{ ds }} || true",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    done = EmptyOperator(task_id="done")

    silver = bronze_to_silver()
    gold = silver_to_gold()
    summary = reconciliation_check()
    metrics = publish_metrics(summary)

    wait_for_bronze >> silver >> gold >> summary >> metrics >> done
    metrics >> cleanup


payment_lakehouse()
