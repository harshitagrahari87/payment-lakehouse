"""
Registers the lakehouse tables in the Glue Catalog and measures query cost.

Athena bills on bytes scanned, so `bench` runs the same question against
different layouts and reports what each one read.

Usage:
    python3 src/athena.py ddl
    python3 src/athena.py query "SELECT state, count(*) FROM payment_lakehouse.fct_payment_state GROUP BY state"
    python3 src/athena.py bench
"""

from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[1]
CONF_PATH = ROOT / "conf" / "config.json"
DDL_PATH = ROOT / "sql" / "ddl.sql"

DB = "payment_lakehouse"
PRICE_PER_TB = 5.0
MIN_BILLED_BYTES = 10 * 1024 * 1024  # Athena rounds every query up to 10 MB


def load_config() -> dict:
    with open(CONF_PATH, encoding="utf-8") as fh:
        return json.load(fh)


class Athena:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.client = boto3.client("athena", region_name=cfg["region"])
        self.output = f"s3://{cfg['bucket']}/athena-results/"

    def run(self, sql: str) -> dict:
        qid = self.client.start_query_execution(
            QueryString=sql,
            ResultConfiguration={"OutputLocation": self.output},
        )["QueryExecutionId"]

        while True:
            execution = self.client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            state = execution["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)

        if state != "SUCCEEDED":
            reason = execution["Status"].get("StateChangeReason", "unknown")
            raise RuntimeError(f"{state}: {reason}\n  SQL: {sql[:160]}")

        stats = execution.get("Statistics", {})
        return {
            "id": qid,
            "scanned": stats.get("DataScannedInBytes", 0),
            "millis": stats.get("TotalExecutionTimeInMillis", 0),
        }

    def rows(self, qid: str, limit: int = 15) -> list[list[str]]:
        result = self.client.get_query_results(QueryExecutionId=qid, MaxResults=limit)
        return [
            [c.get("VarCharValue", "") for c in r["Data"]]
            for r in result["ResultSet"]["Rows"]
        ]


def split_statements(sql_text: str) -> list[str]:
    """Strip -- comments, split on ';', drop blanks."""
    no_comments = re.sub(r"--[^\n]*", "", sql_text)
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def cmd_ddl(athena: Athena, cfg: dict) -> None:
    sql_text = DDL_PATH.read_text(encoding="utf-8").replace("__BUCKET__", cfg["bucket"])
    for statement in split_statements(sql_text):
        label = " ".join(statement.split())[:70]
        athena.run(statement)
        print(f"  ok  {label}")
    print("\ncatalog ready.")


def cmd_query(athena: Athena, sql: str) -> None:
    result = athena.run(sql)
    for row in athena.rows(result["id"]):
        print("  " + " | ".join(row))
    print(f"\n  scanned {human(result['scanned'])} in {result['millis'] / 1000:.1f}s")


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} TB"


BENCHMARKS = [
    ("count rows — BRONZE (raw JSON)",
     f"SELECT count(*) FROM {DB}.bronze_events"),

    ("count rows — GOLD (Parquet)",
     f"SELECT count(*) FROM {DB}.fct_payment_state"),

    ("2 columns — GOLD (column pruning)",
     f"SELECT region, count(*) FROM {DB}.fct_payment_state GROUP BY region"),

    ("SELECT * — GOLD (pruning defeated)",
     f"SELECT * FROM {DB}.fct_payment_state"),

    ("scan the raw JSON payload — BRONZE",
     f"SELECT count(*) FROM {DB}.bronze_events WHERE raw LIKE '%\"region\": \"IN\"%'"),
]


def cmd_bench(athena: Athena) -> None:
    print(f"{'query':<40} {'scanned':>12} {'cost @ $5/TB':>14}")
    print("-" * 68)

    results = []
    for label, sql in BENCHMARKS:
        result = athena.run(sql)
        scanned = result["scanned"]
        billed = max(scanned, MIN_BILLED_BYTES)
        cost = billed / (1024 ** 4) * PRICE_PER_TB
        results.append((label, scanned))
        print(f"{label:<40} {human(scanned):>12} {'$' + format(cost, '.6f'):>14}")

    bronze_count = dict(results)["count rows — BRONZE (raw JSON)"]
    gold_count = dict(results)["count rows — GOLD (Parquet)"]
    gold_two_col = dict(results)["2 columns — GOLD (column pruning)"]
    gold_star = dict(results)["SELECT * — GOLD (pruning defeated)"]

    print("\n--- notes ---")
    if gold_count == 0:
        print(f"  count(*): JSON scanned {human(bronze_count)}, Parquet scanned 0 B.")
        print("  Parquet keeps row counts in the file footer, so Athena answers")
        print("  from metadata. JSON has no footer and must parse every byte.")
    else:
        print(f"  JSON vs Parquet, same question : {bronze_count / gold_count:>6.1f}x fewer bytes")

    if gold_two_col:
        print(f"\n  2 columns vs SELECT *          : {gold_star / gold_two_col:>6.1f}x fewer bytes")

    print("\n  Athena bills a 10 MB minimum per query, so at this volume every")
    print("  query costs the same. The ratios are what scale with data size.")


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cfg = load_config()
    athena = Athena(cfg)
    command = sys.argv[1]

    if command == "ddl":
        cmd_ddl(athena, cfg)
    elif command == "query":
        cmd_query(athena, sys.argv[2])
    elif command == "bench":
        cmd_bench(athena)
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
