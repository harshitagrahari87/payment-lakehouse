# Payment Reconciliation Lakehouse

One business problem, built twice: once on **AWS** (Kinesis, Lambda, Glue, S3,
Athena) and once on **Databricks** (Auto Loader, Delta, Unity Catalog, Lakeflow
Declarative Pipelines).

Both implementations ingest payment and invoice events, match them under a
region-configurable retry window, and produce a daily reconciliation report that
**must balance to the payment**. Both reconcile to the same figures, which is how
the second one was verified against the first.

Every number below was measured on live runs, not estimated.

```
AWS          Kinesis -> Lambda -> S3 bronze -> silver -> gold -> Athena
Databricks   Auto Loader -> Delta bronze -> silver -> gold -> Lakeflow pipeline

9,446 events        MATCHED 4,115 · MISMATCH 143 · PENDING 487
                    EXPIRED 192 · REJECTED 63        — identical on both
```

---

## 1. The business problem

> In cross-border payments **the money arrives before the paperwork.** Customs
> clearance lags by days, so a payment lands with no invoice to apply it against.
>
> It must be recorded immediately — it is real money and it is auditable — but it
> cannot be applied until the matching invoice appears. How long to wait before
> writing it off varies by region, because clearance times do.
>
> Finance needs one question answered defensibly every morning: **does everything
> we received equal everything we matched, are still holding, or have written off?**
> If that does not balance, the pipeline lost money somewhere.

Every design decision below traces back to a constraint in that paragraph.

---

## 2. Architecture

### AWS

```mermaid
flowchart TD
    P["producer.py<br/>synthetic events<br/>+ deliberate defects"]
    K["Kinesis Data Stream<br/>1 shard · PK = payment_id"]
    L["Lambda<br/>idempotent consumer<br/>partial batch response"]
    B["S3 BRONZE<br/>raw JSON · append-only<br/>partitioned by INGEST date"]
    S["Glue / silver_job<br/>parse · validate · quarantine<br/>dedup on event_id"]
    SV["S3 SILVER<br/>Parquet<br/>partitioned by EVENT date"]
    Q["S3 QUARANTINE<br/>bad rows kept, never dropped"]
    G["Glue / gold_job<br/>match payments to invoices<br/>retry window · state machine"]
    GD["S3 GOLD<br/>fct_payment_state<br/>dim_region<br/>reconciliation_daily"]
    A["Athena<br/>Glue Data Catalog"]
    AF["Airflow DAG<br/>sensor → silver → gold →<br/>reconciliation gate → metrics"]

    P -->|put_records| K --> L --> B --> S
    S --> SV
    S --> Q
    SV --> G --> GD --> A
    AF -.orchestrates.-> S
    AF -.orchestrates.-> G
```

### Databricks

```mermaid
flowchart TD
    V["UC Volume<br/>landing/events"]
    AL["Auto Loader<br/>cloudFiles · rescue mode<br/>availableNow trigger"]
    BB["bronze_events<br/>Delta streaming table<br/>partitioned by ingest_dt"]
    ET["events_typed (view)<br/>watermark 7d<br/>dropDuplicatesWithinWatermark"]
    SP["silver_payments<br/>silver_invoices<br/>expectations enforced"]
    QT["silver_quarantine<br/>inverted expectation"]
    FG["fct_payment_state<br/>MATERIALIZED VIEW<br/>state machine"]
    RD["reconciliation_daily"]

    V --> AL --> BB --> ET
    ET --> SP --> FG --> RD
    ET --> QT
```

**The partition key changes meaning between layers, and that is deliberate.**
Bronze partitions by **ingest** date — when we received it. Silver and gold by
**event** date — when it happened. A six-day-late invoice lands in a *fresh*
ingest partition, so a forward-only read still catches it; only the write reaches
back in time. Partition bronze by event date and late arrivals land in partitions
already processed.

---

## 3. The state machine

```
PAYMENT_RECEIVED
      |
      +-- invoice found, amounts agree ------> MATCHED
      |
      +-- invoice found, amounts differ -----> AMOUNT_MISMATCH   (flagged for finance)
      |
      +-- no invoice yet
               |
               +-- within region's retry window --> PENDING
               |
               +-- past the window ---------------> EXPIRED      (exception report)
```

The retry window is **data, not code** — `conf/config.json`, per region.
US = 3 days, AE = 10. The business changes clearance windows without a deploy.

---

## 4. Results

### Correctness — the pipeline proves itself

The generator injects defects and writes an **answer key**. Every layer reconciles
against it:

| | Expected | Actual |
|---|---:|---:|
| Unique events after dedup | 9,258 | **9,258** |
| Quarantined (malformed) | 63 | **63** |
| MATCHED | 4,115 | **4,115** |
| AMOUNT_MISMATCH | 143 | **143** |
| PENDING | 487 | **487** |
| EXPIRED | 192 | **192** |

Control identity holds:
`received = matched + mismatch + pending + expired + rejected`

**Idempotency is demonstrated, not claimed.** Re-ingesting the entire stream
produces identical object counts, because the S3 key derives from batch content
rather than a timestamp.

### Cost — Athena bills on bytes scanned

| Query | Format | Scanned |
|---|---|---:|
| `count(*)` | bronze JSON | 4.2 MB |
| `count(*)` | gold Parquet | **0 B** |
| 2 columns, grouped | gold Parquet | 1.9 KB |
| `SELECT *` | gold Parquet | 274.9 KB |

`count(*)` on Parquet scans **zero bytes** — row counts live in the file footer,
so Athena answers from metadata without opening a data page. JSON has no footer
and must parse all 4.2 MB. `SELECT *` costs **144x** more than naming two columns.

Full analysis: [`notes/costs.md`](notes/costs.md)

### Resilience — a real failure, caught

Pushing 9,446 records at a 1-shard stream throttled **639 of them**. `PutRecords`
returned **HTTP 200** with `FailedRecordCount: 639`. Without resending only the
failed subset those payments would have vanished silently, and every count would
still have looked plausible. The reconciliation is what proves they arrived.

### The business answer the pipeline produced

| Region | Retry window | Write-off rate |
|---|---:|---:|
| US | 3 days | **6.5%** |
| EU | 5 days | 4.1% |
| SG | 4 days | 3.9% |
| IN | 7 days | 3.5% |
| AE | 10 days | **0.0%** |

The US writes off payments at nearly **twice** India's rate — not because US
payments are worse, but because its matching window is 3 days instead of 7.
Extending it toward the 5-7 day norm would recover much of the difference.

---

## 5. Design decisions

| Decision | Chose | Over | Because | Would revisit if |
|---|---|---|---|---|
| Ingestion | Kinesis | Kafka / MSK | AWS-native, no brokers to run; 1 shard covers the 116/s peak | we needed >7d replay, Connect, or Schema Registry |
| Dedup key | `event_id` | `payment_id` | a redelivery repeats event_id; a *correction* reuses payment_id with a new event_id and must survive | — |
| Bronze format | raw JSON string | parsed columns | bronze must accept malformed input; parsing there lets one bad record break ingestion | — |
| S3 object key | hash of batch contents | timestamp / uuid | a retry overwrites an identical object instead of duplicating 100 records | — |
| Bronze partition | ingest date | event date | late events land in a fresh partition, so forward-only reads still catch them | — |
| Silver partition | event date | ingest date | queries ask "what happened on the 14th", not "what arrived on the 26th" | — |
| Bad records | quarantine table | drop | in finance you never silently discard a transaction | — |
| Retry window | config file | code | the business changes clearance windows without a deploy | — |
| `as_of` | DAG `data_interval_end` | `now()` | PENDING vs EXPIRED depends on the date asked; `now()` makes backfills rewrite history | — |
| Compute | pandas | PySpark | 9,446 records — Spark's JVM and shuffle overhead exceed the work | volume reaches ~3 GB/day, then Glue Spark |
| Serving | Athena on S3 | Redshift | spiky analyst queries, pay-per-query, no idle cluster | sustained BI concurrency needing WLM |
| Table format | Parquet | Iceberg / Delta | no concurrent writers or row-level updates when the choice was made | **this has already bitten** — two writers in one prefix inflated every count, since a Hive-style table is defined by directory listing rather than a manifest. Migrating gold to Iceberg is the next change. |

---

## 6. Repo layout

```
src/                       AWS — producer, Lambda handler, pandas jobs, Athena
glue/                      AWS — PySpark jobs for Glue
dags/                      AWS — Airflow DAG with a reconciliation gate
infra/                     AWS — deploy, teardown, alarms, Glue deployment
sql/                       AWS — explicit external table definitions

databricks/
  01_setup.ipynb           Unity Catalog: catalog, schema, volume
  02_producer.ipynb        event generator + control-total answer key
  03_bronze.ipynb          Auto Loader streaming into a Delta table
  04_silver.ipynb          watermark, dedup, foreachBatch MERGE, quarantine
  05_gold.ipynb            state machine, reconciliation_daily, control total
  06_dlt_pipeline.py       the same pipeline declaratively, with expectations

tests/                     transformation tests
notes/                     failures.md (incident log), costs.md (measurements)
conf/                      region retry windows, defect rates
```

---

## 7. The Databricks implementation

Built on Databricks Free Edition. Same business logic, same data, same answer
key — verified against the AWS output row for row.

| | |
|---|---|
| **Ingestion** | Auto Loader (`cloudFiles`) with `schemaEvolutionMode=rescue`, `availableNow` trigger — batch economics with streaming semantics |
| **Late data** | `withWatermark("event_ts", "7 days")` + `dropDuplicatesWithinWatermark` — bounded dedup state matching the longest regional retry window |
| **Upserts** | `foreachBatch` + Delta `MERGE`; the native streaming sink only appends |
| **Governance** | Unity Catalog three-level namespace, managed tables, a Volume for the landing zone |
| **Declarative** | A Lakeflow pipeline building the same seven datasets with expectations |
| **Quality** | `expect` to flag, `expect_or_drop` to reject, and a second dataset with the inverted condition so nothing is lost silently |
| **Late-data modelling** | Gold is a **materialized view**, not a streaming table — a late invoice must revise an already-computed aggregate, which append-only cannot do |

### Three bugs worth reading `notes/failures.md` for

```
Auto Loader inferred `amount` as STRING, because the generator injects
malformed values. A `<= 0` comparison then cast to BIGINT and killed the
stream. try_cast turns it into a quarantined row instead of an outage.

datediff() counts calendar boundaries, not elapsed time. Ten payments on
the boundary were written off as expired. Every job reported SUCCESS.

NOT (region IN (...)) where region IS NULL evaluates to NULL, not TRUE.
Nine malformed records passed through neither the clean table nor the
quarantine table — lost by the very thing built to catch them.
```

All three were caught the same way: the generator writes an answer key and every
layer reconciles against it.

---

## 8. How to run

### AWS

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# local — no AWS required
python3 src/producer.py --count 5000 --seed 42
python3 src/bronze_writer.py --batch-size 100
python3 src/silver_job.py
python3 src/gold_job.py

# on AWS
./infra/deploy.sh
python3 src/producer.py --count 5000 --seed 42 --sink kinesis
python3 src/silver_job.py --source s3 --sink s3
python3 src/gold_job.py   --source s3 --sink s3
python3 src/athena.py ddl && python3 src/athena.py bench
./infra/teardown.sh
```

### Databricks

Import `databricks/` into a workspace and run the notebooks in order. They
create their own catalog, schema and volume, so no prior setup is needed.

For the declarative version, create a Lakeflow pipeline pointing at
`databricks/06_dlt_pipeline.py`, with a target catalog and schema of your
choosing, and run it.

`--seed` makes every run byte-identical, so a bug is the same bug twice.

---

## 9. Teardown

```bash
./infra/teardown.sh          # stream, lambda, mapping, IAM role
./infra/teardown.sh --all    # also empties and deletes the bucket
```

A Kinesis shard bills around $0.36/day whether or not data flows through it,
so the stream is created per session and removed at the end of it. S3 and
Athena at this volume are negligible.
