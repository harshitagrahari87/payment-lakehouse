# Payment Reconciliation Lakehouse

An end-to-end AWS data platform that ingests payment and invoice events, matches
them under a region-configurable retry window, and produces a daily reconciliation
report that must balance to the event.

---

## 1. The business problem

> In cross-border payments, **the payment arrives before the invoice.** Customs
> clearance lags by days, so money lands with no invoice reference to apply it
> against.
>
> The payment must be recorded the moment it arrives — it is real money and it is
> auditable — but it cannot be applied until the matching invoice appears. How long
> we wait before writing it off varies by region, because clearance times do.
>
> Finance needs one question answered every morning, defensibly:
> **does everything we received equal everything we matched, are still holding, or
> have written off?** If that does not balance, the pipeline lost money somewhere.

## 2. Requirements

| # | Requirement | Why it drives design |
|---|---|---|
| R1 | Ingest payment + invoice events continuously | Streaming ingest, not nightly batch |
| R2 | Match payments to invoices as invoices arrive | Repeated MERGE over an open window |
| R3 | Tolerate invoices up to N days late, N per region | Retry window as **data**, not code |
| R4 | A duplicate event must never double-count | Idempotency on `event_id` |
| R5 | Every state change traceable | Append-only bronze + audit columns |
| R6 | Daily reconciliation must balance | `received = matched + pending + expired + rejected` |

## 3. Scale

```
2,000,000 payments/day  x  ~1.5 KB        =  ~3 GB/day raw
3 GB / 86,400 s                            =  ~23 events/sec average
x 5 peak factor                            =  ~116 events/sec peak
116 x 1.5 KB                               =  ~0.17 MB/s peak
```

**Implication:** one Kinesis shard covers this with room to spare (1 MB/s, 1000 rec/s).
**This is a correctness problem, not a scale problem** — and the architecture is
chosen accordingly. Nothing here is sized for throughput; everything is sized for
"never lose or double-count a payment."

## 4. Non-goals — deliberately not built

- Sub-second decisioning. Nothing blocks on this pipeline.
- ML / fraud scoring.
- Multi-region active-active.
- A serving API. Consumers are analysts via Athena, not applications.

## 5. Architecture

```
producer.py  (synthetic payments + invoices, with deliberate bad data)
      |
      v
Kinesis Data Stream          1 shard, partition key = payment_id
      |
      v
Lambda consumer              idempotent, batched
      |
      v
S3 BRONZE                    raw JSON, partitioned by ingest_dt / hour, append-only
      |
      v
Glue Job 1  bronze -> silver     dedup on event_id, cast, quarantine bad rows
      |
      v
S3 SILVER                    Iceberg, partitioned by event_dt
      |
      v
Glue Job 2  silver -> gold       MERGE payments <-> invoices, retry window,
      |                          state machine PENDING / MATCHED / EXPIRED
      v
S3 GOLD                      fct_payment_state, dim_region, reconciliation_daily
      |
      +--> Athena            analyst queries + daily reconciliation report
      +--> CloudWatch        freshness, volume anomaly, unmatched-rate alarms

Airflow DAG orchestrates:  Glue 1 -> Glue 2 -> reconciliation check -> publish metrics
```

**Key rule applied:** bronze is **read by ingest date, written by event date** into
silver. A late invoice lands in a fresh ingest partition, so a forward-only read
still catches it; only the write goes backwards in time.

## 6. Design decisions

| Decision | Chose | Over | Because | Would revisit if |
|---|---|---|---|---|
| *(filled in as built)* | | | | |

## 7. Results

| Metric | Before | After |
|---|---|---|
| *(filled in at Step 5)* | | |

## 8. Repo layout

```
src/     producer, Lambda handler, Glue job scripts
dags/    Airflow DAG
conf/    region retry windows, bucket + stream names
tests/   pytest over transformation logic
notes/   failures.md (the debugging log), costs.md
infra/   IAM policies, teardown script
```

## 9. How to run

*(filled in once the pipeline runs end to end)*

## 10. Teardown

```powershell
.\infra\teardown.ps1
```
Kinesis bills per shard-hour whether or not data flows. Tear down after every session.
