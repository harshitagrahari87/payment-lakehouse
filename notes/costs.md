# Cost & Performance Measurements

All figures measured on the live pipeline in `ap-southeast-2`, Oct 26 2026.
5,000 payments -> 9,446 events. Athena engine v3.

---

## 1. Storage by layer

| Layer | Objects | Size | Format | Contents |
|---|---:|---:|---|---|
| bronze | 95 | 4,400,182 B (4.20 MB) | JSON | 9,446 raw events incl. 188 duplicates |
| silver | 27 | 922,573 B (0.88 MB) | Parquet | 9,195 clean + 63 quarantined |
| gold | 3 | 299,665 B (0.29 MB) | Parquet | 5,000 payment states + dim + recon |

**4.8x reduction bronze -> silver.** Attributable to three separate causes,
and it is worth separating them rather than claiming it is all compression:

1. Parquet + dictionary encoding vs JSON
2. 188 duplicate events removed
3. bronze stores the original message as an *escaped* string, which roughly
   doubles the quoting

The clean, format-only number is the query benchmark below.

---

## 2. Bytes scanned — the number Athena actually bills

| Query | Layer / format | Scanned |
|---|---|---:|
| `count(*)` | bronze, JSON | **4.2 MB** |
| `count(*)` | gold, Parquet | **0 B** |
| `SELECT region, count(*) GROUP BY region` | gold, Parquet | **1.9 KB** |
| `SELECT *` | gold, Parquet | **274.9 KB** |
| `WHERE raw LIKE '%"region": "IN"%'` | bronze, JSON | **4.2 MB** |
| `SELECT state, count(*) GROUP BY state` | gold, Parquet | **1.2 KB** |
| fact-to-dim join, filtered on state | gold, Parquet | **3.4 KB** |

### Why `count(*)` on Parquet scans zero bytes
Parquet writes row counts and per-column statistics into the **file footer**.
Athena answers `count(*)` from that metadata and never opens a data page.
JSON has no footer and no statistics, so the same question requires parsing
every byte.

**Same question. 4.2 MB versus 0 B.**

### Column pruning
```
SELECT region, count(*)   ->   1.9 KB     2 of 16 columns
SELECT *                  -> 274.9 KB     all 16 columns
                             = 144x
```
`SELECT *` defeats columnar storage entirely. This is the single most common
self-inflicted Athena cost.

---

## 3. What this costs, honestly

Athena bills **$5/TB scanned with a 10 MB minimum per query.** At this data
volume every query above costs the same **$0.000048** — the minimum.

So the dollar figures are meaningless here; **the ratios are the point.**
Projected to 3 years of production volume (2M payments/day, ~3 GB/day raw,
~3.2 TB):

| Pattern | Scanned per full-history query | Cost |
|---|---:|---:|
| JSON, no partitioning | ~3.2 TB | **~$16.00** |
| Parquet, no partitioning | ~0.67 TB | ~$3.35 |
| Parquet + partition pruning (1 day) | ~0.6 GB | **~$0.003** |

One analyst running 20 investigation queries a day is the difference between
**~$320/day** and **~$0.06/day**. That is the whole argument for the layout.

---

## 4. Current AWS spend

```
Charges to date        $0.00
Credits remaining      $120.00 (expires Apr 25, 2027)
Hard monthly cap       $20 spend limit + early cost controls
Running compute        none — no Kinesis stream, no Glue job, no cluster
```

S3 at 5.4 MB total and Athena at the 10 MB minimum are rounding errors.
**The only thing that would bill meaningfully is a forgotten Kinesis shard
(~$0.36/day)** — hence `infra/teardown.sh` before the stream is created.

---

## 4. Streaming path — measured Oct 26

Full run: `producer --sink kinesis` -> Kinesis (1 shard) -> Lambda -> S3 bronze.

| | |
|---|---|
| Records sent | 9,446 |
| **Throttled and resent** | **639** |
| Lambda invocations | 95 (94 full batches of 100, one of 46) |
| Lambda duration | 35-50 ms typical, 136 ms max |
| Memory allocated / used | 256 MB / **98 MB** |
| Bronze objects landed | 95 |
| End-to-end reconciliation | ALL MATCH |

### The 639 throttled records
One shard accepts 1,000 records/sec or 1 MB/sec. `PutRecords` returned
**HTTP 200 with `FailedRecordCount: 639`** — a partial failure, not an error.

Without resending only the failed subset, 639 payments would have been lost
silently and the pipeline would still have reported success. The reconciliation
against the answer key is what proves they all arrived.

**A 200 from PutRecords does not mean every record landed.**

### Batch flush behaviour
Batches of 100 flushed on `BatchSize`; the final batch of 46 flushed on
`MaximumBatchingWindowInSeconds=10`. The event source mapping polls and
batches — the function never implements a poll loop.

### Lambda right-sizing
98 MB used of 256 MB allocated. Lambda CPU scales **with** memory, so halving
memory also halves CPU; at 35-50 ms per invocation the cost difference is
negligible either way. Memory here is a performance dial, not a limit.

### Cost of this run
```
Kinesis   1 shard  ~$0.36/day, deleted after ~8 minutes   ~= $0.002
Lambda    95 invocations x ~45 ms @ 256 MB                 free tier
S3        95 PUTs + 4.4 MB                                 fractions of a cent
```
`infra/teardown.sh` removed the stream, function, mapping and IAM role
immediately afterwards. Verified: `list-streams` returned empty.

---

## 5. Business finding the pipeline produced

| Region | Retry window | Expired | Payments | Write-off rate |
|---|---:|---:|---:|---:|
| US | 3 days | 64 | 989 | **6.5%** |
| EU | 5 days | 31 | 755 | 4.1% |
| SG | 4 days | 30 | 760 | 3.9% |
| IN | 7 days | 67 | 1,926 | 3.5% |
| AE | 10 days | 0 | 507 | **0.0%** |

The US writes off payments at roughly **twice** the rate of India, and the
driver is not payment quality — it is the 3-day matching window. Extending it
toward the 5-7 day norm would recover a large share of those write-offs.

This is the output the whole pipeline exists to produce.
