# Failure Log

Problems hit while building this, how each was diagnosed, and what fixed it.
Kept because the diagnosis is usually more useful than the fix.

---

## 1. Secret access key exposed

**Severity:** critical — credential exposure on a role with administrator access

An `aws configure` session was captured in a context that echoed the secret
access key in plain text. Keys matching `AKIA...` are scraped aggressively and
exploited within hours.

Rotated immediately:

```bash
aws iam create-access-key --user-name <user>          # mint replacement
aws configure                                          # repoint the CLI
aws sts get-caller-identity                            # confirm new key works
aws iam update-access-key --user-name <user> \
    --access-key-id <OLD> --status Inactive            # deactivate
aws sts get-caller-identity                            # confirm still working
aws iam delete-access-key --user-name <user> \
    --access-key-id <OLD>                              # delete
aws iam list-access-keys --user-name <user>            # verify one key remains
```

Deactivate before delete: deactivation is reversible and revokes the
credential instantly, so if something still depended on the old key the
failure surfaces while rollback is still possible.

Blast radius checked with Cost Explorer — no usage.

**Controls added**

- `.gitignore` covers `*credentials*.csv`, `.env`, `*.pem`, `.aws/`, written
  before the first line of application code.
- The access key **ID** is safe to show; the **secret** is never echoed,
  logged or committed.

**Still owed:** replace the administrator policy with a least-privilege one
scoped to S3, Glue, Kinesis, Lambda and Athena.

---

## 2. Administrator access denied by a service control policy

**Severity:** blocker — no resources could be created

```
AccessDenied: User ... is not authorized to perform: s3:CreateBucket
with an explicit deny in a service control policy
```

The IAM user had `AdministratorAccess` attached.

The error names a **service control policy**, not an IAM policy, which
identifies the layer immediately. An SCP is a ceiling on a member account;
IAM grants operate inside that ceiling, and an explicit deny in an SCP cannot
be overridden by any IAM policy. No amount of IAM work would have helped.

**Diagnosis**

| Test | Result |
|---|---|
| Same call from CloudShell under a different role | succeeded — so not account-wide |
| `sts:AssumeRole` from the IAM user | denied by the same policy |
| Custom role created, then assumed | denied |
| `s3:CreateBucket` from the working role | denied |
| Plan tier upgraded | restriction persisted |

Conclusion: the deny was both principal-scoped and action-scoped, and
originated from a managed account restriction rather than a misconfiguration.

The real cause turned out to be region-scoped. See #4.

**Not done:** removing the policies would have required activating an
irreversible org feature that also deletes the account's spend limit.
Declining to remove a cost control in order to unblock a problem that was not
yet understood was the right call — and the actual cause made it unnecessary.

---

## 3. A re-ingestion doubled the rejected count, and the job reported success

**Severity:** silent data quality defect — no error, no alert, wrong numbers

The bronze writer ran twice. The object key is a hash of the batch contents,
so the second run should have overwritten the first. It did not: the key path
includes the ingest hour, and the two runs fell in different hours.

```
bronze/ingest_dt=.../hour=12/   95 files
bronze/ingest_dt=.../hour=13/   95 files   — every event, twice
```

Silver then read double the records and reported 126 quarantined against an
expected 63.

**Why nothing caught it**

The job completed with exit code 0. Valid records were deduplicated on
`event_id`; quarantined records were not, because deduplication ran only on
the good branch after the split. Only the reconciliation against known totals
exposed it, and the discrepancy was exactly the number of duplicated rejects.

**Fix:** `dedup_quarantine()`, applying the same first-arrival rule and
falling back to the raw bytes for records too malformed to carry an event_id.

**Takeaways**

1. Quarantine is a table and needs the same idempotency guarantees as the
   happy path. A duplicated reject corrupts the rejected total.
2. A deterministic key is only as deterministic as everything inside it. The
   hour component broke the guarantee across partitions, which is why
   event-level deduplication downstream is a second independent defence
   rather than redundancy.
3. Success is not correctness. Only a control total against a known figure
   catches this class of bug.

---

## 4. Three hours lost because the region was never varied

**Severity:** blocker — believed to be a service restriction, was a region one

Every data service appeared to be denied:

```
s3:CreateBucket        denied
kinesis:ListStreams    denied
glue:GetDatabases      denied
lambda:ListFunctions   denied
athena:ListWorkGroups  denied
```

All five carried an explicit deny from the same policy, which led to the
conclusion that the analytics stack was blocked outright.

**What was actually true:** the account is restricted to a single region. Every
failing call had been made against two other regions.

**The clue that was present throughout:** `aws s3 ls` succeeded the entire
time. `ListAllMyBuckets` is a global endpoint and carries no region; every
call that failed carried an explicit `--region`. CloudShell had also defaulted
to the permitted region from the start.

**The diagnostic error:** three hypotheses were tested by varying *principal*
(IAM user, custom role, managed role) and *action* (five different services).
Region was treated as fixed context rather than as a variable. One call to
`lambda:ListFunctions` in the permitted region returned successfully and
inverted the whole picture.

**Takeaways**

1. An AccessDenied has four dimensions — principal, action, resource, and
   condition (region, tags, source IP). Vary one at a time. Eliminating three
   principals and five services proved nothing, because the real discriminator
   was never under test.
2. A call that succeeds is as informative as one that fails. `s3 ls` working
   should have prompted the question "what is different about that call?"

---

## 5. PutRecords returned HTTP 200 and dropped 639 records

**Severity:** would have been silent data loss

9,446 events were sent to a single-shard stream as fast as the API allowed.
Every `PutRecords` call returned HTTP 200. Inside those responses, **639
records carried `ProvisionedThroughputExceededException`**.

A shard accepts 1,000 records/sec or 1 MB/sec. The producer exceeded the
record ceiling, and Kinesis rejected the excess per record, not per call.

```python
response = client.put_records(...)   # 200 OK, and incomplete
```

Nothing raises. A producer that checks only the HTTP status drops records
silently, and the loss is invisible until someone reconciles totals.

**Fix**

```python
failed = response["FailedRecordCount"]
if failed:
    # resend only the failed subset; resending the whole chunk would
    # duplicate the records that already succeeded
    chunk = [rec for rec, result in zip(chunk, response["Records"])
             if result.get("ErrorCode")]
```

with exponential backoff, up to five attempts. All 9,446 records subsequently
reached bronze and reconciled exactly.

**Takeaways**

1. `PutRecords` is a partial-failure API, as are DynamoDB `BatchWriteItem`
   (`UnprocessedItems`) and SQS `SendMessageBatch` (`Failed`). HTTP 200 means
   the call succeeded, not that every item did.
2. Resend the failed subset, never the whole batch.
3. Throttling is expected behaviour for a correctly sized shard under burst.
   Backpressure is the producer's responsibility.

---

## 6. Two writers, one prefix — every count doubled

**Severity:** silent — both jobs reported success, the table was wrong

The Spark and pandas implementations were both run against `gold/`. Each
printed a correct, internally consistent control total. Athena then returned
roughly double every figure.

**Cause:** the two writers use different file-naming conventions.

```
part-00000-<uuid>-c000.snappy.parquet    Spark
part-000.parquet                          pandas / pyarrow
```

Spark's `mode("overwrite")` removed the files it knew about; the pandas job
then wrote its own key alongside. Neither removed the other, and a Hive-style
external table is defined by whatever files sit under the prefix — so Athena
read both sets and summed them.

The counts were not exactly double because the two runs used different `as_of`
values, so a number of payments had moved from PENDING to EXPIRED between
them. Same payments, two snapshots, both live in the table at once.

**Why the reconciliation gate missed it:** each job validated its own
DataFrame in memory. Both were correct. Nothing validated the state of the
table afterwards.

**Fix:** both S3 writers now clear their prefix before writing.

**The structural lesson.** A Hive-style table is defined by directory listing;
a table format is defined by a transaction log naming exactly which files
constitute the current snapshot. Under Iceberg or Delta a stray file written
by another process is simply not part of the table, and an overwrite is an
atomic commit rather than a sequence of deletes and puts that another writer
can interleave with. Migrating gold to Iceberg is the next change.

**Takeaways**

1. Two writers in one prefix is a correctness bug, not an untidiness issue.
2. `overwrite` removes only what the writer knows about; it is not a
   directory truncation.
3. Validate the table, not just the DataFrame. A gate that reads back from
   the target after writing would have caught this.

---

## 7. Reconciliation failed because the fixture and its expectations diverged

**Severity:** low — false alarm, but it cost time

```
PENDING   expected 487   actual 449
EXPIRED   expected 192   actual 230
```

38 payments had moved between the two states. MATCHED, AMOUNT_MISMATCH and
REJECTED all matched exactly.

**Cause:** the bronze data had been generated on one day and the expected
totals regenerated on the next. The generator anchors `event_ts` to the
current time, so the same `--seed` reproduces the same random *choices* but
not the same absolute timestamps.

The pipeline was correct; the expectations described a different dataset.

**Fix:** regenerate the whole chain together.

---

## 8. Auto Loader typed a column as STRING and the cast killed the stream

**Severity:** pipeline outage

Building the Databricks implementation, the silver stream failed on the first
run:

```
CAST_INVALID_INPUT: the value '2461.65' of type STRING cannot be cast to BIGINT
```

**Cause:** the generator deliberately injects `"amount": "not-a-number"` for
about 1% of records. Auto Loader, inferring the schema, therefore typed `amount`
as STRING to accommodate every value it saw. The validation expression
`amount <= 0` then compared a STRING against an integer literal, which Spark
resolves by casting to BIGINT — and `'2461.65'` is not a valid BIGINT.

So a malformed record did not merely fail validation. It changed the inferred
type of the column, and the validation written to catch it became the thing that
crashed.

**Fix**

```python
.withColumn("amount", F.expr("try_cast(amount_raw AS DOUBLE)"))
```

`try_cast` returns NULL instead of raising, so the bad value flows into the
quarantine path as `amount_not_numeric` rather than taking the stream down.

**Takeaways**

1. Schema inference is driven by the data, including the bad data. A single
   malformed value can retype a column and change the behaviour of every
   expression downstream.
2. In an ingestion path, prefer the non-throwing variant of any cast or parse.
   `try_cast` and `from_json` returning NULL are what make a quarantine pattern
   possible at all.

---

## 9. datediff counts calendar boundaries, not elapsed time

**Severity:** silent misclassification — 10 payments written off incorrectly

Gold reconciled exactly on MATCHED, AMOUNT_MISMATCH and REJECTED, but was off by
ten on the two time-dependent states:

```
PENDING   expected 487   actual 477
EXPIRED   expected 192   actual 202
```

**Cause:** the generator measures the wait as a timedelta — complete 24-hour
periods. Gold used `datediff(as_of, payment_ts)`, which counts **calendar day
boundaries crossed**.

A payment at 23:00 on Oct 28, evaluated at 18:37 on Nov 4, is six elapsed days
but seven calendar days. Ten payments sat exactly on that boundary and were
pushed past their retry window.

**Fix**

```python
F.floor((F.unix_timestamp(F.to_timestamp(F.lit(AS_OF)))
       - F.unix_timestamp("payment_ts")) / 86400)
```

**Takeaways**

1. `datediff` answers "how many midnights between these two timestamps", not
   "how much time has elapsed". For a duration-based rule the difference is
   material and it lands exactly on the boundary cases.
2. Nothing raised. The job succeeded and wrote off ten real payments. Only the
   reconciliation against known totals exposed it — and it exposed it precisely
   because MATCHED and REJECTED are time-invariant while PENDING and EXPIRED
   are not, which immediately pointed at the time arithmetic.

---

## 10. NOT(NULL) is NULL, so the quarantine table lost nine records

**Severity:** silent data loss, in the table built to prevent silent data loss

The declarative pipeline produced 54 quarantined records against an expected 63.
Nine missing — exactly the number of records where the generator removes the
`region` field entirely.

**Cause:** the quarantine dataset selected rejects with

```sql
NOT (... AND region IN ('IN','US','EU','SG','AE') ...)
```

When `region` is NULL, `region IN (...)` evaluates to **NULL**, not FALSE. The
whole conjunction becomes NULL, `NOT (NULL)` is NULL, and a filter keeps only
rows where the predicate is TRUE.

So those nine records were dropped by the validity expectation on the clean
table **and** excluded from the quarantine table. They passed through the
pipeline and ceased to exist.

**Fix**

```python
.filter(f"({VALID}) IS NOT TRUE")
```

`IS NOT TRUE` collapses both FALSE and NULL to true, which is the correct
semantics for "this row did not pass validation".

**Takeaways**

1. SQL is three-valued. `NOT (unknown)` is still unknown, and a WHERE clause
   discards unknown. Any validity check written as `NOT (...)` over nullable
   columns has this hole.
2. The inverted-condition quarantine pattern only works if the inversion is
   null-safe. Otherwise the records most likely to be malformed — the ones with
   missing fields — are exactly the ones it fails to capture.
---

## 11. A background OPTIMIZE made a RESTORE fail

**Severity:** transient — resolved by retrying

Demonstrating time travel, a `RESTORE TABLE ... TO VERSION AS OF 1` failed:

```
DELTA_CONCURRENT_WRITE: a concurrent transaction has written new data
since the current transaction read the table.
Conflicting commit: {"operation":"OPTIMIZE", ... "auto":true}
```

**Cause:** auto-compaction ran in the background and committed a new version
while the RESTORE was in flight. The RESTORE had read version 2 and attempted
to commit version 3; OPTIMIZE got there first.

**Why this is correct behaviour.** Delta does not lock the table. Writers
proceed optimistically against a snapshot and attempt to commit a new version;
conflicts are detected at commit time and the losing writer is rejected rather
than silently overwriting. A table-level lock would make this impossible — and
would serialise every writer, which is the opposite of what a lakehouse needs.

**Resolution:** rerun the statement. It re-reads the current snapshot and
re-applies against it.

**Takeaways**

1. Optimistic concurrency means retry logic is part of normal operation, not
   error handling. Any client issuing writes to a shared Delta table needs it.
2. Auto-compaction and auto-optimize are writers too. They commit versions and
   can conflict with user operations, which is worth knowing before enabling
   them on a table with concurrent access.
3. Conflicts are evaluated per-file, so appends to disjoint partitions
   generally do not collide. Operations that rewrite broad table state —
   RESTORE, OPTIMIZE, large MERGEs — are the ones that race.

---
