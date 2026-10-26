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

**Takeaway:** a fixture and the expectations derived from it are one artifact
and must be regenerated as a unit. Only the time-invariant states matched —
which is precisely why PENDING and EXPIRED were the two that drifted.
