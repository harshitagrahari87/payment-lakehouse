# Failure Log

> Every error, how it was diagnosed, what fixed it.
> This is the most valuable file in the repo — it is the answer to
> *"tell me about a time you debugged something in production."*

---

## #1 — Leaked an IAM secret access key into a chat log

**Date:** Oct 25, 2026
**Severity:** Critical (credential exposure, `AdministratorAccess`)

**What happened**
While sharing terminal output, I pasted the full `aws configure` session — which
echoed the **Secret Access Key** in plain text. An `AKIA...` key with admin rights
is the single most-scraped credential type on the internet; compromised keys are
typically exploited for crypto mining within hours of exposure.

**How it was caught**
Immediately, on review of the pasted output. No automated control caught it —
that is itself a finding.

**Fix (executed within ~2 minutes)**
```bash
aws iam create-access-key  --user-name de-projects          # 1. mint replacement
aws configure                                                # 2. repoint the CLI
aws sts get-caller-identity                                  # 3. prove new key works
aws iam update-access-key  --user-name de-projects \
    --access-key-id <OLD> --status Inactive                  # 4. deactivate old
aws sts get-caller-identity                                  # 5. prove still working
aws iam delete-access-key  --user-name de-projects \
    --access-key-id <OLD>                                    # 6. delete old
aws iam list-access-keys   --user-name de-projects           # 7. verify exactly 1 key
```

**Verification of blast radius**
```bash
aws ce get-cost-and-usage --time-period Start=2026-10-01,End=2026-10-26 \
    --granularity MONTHLY --metrics UnblendedCost
# -> $0.00  — no unauthorised usage
```

**Why deactivate before delete**
Deactivating is reversible and instantly revokes the credential. If anything had
still depended on the old key, the failure would have surfaced while rollback was
still possible. Deleting first makes the rollback path disappear.

**Root cause**
No separation between "output I read" and "output I share". The secret was only
ever on screen because `aws configure` echoes input.

**Preventive controls added**
1. `.gitignore` blocks `*credentials*.csv`, `.env`, `*.pem`, `.aws/` — written
   before the first line of application code existed.
2. Never retype or echo a secret; `aws configure` is run once and the credential
   lives only in `~/.aws/credentials`.
3. Access Key **ID** is semi-public and safe to show. The **Secret** is shown to
   no one, ever.

**Hardening still owed**
- Replace `AdministratorAccess` on `de-projects` with a least-privilege policy
  scoped to S3 / Glue / Kinesis / Lambda / Athena once resource ARNs are known.
- Enable CloudTrail and alarm on `ConsoleLogin` + IAM mutations.
- Treat the $10 budget alarm as the backstop, not the control.

---

## #2 — `AdministratorAccess` returned AccessDenied

**Date:** Oct 25, 2026
**Severity:** Blocker — no AWS resources could be created

**Symptom**
```
aws s3api create-bucket --bucket payment-lakehouse-... --region ap-south-1

AccessDenied: User: arn:aws:iam::569854554172:user/de-projects is not
authorized to perform: s3:CreateBucket ... with an explicit deny in a
service control policy: .../p-sngva019
```
The IAM user had `AdministratorAccess` attached.

**Reading the error**
The message named a **service control policy**, not an IAM policy. That
settles the layer immediately:

```
SCP        a permissions CEILING on a member account
IAM        a permissions GRANT within that ceiling
Evaluation explicit Deny  >  Allow  >  implicit deny
```
An explicit `Deny` in an SCP cannot be overridden by any IAM policy, managed
or inline. So no amount of IAM work would ever fix this — the wrong layer was
being looked at.

**Diagnosis — narrowing by controlled comparison**

| # | Test | Result | Eliminated |
|---|---|---|---|
| 1 | KYC status on the account | Verification **Failed** | fixed it — address on file was Hyderabad, PAN record is Kanpur. Re-verified with PAN. Restriction persisted, so KYC was not the cause |
| 2 | Same call from **CloudShell** (`AccountFullAccessRole`) | `s3 ls` **succeeded** | account-wide deny — the restriction is **principal-scoped** |
| 3 | `sts:AssumeRole` from the IAM user | denied by the same SCP | the user cannot escape into a role |
| 4 | Created a custom role, tried to assume it | denied | not specific to the AWS-managed role |
| 5 | `s3:CreateBucket` from CloudShell | **denied** | principal-scoped theory — this action is denied for *everyone* |
| 6 | Upgraded Free Plan → Paid Plan (+ $20 hard spend limit) | restriction persisted | plan tier alone |

**Conclusion**
`s3:ListAllMyBuckets` is permitted for the managed role but denied for IAM
users, while `s3:CreateBucket` is denied for **every** principal. The deny is
therefore both **principal-scoped and action-scoped** — an AWS-applied
restriction on this account model, not a misconfiguration.

**Resolution**
Support case `179293343000299` (Account and Billing — free on every plan),
quoting the policy id and the exact denied actions so it routes correctly.

**The option deliberately NOT taken**
Activating AWS Organizations advanced features would have allowed the SCPs to
be removed — and the console stated plainly that it **deletes spend limits**
and **cannot be reversed**. Trading an irreversible loss of a hard cost control
for a problem with a free support path is a bad trade. Deferred pending the
support response, and if it ever becomes necessary, Budgets alerts get
configured *first*. You do not remove a control and leave nothing in its place.

**Takeaways**
1. Read *which layer* an AccessDenied names. SCP, IAM, resource policy and
   permissions boundary fail in different places and have different fixes.
2. Isolate with a controlled comparison: same action, same account, different
   principal. One CloudShell call eliminated three hypotheses at once.
3. Never work around a guardrail. Fix it at its source or escalate.

---

## #3 — A re-ingestion doubled the quarantine count, and the job reported SUCCESS

**Date:** Oct 25, 2026
**Severity:** Silent data-quality defect — no error, no alert, wrong numbers

**What happened**
`bronze_writer.py` was run twice. The object key is a deterministic hash of
the batch's `event_id`s, so the second run should have overwritten the first.
It did not: the key embeds the **ingest hour**, and the two runs fell in
different hours.

```
data/bronze/ingest_dt=2026-10-25/hour=12/  95 files
data/bronze/ingest_dt=2026-10-25/hour=13/  95 files   <- every event, twice
```

Silver then read 18,892 records instead of 9,446 and reported:
```
quarantined : 126      expected : 63
unique      : 9,321    expected : 9,258
```

**Why it was not caught by the job**
It ran to completion. Exit code 0. No exception, no warning. Valid records
were deduplicated on `event_id`; **quarantined records were not.** Dedup ran
only on the good branch after the good/bad split, so both copies of every
malformed record were written.

Only the reconciliation against the producer's answer key exposed it, and the
discrepancy was exactly 63 — the duplicated rejects.

**Fix**
`dedup_quarantine()` — same first-arrival-per-`event_id` rule, falling back to
the raw bytes for records too malformed to carry an `event_id`.

Result with the doubled input unchanged at 18,892 records in:
```
silver records : 9,195   quarantine : 63
unique 9,258 = 9,258  ✓      quarantined 63 = 63  ✓
```

**Takeaways**
1. **Quarantine is a table.** It needs the same idempotency guarantees as the
   happy path. A duplicated reject corrupts the rejected total, and therefore
   the daily reconciliation.
2. **A deterministic key is only as deterministic as everything in it.** The
   hour component silently broke the guarantee across partitions — which is
   precisely why event-level dedup in silver is a second, independent defence
   rather than redundancy.
3. **Success is not correctness.** The job succeeded while producing wrong
   numbers. Only a control total against a known figure catches that class of
   bug, and it is the reason the generator emits an answer key at all.

---
