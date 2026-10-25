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
