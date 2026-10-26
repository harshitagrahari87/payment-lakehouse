"""
Tests for the transformation rules.

These cover the decisions that would be wrong without raising anything —
validation boundaries, which row survives deduplication, and the retry-window
edges that decide whether a payment is still pending or written off.

    pytest -q
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from gold_job import build_payment_state  # noqa: E402
from silver_job import dedup, dedup_quarantine, validate  # noqa: E402

CFG = {
    "regions": [
        {"code": "US", "currency": "USD", "retry_window_days": 3, "weight": 0.5},
        {"code": "AE", "currency": "AED", "retry_window_days": 10, "weight": 0.5},
    ]
}
VALID_REGIONS = {"US", "AE"}
VALID_CURRENCIES = {"USD", "AED"}


def payment(**overrides) -> dict:
    base = {
        "event_id": "e1",
        "event_type": "PAYMENT_RECEIVED",
        "event_ts": "2026-10-20T10:00:00Z",
        "payment_id": "PAY-1",
        "region": "US",
        "currency": "USD",
        "amount": 100.0,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# validation — each corruption the producer injects must be caught
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("overrides,expected", [
    ({}, None),
    ({"amount": None}, "null_field:amount"),
    ({"amount": "abc"}, "amount_not_numeric"),
    ({"amount": -50.0}, "amount_not_positive"),
    ({"amount": 0}, "amount_not_positive"),
    ({"currency": "XXX"}, "unknown_currency:XXX"),
    ({"region": "ZZ"}, "unknown_region:ZZ"),
])
def test_validation_catches_each_defect(overrides, expected):
    assert validate(payment(**overrides), VALID_REGIONS, VALID_CURRENCIES) == expected


def test_missing_field_is_caught():
    record = payment()
    del record["region"]
    assert validate(record, VALID_REGIONS, VALID_CURRENCIES) == "missing_field:region"


def test_amount_bool_is_not_numeric():
    """True == 1 in Python. Without an explicit bool check this passes as 1.0."""
    assert validate(payment(amount=True), VALID_REGIONS, VALID_CURRENCIES) == "amount_not_numeric"


def test_invoice_requires_both_ids():
    invoice = {
        "event_id": "e2", "event_type": "INVOICE_CLEARED",
        "event_ts": "2026-10-20T10:00:00Z", "region": "US",
        "currency": "USD", "amount": 100.0, "payment_id": "PAY-1",
    }
    assert validate(invoice, VALID_REGIONS, VALID_CURRENCIES) == "missing_field:invoice_id"


# ---------------------------------------------------------------------------
# dedup — the rule the whole idempotency story rests on
# ---------------------------------------------------------------------------

def test_dedup_keeps_first_arrival():
    df = pd.DataFrame([
        {"event_id": "a", "ingest_ts": "2026-10-26T12:00:00Z", "amount": 100},
        {"event_id": "a", "ingest_ts": "2026-10-26T13:00:00Z", "amount": 999},
        {"event_id": "b", "ingest_ts": "2026-10-26T12:00:00Z", "amount": 200},
    ])
    out, removed = dedup(df)
    assert removed == 1
    assert len(out) == 2
    assert out.loc[out["event_id"] == "a", "amount"].iloc[0] == 100


def test_dedup_does_not_collapse_corrections():
    """
    Same payment_id, different event_id = a CORRECTION, not a duplicate.
    Deduping on payment_id would silently destroy every amendment.
    """
    df = pd.DataFrame([
        {"event_id": "e1", "payment_id": "PAY-1", "ingest_ts": "t1", "amount": 500},
        {"event_id": "e2", "payment_id": "PAY-1", "ingest_ts": "t2", "amount": 450},
    ])
    out, removed = dedup(df)
    assert removed == 0
    assert len(out) == 2


def test_quarantine_is_deduped_too():
    """
    A re-ingestion must not inflate the rejected count, which would break the
    daily reconciliation.
    """
    df = pd.DataFrame([
        {"event_id": "bad1", "ingest_ts": "t1", "raw": "{}", "reject_reason": "x"},
        {"event_id": "bad1", "ingest_ts": "t2", "raw": "{}", "reject_reason": "x"},
    ])
    out, removed = dedup_quarantine(df)
    assert removed == 1
    assert len(out) == 1


def test_quarantine_dedup_falls_back_to_raw():
    """Unparseable records have no event_id — dedup on the bytes instead."""
    df = pd.DataFrame([
        {"event_id": None, "ingest_ts": "t1", "raw": "not json", "reject_reason": "x"},
        {"event_id": None, "ingest_ts": "t2", "raw": "not json", "reject_reason": "x"},
        {"event_id": None, "ingest_ts": "t3", "raw": "different", "reject_reason": "x"},
    ])
    out, removed = dedup_quarantine(df)
    assert removed == 1
    assert len(out) == 2


# ---------------------------------------------------------------------------
# the state machine
# ---------------------------------------------------------------------------

def make_frames(payments: list[dict], invoices: list[dict]):
    pay_cols = ["payment_id", "customer_id", "region", "currency",
                "amount", "payment_method", "event_ts"]
    p = pd.DataFrame(payments)
    for col in pay_cols:
        if col not in p.columns:
            p[col] = None
    i = pd.DataFrame(invoices, columns=["payment_id", "invoice_id", "amount", "event_ts"])
    return p[pay_cols], i


AS_OF = pd.Timestamp("2026-10-26T00:00:00Z")


def test_matched_when_amounts_agree():
    p, i = make_frames(
        [{"payment_id": "P1", "region": "US", "amount": 100.0, "event_ts": "2026-10-25T00:00:00Z"}],
        [{"payment_id": "P1", "invoice_id": "I1", "amount": 100.0, "event_ts": "2026-10-25T06:00:00Z"}],
    )
    assert build_payment_state(p, i, CFG, AS_OF)["state"].iloc[0] == "MATCHED"


def test_mismatch_when_amounts_differ():
    p, i = make_frames(
        [{"payment_id": "P1", "region": "US", "amount": 100.0, "event_ts": "2026-10-25T00:00:00Z"}],
        [{"payment_id": "P1", "invoice_id": "I1", "amount": 90.0, "event_ts": "2026-10-25T06:00:00Z"}],
    )
    out = build_payment_state(p, i, CFG, AS_OF)
    assert out["state"].iloc[0] == "AMOUNT_MISMATCH"
    assert out["amount_variance"].iloc[0] == pytest.approx(-10.0)


def test_pending_inside_the_window():
    p, i = make_frames(
        [{"payment_id": "P1", "region": "US", "amount": 100.0, "event_ts": "2026-10-24T00:00:00Z"}],
        [],
    )
    out = build_payment_state(p, i, CFG, AS_OF)
    assert out["days_waiting"].iloc[0] == 2        # window is 3
    assert out["state"].iloc[0] == "PENDING"


def test_expired_outside_the_window():
    p, i = make_frames(
        [{"payment_id": "P1", "region": "US", "amount": 100.0, "event_ts": "2026-10-20T00:00:00Z"}],
        [],
    )
    out = build_payment_state(p, i, CFG, AS_OF)
    assert out["days_waiting"].iloc[0] == 6        # window is 3
    assert out["state"].iloc[0] == "EXPIRED"


def test_window_boundary_is_strictly_greater_than():
    """days_waiting == window is still PENDING. Off-by-one here silently
    writes off a day's worth of payments."""
    p, i = make_frames(
        [{"payment_id": "P1", "region": "US", "amount": 100.0, "event_ts": "2026-10-23T00:00:00Z"}],
        [],
    )
    out = build_payment_state(p, i, CFG, AS_OF)
    assert out["days_waiting"].iloc[0] == 3
    assert out["state"].iloc[0] == "PENDING"


def test_retry_window_is_per_region():
    """Identical payments, different regions, different outcomes."""
    p, i = make_frames(
        [
            {"payment_id": "P1", "region": "US", "amount": 100.0, "event_ts": "2026-10-20T00:00:00Z"},
            {"payment_id": "P2", "region": "AE", "amount": 100.0, "event_ts": "2026-10-20T00:00:00Z"},
        ],
        [],
    )
    out = build_payment_state(p, i, CFG, AS_OF).set_index("payment_id")
    assert out.loc["P1", "state"] == "EXPIRED"     # 3-day window
    assert out.loc["P2", "state"] == "PENDING"     # 10-day window


def test_duplicate_invoices_do_not_fan_out_the_payment():
    """Two invoices for one payment must not produce two fact rows."""
    p, i = make_frames(
        [{"payment_id": "P1", "region": "US", "amount": 100.0, "event_ts": "2026-10-25T00:00:00Z"}],
        [
            {"payment_id": "P1", "invoice_id": "I1", "amount": 100.0, "event_ts": "2026-10-25T06:00:00Z"},
            {"payment_id": "P1", "invoice_id": "I2", "amount": 100.0, "event_ts": "2026-10-25T07:00:00Z"},
        ],
    )
    out = build_payment_state(p, i, CFG, AS_OF)
    assert len(out) == 1
    assert out["invoice_id"].iloc[0] == "I1"       # earliest wins


def test_as_of_changes_the_answer():
    """
    The same payment is PENDING or EXPIRED depending on the date asked.
    This is why as_of is a parameter and never now().
    """
    p, i = make_frames(
        [{"payment_id": "P1", "region": "US", "amount": 100.0, "event_ts": "2026-10-20T00:00:00Z"}],
        [],
    )
    early = build_payment_state(p, i, CFG, pd.Timestamp("2026-10-22T00:00:00Z"))
    late = build_payment_state(p, i, CFG, pd.Timestamp("2026-10-26T00:00:00Z"))
    assert early["state"].iloc[0] == "PENDING"
    assert late["state"].iloc[0] == "EXPIRED"


def test_control_total_holds():
    p, i = make_frames(
        [
            {"payment_id": "P1", "region": "US", "amount": 100.0, "event_ts": "2026-10-25T00:00:00Z"},
            {"payment_id": "P2", "region": "US", "amount": 100.0, "event_ts": "2026-10-24T00:00:00Z"},
            {"payment_id": "P3", "region": "US", "amount": 100.0, "event_ts": "2026-10-20T00:00:00Z"},
            {"payment_id": "P4", "region": "AE", "amount": 100.0, "event_ts": "2026-10-25T00:00:00Z"},
        ],
        [{"payment_id": "P1", "invoice_id": "I1", "amount": 100.0, "event_ts": "2026-10-25T06:00:00Z"}],
    )
    out = build_payment_state(p, i, CFG, AS_OF)
    states = ["MATCHED", "AMOUNT_MISMATCH", "PENDING", "EXPIRED"]
    assert out["state"].value_counts().reindex(states).fillna(0).sum() == len(out)
    assert set(out["state"]) <= set(states)


# ---------------------------------------------------------------------------
# the producer's answer key must itself balance
# ---------------------------------------------------------------------------

def test_producer_answer_key_balances():
    path = Path(__file__).resolve().parents[1] / "data" / "_expected_totals.json"
    if not path.exists():
        pytest.skip("run producer.py first")
    truth = json.loads(path.read_text(encoding="utf-8"))
    accounted = sum(
        truth[k] for k in ("MATCHED", "AMOUNT_MISMATCH", "PENDING", "EXPIRED", "REJECTED")
    )
    assert accounted == truth["payments_generated"]
