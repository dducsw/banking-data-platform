"""Loan payment event generator — replaces loan_monthly_snapshot.

1 row per loan per month. Captures EMI payment details, outstanding balance,
DPD, and loan status. The EMI calculation logic is extracted from simulate.py.
"""

from datetime import date, datetime, time
from typing import Dict, List
import numpy as np


def generate_monthly_loan_payment_events(
    loans_by_customer: Dict[int, List[dict]],
    running_loans: Dict[int, dict],
    active_cids: List[int],
    snapshot_month: date,
    start_payment_id: int,
    rng: np.random.Generator,
) -> list[dict]:
    """Generate one loan_payment_event per active loan per month.

    The EMI amounts and DPD state are read from running_loans (mutated in-place
    by simulate.py). This function only records the state; delinquency logic
    remains in the main simulation loop.

    Returns list of loan_payment_event dicts.
    """
    events = []
    payment_id = start_payment_id
    payment_day = date(snapshot_month.year, snapshot_month.month, 10)
    payment_ts = datetime.combine(payment_day, time(10, 0, 0))

    for cid in active_cids:
        for ln in loans_by_customer.get(cid, []):
            loan_id = ln["loan_id"]
            ln_state = running_loans[loan_id]

            if ln_state["status"] == "Closed":
                # Emit a single closed-status record so we can track payoff date
                events.append({
                    "payment_id": payment_id,
                    "loan_id": loan_id,
                    "customer_id": cid,
                    "payment_date": payment_day,
                    "payment_month": snapshot_month,
                    "payment_timestamp": payment_ts,
                    "emi_due_amount": 0.0,
                    "emi_paid_amount": 0.0,
                    "principal_paid": 0.0,
                    "interest_paid": 0.0,
                    "outstanding_balance": 0.0,
                    "dpd_days": 0,
                    "loan_status": "Closed",
                    "is_delinquent": False,
                    "restructuring_flag": False,
                })
                payment_id += 1
                continue

            outstanding = ln_state["outstanding_balance"]
            interest_rate = ln["interest_rate"]
            emi = ln["emi_amount"]

            interest = outstanding * (interest_rate / 12.0 / 100.0)
            principal = emi - interest
            principal = min(principal, outstanding)
            actual_emi = principal + interest

            is_delinquent = ln_state["dpd_days"] > 0

            if is_delinquent:
                emi_paid = 0.0
                principal_paid = 0.0
                interest_paid = 0.0
            else:
                emi_paid = round(actual_emi, 2)
                principal_paid = round(principal, 2)
                interest_paid = round(interest, 2)

            events.append({
                "payment_id": payment_id,
                "loan_id": loan_id,
                "customer_id": cid,
                "payment_date": payment_day,
                "payment_month": snapshot_month,
                "payment_timestamp": payment_ts,
                "emi_due_amount": round(actual_emi, 2),
                "emi_paid_amount": emi_paid,
                "principal_paid": principal_paid,
                "interest_paid": interest_paid,
                "outstanding_balance": round(outstanding, 2),
                "dpd_days": int(ln_state["dpd_days"]),
                "loan_status": ln_state["status"],
                "is_delinquent": is_delinquent,
                "restructuring_flag": False,
            })
            payment_id += 1

    return events
