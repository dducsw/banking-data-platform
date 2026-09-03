"""Notification event generator — replaces digital_engagement_monthly (push + email columns).

1 row per notification sent. Includes push notifications and email campaigns.
notification_type adds semantic meaning for EDA/BI use cases.
"""

from datetime import date, datetime
from typing import Dict, List, Optional, Set
import numpy as np

from config.personas import Persona


_PUSH_TYPES = [
    "Balance Alert",
    "Payment Due Reminder",
    "Transaction Alert",
    "Low Balance Warning",
    "Promotional Offer",
    "Security Alert",
    "New Feature",
    "Statement Ready",
]
_PUSH_TYPE_WEIGHTS = [0.25, 0.20, 0.20, 0.10, 0.10, 0.07, 0.05, 0.03]

_EMAIL_TYPES = [
    "Monthly Statement",
    "Promotional Campaign",
    "Product Offer",
    "Security Notice",
    "Account Update",
]
_EMAIL_TYPE_WEIGHTS = [0.35, 0.30, 0.20, 0.10, 0.05]

# (push_lambda, email_lambda, opt_in_prob, push_open_rate_base)
_NOTIF_CFG = {
    Persona.DIGITAL_NATIVE:           (10.0, 4.0, 0.92, 0.65),
    Persona.AFFLUENT_MULTI_PRODUCT:   (7.0,  3.0, 0.80, 0.55),
    Persona.SALARY_CORE:              (6.0,  3.0, 0.75, 0.50),
    Persona.CREDIT_STRESSED:          (6.0,  3.0, 0.75, 0.45),
    Persona.COMPLAINT_PRONE_CHURNER:  (6.0,  3.0, 0.70, 0.40),
    Persona.DORMANT_WEALTHY:          (3.0,  2.0, 0.35, 0.30),
}


def generate_monthly_notification_events(
    active_customers: List[dict],
    snapshot_month: date,
    start_notif_id: int,
    rng: np.random.Generator,
    active_events_dict: Optional[Dict[int, Set[str]]] = None,
) -> list[dict]:
    """Generate per-notification events for push and email channels.

    Returns list of notification_event dicts.
    """
    if active_events_dict is None:
        active_events_dict = {}

    if snapshot_month.month == 12:
        next_month = date(snapshot_month.year + 1, 1, 1)
    else:
        next_month = date(snapshot_month.year, snapshot_month.month + 1, 1)
    days_in_month = (next_month - snapshot_month).days

    events = []
    notif_id = start_notif_id

    for c in active_customers:
        cid = c["customer_id"]
        persona = Persona(c["persona"])
        cust_events = active_events_dict.get(cid, set())
        push_lam, email_lam, opt_in_prob, push_open_base = _NOTIF_CFG[persona]

        opted_in = rng.random() < opt_in_prob

        # Campaign exposure boosts email/push volume
        if "campaign_exposure" in cust_events:
            push_lam *= 1.4
            email_lam *= 1.5

        # ── Push notifications ────────────────────────────────────────────
        if opted_in:
            n_push = int(rng.poisson(push_lam))
            for _ in range(n_push):
                day = int(rng.integers(1, days_in_month + 1))
                hour = int(rng.integers(7, 22))
                sent_ts = datetime(snapshot_month.year, snapshot_month.month, day, hour,
                                   int(rng.integers(0, 60)), 0)

                notif_type = rng.choice(_PUSH_TYPES, p=_PUSH_TYPE_WEIGHTS)
                opened = rng.random() < push_open_base

                opened_ts = None
                if opened:
                    # Opened within 0–120 minutes after sent
                    delay_sec = int(rng.integers(0, 7200))
                    opened_ts = datetime.fromtimestamp(sent_ts.timestamp() + delay_sec)

                events.append({
                    "notification_id": notif_id,
                    "customer_id": cid,
                    "sent_at": sent_ts,
                    "sent_date": sent_ts.date(),
                    "sent_month": snapshot_month,
                    "channel": "Push",
                    "notification_type": notif_type,
                    "opened": opened,
                    "opened_at": opened_ts,
                })
                notif_id += 1

        # ── Email notifications ───────────────────────────────────────────
        n_email = int(rng.poisson(email_lam))
        email_open_rate = max(0.05, min(0.40, push_open_base * 0.50))

        for _ in range(n_email):
            day = int(rng.integers(1, days_in_month + 1))
            hour = int(rng.integers(8, 20))
            sent_ts = datetime(snapshot_month.year, snapshot_month.month, day, hour,
                               int(rng.integers(0, 60)), 0)

            notif_type = rng.choice(_EMAIL_TYPES, p=_EMAIL_TYPE_WEIGHTS)
            opened = rng.random() < email_open_rate

            opened_ts = None
            if opened:
                delay_sec = int(rng.integers(0, 86400))  # up to 24h later
                opened_ts = datetime.fromtimestamp(sent_ts.timestamp() + delay_sec)

            events.append({
                "notification_id": notif_id,
                "customer_id": cid,
                "sent_at": sent_ts,
                "sent_date": sent_ts.date(),
                "sent_month": snapshot_month,
                "channel": "Email",
                "notification_type": notif_type,
                "opened": opened,
                "opened_at": opened_ts,
            })
            notif_id += 1

    return events
