"""Login session event generator — replaces digital_engagement_monthly + customer_monthly_activity (login portion).

1 row per login session. Sessions are generated per customer per month
based on persona login lambdas. Each session has a timestamp, channel,
device_type, duration, and page_views.
"""

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set
import numpy as np
from faker import Faker
fake = Faker()

from config.personas import Persona, PERSONA_CONFIGS


# Per-persona login session configuration
_LOGIN_CFG = {
    Persona.DIGITAL_NATIVE: {
        "lambda": 30.0,       # sessions/month
        "mobile_share": 0.90,
        "duration_mu": 480,   # seconds mean
        "duration_sigma": 120,
        "page_views_lambda": 12.0,
        "devices": (["iOS", "Android", "Desktop"], [0.45, 0.45, 0.10]),
    },
    Persona.AFFLUENT_MULTI_PRODUCT: {
        "lambda": 20.0,
        "mobile_share": 0.70,
        "duration_mu": 360,
        "duration_sigma": 90,
        "page_views_lambda": 9.0,
        "devices": (["iOS", "Android", "Desktop"], [0.35, 0.30, 0.35]),
    },
    Persona.SALARY_CORE: {
        "lambda": 15.0,
        "mobile_share": 0.70,
        "duration_mu": 300,
        "duration_sigma": 80,
        "page_views_lambda": 7.0,
        "devices": (["iOS", "Android", "Desktop"], [0.30, 0.40, 0.30]),
    },
    Persona.CREDIT_STRESSED: {
        "lambda": 12.0,
        "mobile_share": 0.75,
        "duration_mu": 240,
        "duration_sigma": 60,
        "page_views_lambda": 5.0,
        "devices": (["iOS", "Android", "Desktop"], [0.25, 0.50, 0.25]),
    },
    Persona.COMPLAINT_PRONE_CHURNER: {
        "lambda": 12.0,
        "mobile_share": 0.65,
        "duration_mu": 300,
        "duration_sigma": 75,
        "page_views_lambda": 6.0,
        "devices": (["iOS", "Android", "Desktop"], [0.30, 0.40, 0.30]),
    },
    Persona.DORMANT_WEALTHY: {
        "lambda": 2.0,
        "mobile_share": 0.30,
        "duration_mu": 180,
        "duration_sigma": 60,
        "page_views_lambda": 3.0,
        "devices": (["iOS", "Android", "Desktop"], [0.20, 0.20, 0.60]),
    },
}

_LOGOUT_TYPES = ["Manual", "Timeout"]
_LOGOUT_PROBS = [0.70, 0.30]


def generate_monthly_login_events(
    active_customers: List[dict],
    snapshot_month: date,
    start_session_id: int,
    rng: np.random.Generator,
    active_events_dict: Optional[Dict[int, Set[str]]] = None,
) -> tuple[list[dict], dict]:
    """Generate per-session login events for all active customers in a month.

    Returns:
        (events, login_counts_by_cid)
        events: list of login_event dicts
        login_counts_by_cid: {customer_id: session_count} — used by churn scorer
    """
    if active_events_dict is None:
        active_events_dict = {}

    if snapshot_month.month == 12:
        next_month = date(snapshot_month.year + 1, 1, 1)
    else:
        next_month = date(snapshot_month.year, snapshot_month.month + 1, 1)
    days_in_month = (next_month - snapshot_month).days

    events = []
    session_id = start_session_id
    login_counts_by_cid: dict[int, int] = {}

    for c in active_customers:
        cid = c["customer_id"]
        persona = Persona(c["persona"])
        cfg = _LOGIN_CFG[persona]
        cust_events = active_events_dict.get(cid, set())

        # Event modifiers
        lam = cfg["lambda"]
        if "bank_service_failure" in cust_events:
            lam *= 0.5   # drop after service failure
        if "campaign_exposure" in cust_events:
            lam *= 1.3

        n_sessions = int(rng.poisson(max(0.5, lam)))
        login_counts_by_cid[cid] = n_sessions

        if n_sessions == 0:
            continue

        mobile_share = cfg["mobile_share"]
        devices, device_probs = cfg["devices"]

        for _ in range(n_sessions):
            # Random timestamp within the month
            day = int(rng.integers(1, days_in_month + 1))
            hour = int(rng.integers(6, 24))
            minute = int(rng.integers(0, 60))
            second = int(rng.integers(0, 60))
            login_ts = datetime(snapshot_month.year, snapshot_month.month, day, hour, minute, second)

            # Channel
            channel = "Mobile App" if rng.random() < mobile_share else "Web Browser"

            # Device
            device = rng.choice(devices, p=device_probs)

            # Duration (seconds)
            duration = int(max(30, rng.normal(cfg["duration_mu"], cfg["duration_sigma"])))

            # Page views
            page_views = int(max(1, rng.poisson(cfg["page_views_lambda"])))

            # Logout type
            logout_type = rng.choice(_LOGOUT_TYPES, p=_LOGOUT_PROBS)

            # Security & Auth Features
            is_successful = rng.random() > 0.05
            failed_attempt_count = rng.integers(1, 4) if not is_successful else 0
            otp_used = rng.random() < 0.2
            biometric_used = (rng.random() < 0.6) if channel == "Mobile App" else False
            ip_address = fake.ipv4()
            is_new_device = rng.random() < 0.05

            events.append({
                "session_id": session_id,
                "customer_id": cid,
                "login_timestamp": login_ts,
                "login_date": login_ts.date(),
                "login_month": snapshot_month,
                "channel": channel,
                "device_type": device,
                "session_duration_seconds": duration,
                "page_views": page_views,
                "logout_type": logout_type,
                "is_successful": is_successful,
                "failed_attempt_count": failed_attempt_count,
                "otp_used": otp_used,
                "biometric_used": biometric_used,
                "ip_address": ip_address,
                "is_new_device": is_new_device,
            })
            session_id += 1

    return events, login_counts_by_cid
