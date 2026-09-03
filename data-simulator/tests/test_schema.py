"""DDL schema contract tests to verify pipeline/schema.sql matches spec Section 12."""

import os
import re
from typing import Dict, Tuple, Set


EXPECTED_SCHEMA: Dict[str, Dict[str, Tuple[str, bool]]] = {
    "branches": {
        "branch_code": ("VARCHAR(20)", False),
        "branch_name": ("VARCHAR(100)", False),
        "city": ("VARCHAR(100)", False),
        "state": ("VARCHAR(100)", False),
        "region": ("VARCHAR(50)", False),
        "branch_type": ("VARCHAR(30)", False),
        "open_date": ("DATE", False),
        "closure_date": ("DATE", True),
        "customer_weight": ("INT", False),
    },
    "customers": {
        "customer_id": ("BIGINT", False),
        "cif_number": ("VARCHAR(20)", False),
        "first_name": ("VARCHAR(100)", False),
        "last_name": ("VARCHAR(100)", False),
        "date_of_birth": ("DATE", False),
        "gender": ("VARCHAR(20)", False),
        "marital_status": ("VARCHAR(20)", False),
        "occupation": ("VARCHAR(100)", False),
        "employment_type": ("VARCHAR(50)", False),
        "annual_income": ("NUMERIC(18,2)", False),
        "customer_since": ("DATE", False),
        "city": ("VARCHAR(100)", False),
        "state": ("VARCHAR(100)", False),
        "country": ("VARCHAR(100)", False),
        "kyc_status": ("VARCHAR(20)", False),
        "is_active": ("BOOLEAN", False),
    },
    "merchants": {
        "merchant_id": ("INT", False),
        "merchant_name": ("VARCHAR(150)", False),
        "transaction_category": ("VARCHAR(100)", False),
        "mcc_code": ("VARCHAR(4)", False),
        "merchant_type": ("VARCHAR(20)", False),
        "is_online": ("BOOLEAN", False),
    },
    "accounts": {
        "account_id": ("BIGINT", False),
        "customer_id": ("BIGINT", True),
        "branch_code": ("VARCHAR(20)", True),
        "account_type": ("VARCHAR(50)", False),
        "open_date": ("DATE", False),
        "account_status": ("VARCHAR(20)", False),
        "account_currency": ("VARCHAR(3)", False),
        "salary_account_flag": ("BOOLEAN", False),
        "overdraft_limit": ("NUMERIC(18,2)", False),
        "account_close_date": ("DATE", True),
    },
    "cards": {
        "card_id": ("BIGINT", False),
        "customer_id": ("BIGINT", True),
        "card_type": ("VARCHAR(20)", False),
        "network": ("VARCHAR(20)", False),
        "issue_date": ("DATE", False),
        "expiry_date": ("DATE", False),
        "card_status": ("VARCHAR(20)", False),
        "primary_card_flag": ("BOOLEAN", False),
        "credit_limit": ("NUMERIC(18,2)", False),
        "rewards_program": ("VARCHAR(50)", False),
        "reward_tier": ("VARCHAR(20)", False),
    },
    "loans": {
        "loan_id": ("BIGINT", False),
        "customer_id": ("BIGINT", True),
        "branch_code": ("VARCHAR(20)", True),
        "loan_type": ("VARCHAR(50)", False),
        "sanctioned_amount": ("NUMERIC(18,2)", False),
        "disbursement_date": ("DATE", False),
        "interest_rate": ("NUMERIC(6,3)", False),
        "tenure_months": ("INT", False),
        "emi_amount": ("NUMERIC(18,2)", False),
        "loan_purpose": ("VARCHAR(100)", False),
        "origination_channel": ("VARCHAR(50)", False),
        "loan_status": ("VARCHAR(20)", False),
        "maturity_date": ("DATE", False),
    },
    "churn_simulation_state": {
        "customer_id": ("BIGINT", False),
        "persona": ("VARCHAR(50)", False),
        "low_sensitivity_segment": ("BOOLEAN", False),
        "churn_month": ("DATE", True),
        "churned_flag": ("BOOLEAN", False),
        "churn_reason": ("VARCHAR(100)", True),
        "active_months_generated": ("INT", False),
    },
    "transactions": {
        "transaction_id": ("BIGINT", False),
        "account_id": ("BIGINT", True),
        "customer_id": ("BIGINT", True),
        "received_customer_id": ("BIGINT", True),
        "received_account_id": ("BIGINT", True),
        "txn_timestamp": ("TIMESTAMP", False),
        "txn_date": ("DATE", False),
        "txn_month": ("DATE", False),
        "txn_type": ("VARCHAR(50)", False),
        "direction": ("VARCHAR(10)", False),
        "channel": ("VARCHAR(30)", False),
        "amount": ("NUMERIC(18,2)", False),
        "currency": ("VARCHAR(3)", False),
        "transaction_category": ("VARCHAR(100)", False),
        "transaction_description": ("VARCHAR(150)", False),
        "merchant_id": ("INT", True),
        "counterparty_type": ("VARCHAR(50)", False),
        "city": ("VARCHAR(100)", False),
        "state": ("VARCHAR(100)", False),
        "is_salary_credit": ("BOOLEAN", False),
        "is_fee": ("BOOLEAN", False),
        "is_reversal": ("BOOLEAN", False),
        "balance_after_txn": ("NUMERIC(18,2)", False),
        "is_fraud": ("BOOLEAN", False),
        "is_disputed": ("BOOLEAN", False),
        "risk_score": ("NUMERIC(5,4)", False),
        "device_id": ("VARCHAR(100)", True),
        "ip_address": ("VARCHAR(45)", True),
        "geolocation": ("VARCHAR(150)", True),
    },
    "account_balance_snapshots": {
        "snapshot_id": ("BIGINT", False),
        "account_id": ("BIGINT", True),
        "customer_id": ("BIGINT", True),
        "snapshot_date": ("DATE", False),
        "snapshot_month": ("DATE", False),
        "end_of_day_balance": ("NUMERIC(18,2)", False),
        "is_month_end": ("BOOLEAN", False),
    },
    "account_ledger": {
        "entry_id": ("BIGINT", False),
        "transaction_id": ("BIGINT", True),
        "account_id": ("BIGINT", True),
        "customer_id": ("BIGINT", True),
        "entry_date": ("DATE", False),
        "entry_month": ("DATE", False),
        "entry_timestamp": ("TIMESTAMP", False),
        "entry_type": ("VARCHAR(10)", False),
        "debit_amount": ("NUMERIC(18,2)", False),
        "credit_amount": ("NUMERIC(18,2)", False),
        "amount": ("NUMERIC(18,2)", False),
        "currency": ("VARCHAR(3)", False),
        "running_balance": ("NUMERIC(18,2)", False),
        "description": ("VARCHAR(200)", False),
        "txn_type": ("VARCHAR(50)", False),
        "channel": ("VARCHAR(30)", False),
        "reference_number": ("VARCHAR(20)", False),
    },
    "login_events": {
        "session_id": ("BIGINT", False),
        "customer_id": ("BIGINT", True),
        "login_timestamp": ("TIMESTAMP", False),
        "login_date": ("DATE", False),
        "login_month": ("DATE", False),
        "channel": ("VARCHAR(20)", False),
        "device_type": ("VARCHAR(30)", False),
        "session_duration_seconds": ("INT", False),
        "page_views": ("INT", False),
        "logout_type": ("VARCHAR(20)", False),
        "is_successful": ("BOOLEAN", False),
        "failed_attempt_count": ("INT", False),
        "otp_used": ("BOOLEAN", False),
        "biometric_used": ("BOOLEAN", False),
        "ip_address": ("VARCHAR(45)", False),
        "is_new_device": ("BOOLEAN", False),
    },
    "notifications": {
        "notification_id": ("BIGINT", False),
        "customer_id": ("BIGINT", True),
        "sent_at": ("TIMESTAMP", False),
        "sent_date": ("DATE", False),
        "sent_month": ("DATE", False),
        "channel": ("VARCHAR(20)", False),
        "notification_type": ("VARCHAR(50)", False),
        "opened": ("BOOLEAN", False),
        "opened_at": ("TIMESTAMP", True),
    },
    "loan_payments": {
        "payment_id": ("BIGINT", False),
        "loan_id": ("BIGINT", True),
        "customer_id": ("BIGINT", True),
        "payment_date": ("DATE", False),
        "payment_month": ("DATE", False),
        "payment_timestamp": ("TIMESTAMP", False),
        "emi_due_amount": ("NUMERIC(18,2)", False),
        "emi_paid_amount": ("NUMERIC(18,2)", False),
        "principal_paid": ("NUMERIC(18,2)", False),
        "interest_paid": ("NUMERIC(18,2)", False),
        "outstanding_balance": ("NUMERIC(18,2)", False),
        "dpd_days": ("INT", False),
        "loan_status": ("VARCHAR(20)", False),
        "is_delinquent": ("BOOLEAN", False),
        "restructuring_flag": ("BOOLEAN", False),
    },
    "complaints": {
        "complaint_id": ("BIGINT", False),
        "customer_id": ("BIGINT", True),
        "complaint_date": ("DATE", False),
        "complaint_month": ("DATE", False),
        "channel": ("VARCHAR(50)", False),
        "category": ("VARCHAR(100)", False),
        "severity": ("VARCHAR(20)", False),
        "resolution_days": ("INT", True),
        "resolved_flag": ("BOOLEAN", False),
        "escalated_flag": ("BOOLEAN", False),
        "csat_score": ("INT", True),
        "root_cause": ("VARCHAR(100)", False),
        "status": ("VARCHAR(20)", False),
    },
    "feedback": {
        "feedback_id": ("BIGINT", False),
        "customer_id": ("BIGINT", True),
        "feedback_date": ("DATE", False),
        "feedback_month": ("DATE", False),
        "survey_channel": ("VARCHAR(50)", False),
        "survey_topic": ("VARCHAR(100)", False),
        "nps_score": ("INT", True),
        "csat_score": ("INT", True),
    },
    "customer_churn_label": {
        "customer_id": ("BIGINT", False),
        "as_of_month": ("DATE", False),
        "prediction_horizon_months": ("INT", False),
        "churned": ("BOOLEAN", False),
        "churn_date": ("DATE", True),
        "churn_reason": ("VARCHAR(100)", True),
    },
    "churn_feature_snapshot": {
        "customer_id": ("BIGINT", False),
        "as_of_month": ("DATE", False),
        "prediction_horizon_months": ("INT", False),
        "tenure_months": ("INT", False),
        "products_count": ("INT", False),
        "balance_change_3m": ("NUMERIC(10,4)", True),
        "txn_count_change_3m": ("NUMERIC(10,4)", True),
        "login_count_change_6m": ("NUMERIC(10,4)", True),
        "complaint_count_6m": ("INT", False),
        "unresolved_complaints": ("INT", False),
        "days_since_last_login": ("INT", False),
        "salary_credit_consistency": ("NUMERIC(6,4)", False),
        "credit_utilization": ("NUMERIC(6,4)", False),
        "emi_to_income_ratio": ("NUMERIC(10,4)", False),
        "dormant_days": ("INT", False),
        "nps_avg_12m": ("NUMERIC(6,4)", True),
        "campaign_response_rate": ("NUMERIC(6,4)", True),
        "product_acquisition_velocity_6m": ("INT", False),
    "churned": ("BOOLEAN", False),
        "churn_date": ("DATE", True),
        "churn_reason": ("VARCHAR(100)", True),
    },
}


def split_table_columns(columns_def: str) -> list[str]:
    """Split column and constraint lines, ignoring commas inside parentheses."""
    parts = []
    current = []
    depth = 0
    for char in columns_def:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1

        if char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if current:
        parts.append("".join(current).strip())
    return parts


def parse_ddl_schema(ddl_path: str) -> Dict[str, Dict[str, Tuple[str, bool]]]:
    """Parses schema.sql directly in Python to extract exact types and nullability."""
    with open(ddl_path, "r") as f:
        content = f.read()

    # Remove SQL comments and multiple whitespace
    content_lines = []
    for line in content.splitlines():
        line_clean = re.sub(r"--.*$", "", line).strip()
        if line_clean:
            content_lines.append(line_clean)

    clean_ddl = " ".join(content_lines)

    # Split by semicolon to get statements
    statements = clean_ddl.split(";")

    parsed_schema = {}

    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue

        # Match CREATE TABLE statement
        match = re.match(r"CREATE\s+TABLE\s+(\w+)\s*\((.*)\)", stmt, re.IGNORECASE)
        if not match:
            continue

        table_name = match.group(1).lower()
        cols_def_str = match.group(2)

        column_definitions = split_table_columns(cols_def_str)
        table_cols = {}
        pk_cols: Set[str] = set()

        # Parse table constraints first
        for col_def in column_definitions:
            col_def_upper = col_def.upper()
            if col_def_upper.startswith("PRIMARY KEY"):
                # Table constraint: PRIMARY KEY (col1, col2)
                pk_match = re.search(
                    r"PRIMARY\s+KEY\s*\((.*?)\)", col_def, re.IGNORECASE
                )
                if pk_match:
                    for pk_col in pk_match.group(1).split(","):
                        pk_cols.add(pk_col.strip().lower())

        for col_def in column_definitions:
            col_def_upper = col_def.upper()

            # Skip table level constraints
            tokens = col_def.split()
            if not tokens:
                continue

            first_token = tokens[0].upper()
            if (
                first_token in ("PRIMARY", "FOREIGN", "CONSTRAINT", "UNIQUE")
                and len(tokens) > 1
                and tokens[1].upper() in ("KEY", "(")
            ):
                continue

            col_name = tokens[0].lower()

            # Match data type (handles VARCHAR(20), NUMERIC(18,2) etc.)
            type_match = re.search(
                r"^\w+(?:\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?", tokens[1], re.IGNORECASE
            )
            type_str = tokens[1]
            if type_match:
                type_str = type_match.group(0).upper().replace(" ", "")

            # Nullability determination
            is_nullable = True
            if "NOT NULL" in col_def_upper:
                is_nullable = False
            elif "PRIMARY KEY" in col_def_upper:
                is_nullable = False
                pk_cols.add(col_name)

            table_cols[col_name] = (type_str, is_nullable)

        # Retrofit composite primary keys to NOT NULL
        for col_name in table_cols:
            if col_name in pk_cols:
                type_str, _ = table_cols[col_name]
                table_cols[col_name] = (type_str, False)

        parsed_schema[table_name] = table_cols

    return parsed_schema


def test_schema_ddl_contract():
    """Validates that pipeline/schema.sql matches EXPECTED_SCHEMA columns, types, and nullability."""
    # Find schema.sql path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    schema_path = os.path.join(base_dir, "pipeline", "schema.sql")

    assert os.path.exists(schema_path), f"schema.sql not found at {schema_path}!"

    parsed = parse_ddl_schema(schema_path)

    # Assert all tables exist in DDL
    for table_name in EXPECTED_SCHEMA:
        assert table_name in parsed, (
            f"Table {table_name} is missing in pipeline/schema.sql DDL!"
        )

        expected_cols = EXPECTED_SCHEMA[table_name]
        parsed_cols = parsed[table_name]

        # Verify columns count and names
        for col_name in expected_cols:
            assert col_name in parsed_cols, (
                f"Column '{col_name}' is missing in table '{table_name}' in schema.sql DDL!"
            )

            expected_type, expected_null = expected_cols[col_name]
            parsed_type, parsed_null = parsed_cols[col_name]

            # Assert exact types
            assert parsed_type == expected_type, (
                f"Data type mismatch for {table_name}.{col_name}: "
                f"expected {expected_type}, got {parsed_type} in DDL!"
            )

            # Assert exact nullability
            assert parsed_null == expected_null, (
                f"Nullability mismatch for {table_name}.{col_name}: "
                f"expected nullable={expected_null}, got nullable={parsed_null} in DDL!"
            )

        # Assert no unexpected columns in parsed schema
        for col_name in parsed_cols:
            assert col_name in expected_cols, (
                f"Unexpected column '{col_name}' in table '{table_name}' inside schema.sql DDL!"
            )
