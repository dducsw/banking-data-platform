-- PostgreSQL DDL Schema -- Big Data Analytics Banking Simulator
-- Grain: most detailed events only; all aggregation happens downstream.
-- 11 tables: 5 master/dim + 6 fact/event

DROP TABLE IF EXISTS churn_feature_snapshot CASCADE;
DROP TABLE IF EXISTS customer_churn_label CASCADE;
DROP TABLE IF EXISTS account_balance_snapshots CASCADE;
DROP TABLE IF EXISTS account_ledger CASCADE;
DROP TABLE IF EXISTS loan_payments CASCADE;
DROP TABLE IF EXISTS notifications CASCADE;
DROP TABLE IF EXISTS login_events CASCADE;
DROP TABLE IF EXISTS feedback CASCADE;
DROP TABLE IF EXISTS complaints CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS churn_simulation_state CASCADE;
DROP TABLE IF EXISTS loans CASCADE;
DROP TABLE IF EXISTS cards CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS branches CASCADE;
DROP TABLE IF EXISTS merchants CASCADE;


-- 0a. merchants  (static dim — normalize merchant names + MCC codes)
CREATE TABLE merchants (
    merchant_id       INT          PRIMARY KEY,
    merchant_name     VARCHAR(150) NOT NULL UNIQUE,
    merchant_category VARCHAR(100) NOT NULL,
    mcc_code          VARCHAR(4)   NOT NULL,   -- ISO 18245
    merchant_type     VARCHAR(20)  NOT NULL,   -- physical / online / internal
    is_online         BOOLEAN      NOT NULL
);



-- 1. branches
CREATE TABLE branches (
    branch_code     VARCHAR(20)  PRIMARY KEY,
    branch_name     VARCHAR(100) NOT NULL,
    city            VARCHAR(100) NOT NULL,
    state           VARCHAR(100) NOT NULL,
    region          VARCHAR(50)  NOT NULL,
    branch_type     VARCHAR(30)  NOT NULL,
    open_date       DATE         NOT NULL,
    closure_date    DATE         NULL,
    customer_weight INT          NOT NULL
);

-- 2. customers
CREATE TABLE customers (
    customer_id     BIGINT       PRIMARY KEY,
    cif_number      VARCHAR(20)  NOT NULL,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    date_of_birth   DATE         NOT NULL,
    gender          VARCHAR(20)  NOT NULL,
    marital_status  VARCHAR(20)  NOT NULL,
    occupation      VARCHAR(100) NOT NULL,
    employment_type VARCHAR(50)  NOT NULL,
    annual_income   NUMERIC(18,2) NOT NULL,
    customer_since  DATE         NOT NULL,
    address         VARCHAR(255) NULL,
    city            VARCHAR(100) NOT NULL,
    state           VARCHAR(100) NOT NULL,
    zipcode         VARCHAR(20)  NULL,
    country         VARCHAR(100) NOT NULL,
    lat             NUMERIC(10,6) NULL,
    lon             NUMERIC(10,6) NULL,
    email           VARCHAR(100) NULL,
    phone           VARCHAR(50)  NULL,
    persona         VARCHAR(50)  NULL,
    kyc_status      VARCHAR(20)  NOT NULL,
    is_active       BOOLEAN      NOT NULL
);

-- 3. accounts
CREATE TABLE accounts (
    account_id          BIGINT PRIMARY KEY,
    customer_id         BIGINT REFERENCES customers(customer_id),
    branch_code         VARCHAR(20) REFERENCES branches(branch_code),
    account_type        VARCHAR(50)  NOT NULL,
    open_date           DATE         NOT NULL,
    account_status      VARCHAR(20)  NOT NULL,
    account_currency    VARCHAR(3)   NOT NULL,
    salary_account_flag BOOLEAN      NOT NULL,
    overdraft_limit     NUMERIC(18,2) NOT NULL,
    account_close_date  DATE         NULL
);

-- 4. cards
CREATE TABLE cards (
    card_id          BIGINT PRIMARY KEY,
    customer_id      BIGINT REFERENCES customers(customer_id),
    card_type        VARCHAR(20)  NOT NULL,
    network          VARCHAR(20)  NOT NULL,
    issue_date       DATE         NOT NULL,
    expiry_date      DATE         NOT NULL,
    card_status      VARCHAR(20)  NOT NULL,
    primary_card_flag BOOLEAN     NOT NULL,
    credit_limit     NUMERIC(18,2) NOT NULL,
    rewards_program  VARCHAR(50)  NOT NULL,
    reward_tier      VARCHAR(20)  NOT NULL
);

-- 5. loans
CREATE TABLE loans (
    loan_id             BIGINT PRIMARY KEY,
    customer_id         BIGINT REFERENCES customers(customer_id),
    branch_code         VARCHAR(20) REFERENCES branches(branch_code),
    loan_type           VARCHAR(50)  NOT NULL,
    sanctioned_amount   NUMERIC(18,2) NOT NULL,
    disbursement_date   DATE         NOT NULL,
    interest_rate       NUMERIC(6,3) NOT NULL,
    tenure_months       INT          NOT NULL,
    emi_amount          NUMERIC(18,2) NOT NULL,
    loan_purpose        VARCHAR(100) NOT NULL,
    origination_channel VARCHAR(50)  NOT NULL,
    loan_status         VARCHAR(20)  NOT NULL,
    maturity_date       DATE         NOT NULL
);

-- 6. churn_simulation_state  (ground truth — not a ML label table)
CREATE TABLE churn_simulation_state (
    customer_id              BIGINT PRIMARY KEY REFERENCES customers(customer_id),
    persona                  VARCHAR(50)  NOT NULL,
    low_sensitivity_segment  BOOLEAN      NOT NULL,
    churn_month              DATE         NULL,
    churned_flag             BOOLEAN      NOT NULL,
    churn_reason             VARCHAR(100) NULL,
    active_months_generated  INT          NOT NULL
);

-- 7. transactions  (raw, 1 row per transaction)
CREATE TABLE transactions (
    transaction_id    BIGINT PRIMARY KEY,
    account_id        BIGINT REFERENCES accounts(account_id),
    customer_id       BIGINT REFERENCES customers(customer_id),
    -- C2C / transfer recipient (NULL for non-transfer transactions)
    received_customer_id BIGINT REFERENCES customers(customer_id) NULL,
    received_account_id  BIGINT REFERENCES accounts(account_id)   NULL,
    txn_timestamp     TIMESTAMP    NOT NULL,
    txn_date          DATE         NOT NULL,
    txn_month         DATE         NOT NULL,
    txn_type          VARCHAR(50)  NOT NULL,
    direction         VARCHAR(10)  NOT NULL,   -- Credit / Debit
    channel           VARCHAR(30)  NOT NULL,
    amount            NUMERIC(18,2) NOT NULL,
    currency          VARCHAR(3)   NOT NULL,
    transaction_category VARCHAR(100) NOT NULL,
    transaction_description VARCHAR(150) NOT NULL,
    merchant_id       INT REFERENCES merchants(merchant_id) NULL,
    counterparty_type VARCHAR(50)  NOT NULL,   -- Merchant / Individual / Bank / Corporate
    city              VARCHAR(100) NOT NULL,
    state             VARCHAR(100) NOT NULL,
    is_salary_credit  BOOLEAN      NOT NULL,
    is_fee            BOOLEAN      NOT NULL,
    is_reversal       BOOLEAN      NOT NULL,
    balance_after_txn NUMERIC(18,2) NOT NULL,
    is_fraud          BOOLEAN      NOT NULL,
    is_disputed       BOOLEAN      NOT NULL,
    risk_score        NUMERIC(5,4) NOT NULL,
    device_id         VARCHAR(100) NULL,
    ip_address        VARCHAR(45)  NULL,
    geolocation       VARCHAR(150) NULL
);

-- 8. login_events  (1 row per login session — replaces digital_engagement_monthly)
CREATE TABLE login_events (
    session_id               BIGINT PRIMARY KEY,
    customer_id              BIGINT REFERENCES customers(customer_id),
    login_timestamp          TIMESTAMP    NOT NULL,
    login_date               DATE         NOT NULL,
    login_month              DATE         NOT NULL,
    channel                  VARCHAR(20)  NOT NULL,   -- Mobile App / Web Browser
    device_type              VARCHAR(30)  NOT NULL,   -- iOS / Android / Desktop
    session_duration_seconds INT          NOT NULL,
    page_views               INT          NOT NULL,
    logout_type              VARCHAR(20)  NOT NULL,   -- Manual / Timeout
    is_successful            BOOLEAN      NOT NULL,
    failed_attempt_count     INT          NOT NULL,
    otp_used                 BOOLEAN      NOT NULL,
    biometric_used           BOOLEAN      NOT NULL,
    ip_address               VARCHAR(45)  NOT NULL,
    is_new_device            BOOLEAN      NOT NULL
);

-- 9. notifications  (1 row per notification — replaces digital_engagement_monthly push/email cols)
CREATE TABLE notifications (
    notification_id   BIGINT PRIMARY KEY,
    customer_id       BIGINT REFERENCES customers(customer_id),
    sent_at           TIMESTAMP    NOT NULL,
    sent_date         DATE         NOT NULL,
    sent_month        DATE         NOT NULL,
    channel           VARCHAR(20)  NOT NULL,   -- Push / Email
    notification_type VARCHAR(50)  NOT NULL,   -- Balance Alert / Payment Due / Campaign / ...
    opened            BOOLEAN      NOT NULL,
    opened_at         TIMESTAMP    NULL
);

-- 10. loan_payments  (1 row per loan per month — replaces loan_monthly_snapshot)
CREATE TABLE loan_payments (
    payment_id          BIGINT PRIMARY KEY,
    loan_id             BIGINT REFERENCES loans(loan_id),
    customer_id         BIGINT REFERENCES customers(customer_id),
    payment_date        DATE         NOT NULL,
    payment_month       DATE         NOT NULL,
    payment_timestamp   TIMESTAMP    NOT NULL,
    emi_due_amount      NUMERIC(18,2) NOT NULL,
    emi_paid_amount     NUMERIC(18,2) NOT NULL,
    principal_paid      NUMERIC(18,2) NOT NULL,
    interest_paid       NUMERIC(18,2) NOT NULL,
    outstanding_balance NUMERIC(18,2) NOT NULL,
    dpd_days            INT          NOT NULL,
    loan_status         VARCHAR(20)  NOT NULL,
    is_delinquent       BOOLEAN      NOT NULL,
    restructuring_flag  BOOLEAN      NOT NULL
);

-- 11. complaints  (raw events)
CREATE TABLE complaints (
    complaint_id    BIGINT PRIMARY KEY,
    customer_id     BIGINT REFERENCES customers(customer_id),
    complaint_date  DATE         NOT NULL,
    complaint_month DATE         NOT NULL,
    channel         VARCHAR(50)  NOT NULL,
    category        VARCHAR(100) NOT NULL,
    severity        VARCHAR(20)  NOT NULL,
    resolution_days INT          NULL,
    resolved_flag   BOOLEAN      NOT NULL,
    escalated_flag  BOOLEAN      NOT NULL,
    csat_score      INT          NULL,
    root_cause      VARCHAR(100) NOT NULL,
    status          VARCHAR(20)  NOT NULL
);

-- 12. feedback  (raw events)
CREATE TABLE feedback (
    feedback_id     BIGINT PRIMARY KEY,
    customer_id     BIGINT REFERENCES customers(customer_id),
    feedback_date   DATE         NOT NULL,
    feedback_month  DATE         NOT NULL,
    survey_channel  VARCHAR(50)  NOT NULL,
    survey_topic    VARCHAR(100) NOT NULL,
    nps_score       INT          NULL,
    csat_score      INT          NULL
);

-- 13. account_ledger  (light ledger — 1 entry per transaction, running balance)
CREATE TABLE account_ledger (
    entry_id          BIGINT PRIMARY KEY,
    transaction_id    BIGINT REFERENCES transactions(transaction_id),
    account_id        BIGINT REFERENCES accounts(account_id),
    customer_id       BIGINT REFERENCES customers(customer_id),
    entry_date        DATE         NOT NULL,
    entry_month       DATE         NOT NULL,
    entry_timestamp   TIMESTAMP    NOT NULL,
    entry_type        VARCHAR(10)  NOT NULL,   -- Debit / Credit
    debit_amount      NUMERIC(18,2) NOT NULL,  -- 0 if Credit entry
    credit_amount     NUMERIC(18,2) NOT NULL,  -- 0 if Debit entry
    amount            NUMERIC(18,2) NOT NULL,
    currency          VARCHAR(3)   NOT NULL,
    running_balance   NUMERIC(18,2) NOT NULL,
    description       VARCHAR(200) NOT NULL,
    txn_type          VARCHAR(50)  NOT NULL,
    channel           VARCHAR(30)  NOT NULL,
    reference_number  VARCHAR(20)  NOT NULL
);

-- account_balance_snapshots
CREATE TABLE account_balance_snapshots (
    snapshot_id        BIGINT PRIMARY KEY,
    account_id         BIGINT REFERENCES accounts(account_id),
    customer_id        BIGINT REFERENCES customers(customer_id),
    snapshot_date      DATE NOT NULL,
    snapshot_month     DATE NOT NULL,
    end_of_day_balance NUMERIC(18,2) NOT NULL,
    is_month_end       BOOLEAN NOT NULL
);

-- 14. customer_churn_label  (ML training labels — grain: customer × as_of_month × horizon)
DROP TABLE IF EXISTS customer_churn_label CASCADE;
CREATE TABLE customer_churn_label (
    customer_id               BIGINT REFERENCES customers(customer_id),
    as_of_month               DATE   NOT NULL,
    prediction_horizon_months INT    NOT NULL,
    churned                   BOOLEAN      NOT NULL,
    churn_date                DATE         NULL,
    churn_reason              VARCHAR(100) NULL,
    PRIMARY KEY (customer_id, as_of_month, prediction_horizon_months)
);

-- 15. churn_feature_snapshot  (materialized features — output of build_features.sql)
DROP TABLE IF EXISTS churn_feature_snapshot CASCADE;
CREATE TABLE churn_feature_snapshot (
    customer_id                     BIGINT       NOT NULL,
    as_of_month                     DATE         NOT NULL,
    prediction_horizon_months       INT          NOT NULL,
    tenure_months                   INT          NOT NULL,
    products_count                  INT          NOT NULL,
    balance_change_3m               NUMERIC(10,4),
    txn_count_change_3m             NUMERIC(10,4),
    login_count_change_6m           NUMERIC(10,4),
    complaint_count_6m              INT          NOT NULL,
    unresolved_complaints           INT          NOT NULL,
    days_since_last_login           INT          NOT NULL,
    salary_credit_consistency       NUMERIC(6,4) NOT NULL,
    credit_utilization              NUMERIC(6,4) NOT NULL,
    emi_to_income_ratio             NUMERIC(10,4) NOT NULL,
    dormant_days                    INT          NOT NULL,
    nps_avg_12m                     NUMERIC(6,4),
    campaign_response_rate          NUMERIC(6,4),
    product_acquisition_velocity_6m INT          NOT NULL,
    churned                         BOOLEAN      NOT NULL,
    churn_date                      DATE         NULL,
    churn_reason                    VARCHAR(100) NULL,
    PRIMARY KEY (customer_id, as_of_month, prediction_horizon_months)
);
