-- DuckDB SQL feature materialization for churn_feature_snapshot
-- Rewritten to use actual event-grain tables (v2 schema — no monthly snapshot aggregates).
--
-- Mapping from old → new:
--   account_monthly_snapshot      → account_balance_snapshots (is_month_end = TRUE)
--   customer_monthly_activity     → aggregate FROM transactions + login_events
--   digital_engagement_monthly    → login_events (last_login) + notifications (campaigns)
--   card_monthly_snapshot         → account_balance_snapshots for card accounts / cards.credit_limit
--   loan_monthly_snapshot         → loan_payments (emi_due_amount, outstanding_balance)
--   product_holdings_monthly      → COUNT DISTINCT from accounts + cards + loans
--
-- Grain: (customer_id, as_of_month, prediction_horizon_months)

DELETE FROM churn_feature_snapshot;

INSERT INTO churn_feature_snapshot (
    customer_id,
    as_of_month,
    prediction_horizon_months,
    tenure_months,
    products_count,
    balance_change_3m,
    txn_count_change_3m,
    login_count_change_6m,
    complaint_count_6m,
    unresolved_complaints,
    days_since_last_login,
    salary_credit_consistency,
    credit_utilization,
    emi_to_income_ratio,
    dormant_days,
    nps_avg_12m,
    campaign_response_rate,
    product_acquisition_velocity_6m,
    churned,
    churn_date,
    churn_reason
)
WITH spine AS (
    SELECT DISTINCT customer_id, as_of_month
    FROM customer_churn_label
    WHERE as_of_month >= (
        SELECT MIN(snapshot_month) + INTERVAL 6 MONTH
        FROM account_balance_snapshots
    )
),

-- ── BALANCE ──────────────────────────────────────────────────────────────────
-- account_balance_snapshots replaces account_monthly_snapshot.
-- Use month-end snapshots only (is_month_end = TRUE).
monthly_balances AS (
    SELECT
        abs2.customer_id,
        abs2.snapshot_month,
        AVG(abs2.end_of_day_balance) AS avg_bal
    FROM account_balance_snapshots abs2
    WHERE abs2.is_month_end = TRUE
    GROUP BY abs2.customer_id, abs2.snapshot_month
),
recent_bal AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        AVG(mb.avg_bal) AS avg_bal_recent
    FROM spine s
    LEFT JOIN monthly_balances mb ON s.customer_id = mb.customer_id
        AND mb.snapshot_month >= CAST(s.as_of_month - INTERVAL 3 MONTH AS DATE)
        AND mb.snapshot_month <  s.as_of_month
    GROUP BY s.customer_id, s.as_of_month
),
prior_bal AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        AVG(mb.avg_bal) AS avg_bal_prior
    FROM spine s
    LEFT JOIN monthly_balances mb ON s.customer_id = mb.customer_id
        AND mb.snapshot_month >= CAST(s.as_of_month - INTERVAL 6 MONTH AS DATE)
        AND mb.snapshot_month <  CAST(s.as_of_month - INTERVAL 3 MONTH AS DATE)
    GROUP BY s.customer_id, s.as_of_month
),

-- ── TRANSACTION COUNTS ────────────────────────────────────────────────────────
-- Replaces customer_monthly_activity.debit_txn_count / credit_txn_count / days_since_last_txn.
monthly_txns AS (
    SELECT
        t.customer_id,
        t.txn_month                                          AS snapshot_month,
        COUNT(*)                                             AS total_txn_count,
        MAX(t.txn_date)                                      AS last_txn_date
    FROM transactions t
    GROUP BY t.customer_id, t.txn_month
),
recent_txns AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        AVG(mt.total_txn_count) AS avg_txn_recent
    FROM spine s
    LEFT JOIN monthly_txns mt ON s.customer_id = mt.customer_id
        AND mt.snapshot_month >= CAST(s.as_of_month - INTERVAL 3 MONTH AS DATE)
        AND mt.snapshot_month <  s.as_of_month
    GROUP BY s.customer_id, s.as_of_month
),
prior_txns AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        AVG(mt.total_txn_count) AS avg_txn_prior
    FROM spine s
    LEFT JOIN monthly_txns mt ON s.customer_id = mt.customer_id
        AND mt.snapshot_month >= CAST(s.as_of_month - INTERVAL 6 MONTH AS DATE)
        AND mt.snapshot_month <  CAST(s.as_of_month - INTERVAL 3 MONTH AS DATE)
    GROUP BY s.customer_id, s.as_of_month
),

-- ── LOGIN COUNTS ──────────────────────────────────────────────────────────────
-- Replaces customer_monthly_activity.login_count and
--           digital_engagement_monthly.last_login_date.
monthly_logins AS (
    SELECT
        le.customer_id,
        le.login_month          AS snapshot_month,
        COUNT(*)                AS login_count,
        MAX(le.login_date)      AS last_login_date
    FROM login_events le
    GROUP BY le.customer_id, le.login_month
),
recent_logins AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        AVG(ml.login_count) AS avg_login_recent
    FROM spine s
    LEFT JOIN monthly_logins ml ON s.customer_id = ml.customer_id
        AND ml.snapshot_month >= CAST(s.as_of_month - INTERVAL 6 MONTH AS DATE)
        AND ml.snapshot_month <  s.as_of_month
    GROUP BY s.customer_id, s.as_of_month
),
prior_logins AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        AVG(ml.login_count) AS avg_login_prior
    FROM spine s
    LEFT JOIN monthly_logins ml ON s.customer_id = ml.customer_id
        AND ml.snapshot_month >= CAST(s.as_of_month - INTERVAL 12 MONTH AS DATE)
        AND ml.snapshot_month <  CAST(s.as_of_month - INTERVAL 6 MONTH AS DATE)
    GROUP BY s.customer_id, s.as_of_month
),

-- ── COMPLAINTS ───────────────────────────────────────────────────────────────
complaints_6m AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        COUNT(c.complaint_id) AS complaint_count_6m
    FROM spine s
    LEFT JOIN complaints c ON s.customer_id = c.customer_id
        AND c.complaint_month >= CAST(s.as_of_month - INTERVAL 6 MONTH AS DATE)
        AND c.complaint_month <  s.as_of_month
    GROUP BY s.customer_id, s.as_of_month
),
unresolved_comps AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        COUNT(c.complaint_id) AS unresolved_complaints
    FROM spine s
    LEFT JOIN complaints c ON s.customer_id = c.customer_id
        AND c.complaint_month < s.as_of_month
        AND c.resolved_flag = FALSE
    GROUP BY s.customer_id, s.as_of_month
),

-- ── DAYS SINCE LAST LOGIN ────────────────────────────────────────────────────
-- Replaces digital_engagement_monthly.last_login_date lookup.
days_since_login AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        COALESCE(
            date_diff('day', ml.last_login_date, s.as_of_month),
            180
        ) AS days_since_last_login
    FROM spine s
    LEFT JOIN monthly_logins ml ON s.customer_id = ml.customer_id
        AND ml.snapshot_month = CAST(s.as_of_month - INTERVAL 1 MONTH AS DATE)
),

-- ── SALARY CREDIT CONSISTENCY ────────────────────────────────────────────────
-- Replaces account_monthly_snapshot.salary_credit_amount > 0 filter.
-- Counts distinct txn_months in last 6M where at least one salary credit exists.
salary_consistency AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        COUNT(DISTINCT t.txn_month) / 6.0 AS salary_credit_consistency
    FROM spine s
    LEFT JOIN transactions t ON s.customer_id = t.customer_id
        AND t.is_salary_credit = TRUE
        AND t.txn_month >= CAST(s.as_of_month - INTERVAL 6 MONTH AS DATE)
        AND t.txn_month <  s.as_of_month
    GROUP BY s.customer_id, s.as_of_month
),

-- ── CREDIT UTILIZATION ───────────────────────────────────────────────────────
-- Replaces card_monthly_snapshot.utilization_rate.
-- Proxy: month-end balance on accounts linked to active credit cards / credit_limit.
credit_util AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        COALESCE(
            SUM(abs2.end_of_day_balance) / NULLIF(SUM(c.credit_limit), 0),
            0.0
        ) AS credit_utilization
    FROM spine s
    LEFT JOIN cards c ON s.customer_id = c.customer_id
        AND c.card_type = 'Credit'
        AND c.card_status = 'Active'
    LEFT JOIN accounts acc ON c.customer_id = acc.customer_id
        AND acc.account_type = 'Credit Card'
    LEFT JOIN account_balance_snapshots abs2
        ON  acc.account_id   = abs2.account_id
        AND abs2.snapshot_month = CAST(s.as_of_month - INTERVAL 1 MONTH AS DATE)
        AND abs2.is_month_end = TRUE
    GROUP BY s.customer_id, s.as_of_month
),

-- ── EMI-TO-INCOME RATIO ──────────────────────────────────────────────────────
-- Replaces loan_monthly_snapshot.emi_amount lookup.
-- Uses loan_payments.emi_due_amount for the prior month.
emi_ratio AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        COALESCE(lp_agg.total_emi, 0.0) / (cu.annual_income / 12.0) AS emi_to_income_ratio
    FROM spine s
    JOIN customers cu ON s.customer_id = cu.customer_id
    LEFT JOIN (
        SELECT customer_id, payment_month, SUM(emi_due_amount) AS total_emi
        FROM loan_payments
        GROUP BY customer_id, payment_month
    ) lp_agg ON s.customer_id = lp_agg.customer_id
        AND lp_agg.payment_month = CAST(s.as_of_month - INTERVAL 1 MONTH AS DATE)
),

-- ── DORMANT DAYS ─────────────────────────────────────────────────────────────
-- Replaces customer_monthly_activity.days_since_last_txn.
dorm_days AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        COALESCE(
            date_diff('day', mt.last_txn_date, s.as_of_month),
            180
        ) AS dormant_days
    FROM spine s
    LEFT JOIN monthly_txns mt ON s.customer_id = mt.customer_id
        AND mt.snapshot_month = CAST(s.as_of_month - INTERVAL 1 MONTH AS DATE)
),

-- ── NPS ──────────────────────────────────────────────────────────────────────
nps_avg AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        AVG(f.nps_score) AS nps_avg_12m
    FROM spine s
    LEFT JOIN feedback f ON s.customer_id = f.customer_id
        AND f.feedback_month >= CAST(s.as_of_month - INTERVAL 12 MONTH AS DATE)
        AND f.feedback_month <  s.as_of_month
        AND f.nps_score IS NOT NULL
    GROUP BY s.customer_id, s.as_of_month
),

-- ── CAMPAIGN RESPONSE RATE ───────────────────────────────────────────────────
-- Replaces digital_engagement_monthly.campaigns_responded / campaigns_received.
-- Uses notifications WHERE notification_type = 'Campaign'.
campaign_rate AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        SUM(CASE WHEN n.opened THEN 1 ELSE 0 END) * 1.0 /
        NULLIF(COUNT(n.notification_id), 0) AS campaign_response_rate
    FROM spine s
    LEFT JOIN notifications n ON s.customer_id = n.customer_id
        AND n.notification_type = 'Campaign'
        AND n.sent_month >= CAST(s.as_of_month - INTERVAL 12 MONTH AS DATE)
        AND n.sent_month <  s.as_of_month
    GROUP BY s.customer_id, s.as_of_month
),

-- ── PRODUCT COUNTS ───────────────────────────────────────────────────────────
-- Replaces product_holdings_monthly.products_count.
-- Derives from accounts (open_date), cards (issue_date), loans (disbursement_date).
all_products AS (
    SELECT customer_id, open_date         AS start_date FROM accounts
    UNION ALL
    SELECT customer_id, issue_date        AS start_date FROM cards
    UNION ALL
    SELECT customer_id, disbursement_date AS start_date FROM loans
),
prod_count AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        COUNT(*) AS products_count
    FROM spine s
    LEFT JOIN all_products ap ON s.customer_id = ap.customer_id
        AND ap.start_date < s.as_of_month
    GROUP BY s.customer_id, s.as_of_month
),
prod_count_6m_ago AS (
    SELECT
        s.customer_id,
        s.as_of_month,
        COUNT(*) AS products_count_6m_ago
    FROM spine s
    LEFT JOIN all_products ap ON s.customer_id = ap.customer_id
        AND ap.start_date < CAST(s.as_of_month - INTERVAL 6 MONTH AS DATE)
    GROUP BY s.customer_id, s.as_of_month
)

SELECT
    lbl.customer_id,
    lbl.as_of_month,
    lbl.prediction_horizon_months,
    -- tenure_months
    (EXTRACT(year  FROM lbl.as_of_month) - EXTRACT(year  FROM c.customer_since)) * 12 +
    (EXTRACT(month FROM lbl.as_of_month) - EXTRACT(month FROM c.customer_since)) AS tenure_months,
    COALESCE(pc.products_count, 1) AS products_count,
    -- balance_change_3m = (avg_bal[M-3,M) - avg_bal[M-6,M-3)) / avg_bal[M-6,M-3)
    ROUND(CASE WHEN pb.avg_bal_prior > 0
          THEN (rb.avg_bal_recent - pb.avg_bal_prior) / pb.avg_bal_prior
          ELSE 0.0 END, 4) AS balance_change_3m,
    -- txn_count_change_3m = (avg_txn[M-3,M) - avg_txn[M-6,M-3)) / avg_txn[M-6,M-3)
    ROUND(CASE WHEN pt.avg_txn_prior > 0
          THEN (rt.avg_txn_recent - pt.avg_txn_prior) / pt.avg_txn_prior
          ELSE 0.0 END, 4) AS txn_count_change_3m,
    -- login_count_change_6m = (avg_login[M-6,M) - avg_login[M-12,M-6)) / avg_login[M-12,M-6)
    ROUND(CASE WHEN pll.avg_login_prior > 0
          THEN (rl.avg_login_recent - pll.avg_login_prior) / pll.avg_login_prior
          ELSE 0.0 END, 4) AS login_count_change_6m,
    COALESCE(c6.complaint_count_6m, 0)   AS complaint_count_6m,
    COALESCE(uc.unresolved_complaints, 0) AS unresolved_complaints,
    dsl.days_since_last_login,
    ROUND(COALESCE(sc.salary_credit_consistency, 0.0), 4) AS salary_credit_consistency,
    ROUND(COALESCE(cu.credit_utilization, 0.0), 4)        AS credit_utilization,
    ROUND(COALESCE(er.emi_to_income_ratio, 0.0), 4)       AS emi_to_income_ratio,
    dd.dormant_days,
    ROUND(COALESCE(nps.nps_avg_12m, 8.0), 4)              AS nps_avg_12m,
    ROUND(COALESCE(cr.campaign_response_rate, 0.0), 4)    AS campaign_response_rate,
    -- product_acquisition_velocity_6m = products now - products 6M ago
    GREATEST(0, COALESCE(pc.products_count, 0) - COALESCE(p6.products_count_6m_ago, 0))
        AS product_acquisition_velocity_6m,
    lbl.churned,
    lbl.churn_date,
    lbl.churn_reason
FROM customer_churn_label lbl
JOIN spine          s   ON lbl.customer_id = s.customer_id   AND lbl.as_of_month = s.as_of_month
JOIN customers      c   ON lbl.customer_id = c.customer_id
LEFT JOIN prod_count         pc  ON lbl.customer_id = pc.customer_id  AND lbl.as_of_month = pc.as_of_month
LEFT JOIN prod_count_6m_ago  p6  ON lbl.customer_id = p6.customer_id  AND lbl.as_of_month = p6.as_of_month
LEFT JOIN recent_bal         rb  ON lbl.customer_id = rb.customer_id  AND lbl.as_of_month = rb.as_of_month
LEFT JOIN prior_bal          pb  ON lbl.customer_id = pb.customer_id  AND lbl.as_of_month = pb.as_of_month
LEFT JOIN recent_txns        rt  ON lbl.customer_id = rt.customer_id  AND lbl.as_of_month = rt.as_of_month
LEFT JOIN prior_txns         pt  ON lbl.customer_id = pt.customer_id  AND lbl.as_of_month = pt.as_of_month
LEFT JOIN recent_logins      rl  ON lbl.customer_id = rl.customer_id  AND lbl.as_of_month = rl.as_of_month
LEFT JOIN prior_logins       pll ON lbl.customer_id = pll.customer_id AND lbl.as_of_month = pll.as_of_month
LEFT JOIN complaints_6m      c6  ON lbl.customer_id = c6.customer_id  AND lbl.as_of_month = c6.as_of_month
LEFT JOIN unresolved_comps   uc  ON lbl.customer_id = uc.customer_id  AND lbl.as_of_month = uc.as_of_month
LEFT JOIN days_since_login   dsl ON lbl.customer_id = dsl.customer_id AND lbl.as_of_month = dsl.as_of_month
LEFT JOIN salary_consistency sc  ON lbl.customer_id = sc.customer_id  AND lbl.as_of_month = sc.as_of_month
LEFT JOIN credit_util        cu  ON lbl.customer_id = cu.customer_id  AND lbl.as_of_month = cu.as_of_month
LEFT JOIN emi_ratio          er  ON lbl.customer_id = er.customer_id  AND lbl.as_of_month = er.as_of_month
LEFT JOIN dorm_days          dd  ON lbl.customer_id = dd.customer_id  AND lbl.as_of_month = dd.as_of_month
LEFT JOIN nps_avg            nps ON lbl.customer_id = nps.customer_id AND lbl.as_of_month = nps.as_of_month
LEFT JOIN campaign_rate      cr  ON lbl.customer_id = cr.customer_id  AND lbl.as_of_month = cr.as_of_month;
