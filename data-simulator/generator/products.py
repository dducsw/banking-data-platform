import polars as pl
import numpy as np

def generate_initial_products(spine: pl.DataFrame, config, rng: np.random.Generator) -> pl.DataFrame:
    spine_dicts = spine.simulation_state.to_dicts()
    rows = []
    for row in spine_dicts:
        cid = row["customer_id"]
        rows.append({
            "customer_id": cid,
            "savings_flag": 1,
            "current_flag": int(rng.random() < 0.8),
            "debit_card_flag": int(rng.random() < 0.9),
            "credit_card_flag": int(rng.random() < 0.6),
            "personal_loan_flag": int(rng.random() < 0.2),
            "home_loan_flag": int(rng.random() < 0.1),
            "fixed_deposit_flag": int(rng.random() < 0.3),
            "insurance_flag": int(rng.random() < 0.4),
            "mutual_fund_flag": int(rng.random() < 0.2),
            "demat_account_flag": int(rng.random() < 0.1),
            "wealth_management_flag": int(rng.random() < 0.05),
        })
    return pl.DataFrame(rows)
