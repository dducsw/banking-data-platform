import polars as pl
from datetime import date, timedelta

def generate_account_balance_snapshots(ledger_df: pl.DataFrame) -> pl.DataFrame:
    if ledger_df.is_empty():
        return pl.DataFrame()
        
    # Get last transaction per day per account
    daily = ledger_df.sort("entry_timestamp").group_by(["account_id", "customer_id", "entry_date"]).last()
    
    # Add snapshot_month and is_month_end
    # is_month_end is True if tomorrow is a new month
    daily = daily.with_columns([
        pl.col("entry_date").dt.truncate("1mo").cast(pl.Date).alias("snapshot_month"),
        (pl.col("entry_date") + pl.duration(days=1)).dt.month().ne(pl.col("entry_date").dt.month()).alias("is_month_end")
    ])
    
    daily = daily.select([
        pl.arange(1, pl.len() + 1).alias("snapshot_id"),
        pl.col("account_id"),
        pl.col("customer_id"),
        pl.col("entry_date").alias("snapshot_date"),
        pl.col("snapshot_month"),
        pl.col("running_balance").alias("end_of_day_balance"),
        pl.col("is_month_end")
    ])
    return daily
