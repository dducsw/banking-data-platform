import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count

def read_postgres_table(spark: SparkSession, table_name: str, pg_host: str = "postgres.postgres.svc.cluster.local", pg_port: int = 5432, pg_db: str = "banking", pg_user: str = "postgres", pg_pass: str = "postgres123"):
    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    print(f"--> Reading table [{table_name}] from {jdbc_url}...")
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", pg_user)
        .option("password", pg_pass)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return df

def main():
    pg_host = os.getenv("PG_HOST", "postgres.postgres.svc.cluster.local")
    pg_port = int(os.getenv("PG_PORT", "5432"))
    pg_db = os.getenv("PG_DB", "banking")
    pg_user = os.getenv("PG_USER", "postgres")
    pg_pass = os.getenv("PG_PASSWORD", "postgres123")

    spark = SparkSession.builder.appName("SparkReadPostgresExample").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"=== Spark Version: {spark.version} ===")

    # 1. Read PostgreSQL Tables
    df_customers = read_postgres_table(spark, "customers", pg_host, pg_port, pg_db, pg_user, pg_pass)
    df_accounts = read_postgres_table(spark, "accounts", pg_host, pg_port, pg_db, pg_user, pg_pass)
    df_transactions = read_postgres_table(spark, "transactions", pg_host, pg_port, pg_db, pg_user, pg_pass)
    df_branches = read_postgres_table(spark, "branches", pg_host, pg_port, pg_db, pg_user, pg_pass)

    print("\n=== 1. Customers Table ===")
    df_customers.select("customer_id", "first_name", "last_name", "occupation", "annual_income", "city").show(truncate=False)

    print("\n=== 2. Accounts Table ===")
    df_accounts.select("account_id", "customer_id", "branch_code", "account_type", "account_status", "account_currency").show(truncate=False)

    print("\n=== 3. Transactions Table ===")
    df_transactions.select("transaction_id", "account_id", "customer_id", "txn_type", "direction", "amount", "currency", "transaction_description").show(truncate=False)

    # 2. Perform Join Analysis: Customer Total Transaction Volume
    print("\n=== 4. Analytical Join: Customer Transaction Summary ===")
    c = df_customers.select(col("customer_id"), col("first_name"), col("last_name"), col("occupation"), col("city").alias("customer_city"))
    a = df_accounts.select("account_id", "customer_id")
    t = df_transactions.select("transaction_id", "account_id", "customer_id", "amount")

    df_joined = (
        c
        .join(a, "customer_id")
        .join(t, ["customer_id", "account_id"])
        .groupBy("customer_id", "first_name", "last_name", "occupation", "customer_city")
        .agg(
            _count("transaction_id").alias("total_txns"),
            _sum("amount").alias("total_amount_vnd")
        )
        .orderBy("customer_id")
    )
    df_joined.show(truncate=False)

    print("\n=== PostgreSQL Read & Analytics Test Completed Successfully! ===")

if __name__ == "__main__":
    main()
