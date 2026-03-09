# Databricks notebook source
# Gold Layer - Dimensional Modelling
#
# Builds a star schema on top of Silver clean data.
# Design decisions:
#   - SHA2 surrogate keys: deterministic and idempotent - same input always
#     produces the same key regardless of when the pipeline runs. This makes
#     MERGE operations safe and avoids key drift across pipeline runs.
#   - dim_category encodes business logic (category_group, category_type) that
#     does not exist in the source system. This is intentional - the warehouse
#     layer is where business knowledge gets formalised.
#   - Foreign key validation runs before every write. A loud failure is always
#     preferable to silently writing a fact table with unresolvable joins.

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
import re

CATALOG   = "workspace"
SILVER_DB = f"{CATALOG}.silver"
GOLD_DB   = f"{CATALOG}.gold"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")


# Redefine date UDF - each notebook is self-contained.
def parse_mixed_date(date_str):
    if date_str is None or str(date_str).strip() == "":
        return (None, False)
    s = str(date_str).strip()
    try:
        from datetime import datetime
        if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
            return (datetime.strptime(s, "%Y-%m-%d").date().isoformat(), False)
        if re.match(r'^\d{2}/\d{2}/\d{4}$', s):
            return (datetime.strptime(s, "%m/%d/%Y").date().isoformat(), False)
        if re.match(r'^\d{2}-\d{2}-\d{4}$', s):
            parts  = s.split("-")
            first  = int(parts[0])
            second = int(parts[1])
            if first > 12:
                return (datetime.strptime(s, "%d-%m-%Y").date().isoformat(), False)
            elif second > 12:
                return (datetime.strptime(s, "%m-%d-%Y").date().isoformat(), False)
            else:
                return (datetime.strptime(s, "%m-%d-%Y").date().isoformat(), True)
    except Exception:
        return (None, False)
    return (None, False)

date_schema = StructType([
    StructField("parsed_date",    StringType(),  True),
    StructField("date_ambiguous", BooleanType(), True),
])
parse_date_udf = F.udf(parse_mixed_date, date_schema)


# dim_date
# Generated from a date spine - no dependency on transaction data.
# Covers 2023-2026 to accommodate edge cases without breaking the pipeline
# if dates slightly outside the expected 2023-2024 range appear in future loads.

dim_date = spark.sql("""
    SELECT explode(
        sequence(to_date('2023-01-01'), to_date('2026-12-31'), interval 1 day)
    ) AS full_date
""")

dim_date = (
    dim_date
    .withColumn("date_key",       F.date_format(F.col("full_date"), "yyyyMMdd"))
    .withColumn("day_of_week",    F.date_format(F.col("full_date"), "EEEE"))
    .withColumn("day_of_month",   F.dayofmonth(F.col("full_date")))
    .withColumn("day_of_year",    F.dayofyear(F.col("full_date")))
    .withColumn("week_of_year",   F.weekofyear(F.col("full_date")))
    .withColumn("month_num",      F.month(F.col("full_date")))
    .withColumn("month_name",     F.date_format(F.col("full_date"), "MMMM"))
    .withColumn("quarter",        F.quarter(F.col("full_date")))
    .withColumn("year",           F.year(F.col("full_date")))
    .withColumn("is_weekend",     F.dayofweek(F.col("full_date")).isin([1, 7]))
    .withColumn("is_month_end",   F.col("full_date") == F.last_day(F.col("full_date")))
    .withColumn("is_month_start", F.dayofmonth(F.col("full_date")) == 1)
    .select(
        "date_key", "full_date", "day_of_week", "day_of_month",
        "day_of_year", "week_of_year", "month_num", "month_name",
        "quarter", "year", "is_weekend", "is_month_end", "is_month_start"
    )
)

(
    dim_date.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD_DB}.dim_date")
)

print(f"{GOLD_DB}.dim_date: {dim_date.count()} rows")


# dim_accounts
# Sourced from Bronze - Silver only cleaned transactions, not accounts.
# account_id is validated before key generation. A NULL surrogate key
# would silently break every fact-to-dimension join downstream.

accounts_raw = spark.table(f"{CATALOG}.bronze.accounts")

null_ids = accounts_raw.filter(F.col("account_id").isNull()).count()
if null_ids > 0:
    raise ValueError(f"{null_ids} NULL account_ids in source - cannot generate surrogate keys")

dim_accounts = (
    accounts_raw
    .withColumn("customer_name", F.trim(F.initcap(F.col("customer_name"))))
    .withColumn("account_type",  F.trim(F.initcap(F.col("account_type"))))
    .withColumn("region",        F.trim(F.initcap(F.col("region"))))
    .withColumn("_date_parsed",  parse_date_udf(F.col("created_date")))
    .withColumn("created_date",  F.to_date(F.col("_date_parsed.parsed_date")))
    .drop("_date_parsed")
    .withColumn("account_key",   F.sha2(F.col("account_id").cast("string"), 256))
    .select("account_key", "account_id", "customer_name", "account_type", "region", "created_date")
)

null_keys = dim_accounts.filter(F.col("account_key").isNull()).count()
if null_keys > 0:
    raise ValueError(f"{null_keys} NULL surrogate keys generated in dim_accounts")

(
    dim_accounts.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD_DB}.dim_accounts")
)

print(f"{GOLD_DB}.dim_accounts: {dim_accounts.count()} rows")


# dim_category
# Business logic encoded here - category_group and category_type do not exist
# in the source system. This is a deliberate modelling decision: the warehouse
# layer is where raw operational categories get mapped to analytical groupings.
# Any changes to this mapping are version-controlled here, not buried in SQL.

category_data = [
    ("Salary",        "Income",        "Inflow"),
    ("Investment",    "Income",        "Inflow"),
    ("Transfer",      "Income",        "Inflow"),
    ("Rent",          "Essential",     "Outflow"),
    ("Groceries",     "Essential",     "Outflow"),
    ("Utilities",     "Essential",     "Outflow"),
    ("Healthcare",    "Essential",     "Outflow"),
    ("Travel",        "Discretionary", "Outflow"),
    ("Entertainment", "Discretionary", "Outflow"),
    ("Miscellaneous", "Discretionary", "Outflow"),
]

dim_category = (
    spark.createDataFrame(category_data, ["category_name", "category_group", "category_type"])
    .withColumn("category_key", F.sha2(F.lower(F.col("category_name")), 256))
    .select("category_key", "category_name", "category_group", "category_type")
)

(
    dim_category.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD_DB}.dim_category")
)

print(f"{GOLD_DB}.dim_category: {dim_category.count()} rows")


# fct_transactions
# Natural keys replaced with SHA2 surrogate keys for consistency and idempotency.
# Foreign key validation runs before write - unresolved joins produce silent
# NULL values in dashboards which are harder to detect than a pipeline failure.

silver_txn = spark.table(f"{SILVER_DB}.transactions")

print(f"Silver rows loaded: {silver_txn.count():,}")

fct_transactions = (
    silver_txn
    .withColumn("transaction_key", F.sha2(F.col("transaction_id").cast("string"), 256))
    .withColumn("account_key",     F.sha2(F.col("account_id").cast("string"), 256))
    .withColumn("date_key",        F.date_format(F.col("transaction_date"), "yyyyMMdd"))
    .withColumn("category_key",    F.sha2(F.lower(F.col("category")), 256))
    .select(
        "transaction_key",
        "account_key",
        "date_key",
        "category_key",
        "amount",
        "transaction_type",
        "status",
        "_amount_corrected",
        "_date_ambiguous",
        "_ingested_at",
    )
)

dim_account_keys  = spark.table(f"{GOLD_DB}.dim_accounts").select("account_key")
dim_date_keys     = spark.table(f"{GOLD_DB}.dim_date").select("date_key")
dim_category_keys = spark.table(f"{GOLD_DB}.dim_category").select("category_key")

orphaned_accounts   = fct_transactions.join(dim_account_keys,  "account_key",  "left_anti").count()
orphaned_dates      = fct_transactions.join(dim_date_keys,     "date_key",     "left_anti").count()
orphaned_categories = fct_transactions.join(dim_category_keys, "category_key", "left_anti").count()

print(f"Orphaned account_keys  : {orphaned_accounts:,}")
print(f"Orphaned date_keys     : {orphaned_dates:,}")
print(f"Orphaned category_keys : {orphaned_categories:,}")

if orphaned_accounts + orphaned_dates + orphaned_categories > 0:
    raise ValueError(
        "Foreign key violations detected - fact table references dimension rows that do not exist. "
        "Joins will produce silent NULLs in dashboards. Aborting write."
    )

(
    fct_transactions.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD_DB}.fct_transactions")
)

print(f"{GOLD_DB}.fct_transactions: {spark.table(f'{GOLD_DB}.fct_transactions').count():,} rows")
