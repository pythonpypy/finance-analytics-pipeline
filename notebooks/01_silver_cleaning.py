# Databricks notebook source
# Silver Layer — Cleaning, Standardisation, Validation
#
# Applies all data quality transformations to Bronze transactions.
# Design decisions:
#   - Quarantine over delete: rejected rows are preserved with a rejection reason
#     so upstream issues are visible and fixable rather than silently lost.
#   - Correct only what is unambiguous: typos like "compleeted" go to quarantine,
#     not silently mapped, because intent cannot be confirmed without source team input.
#   - Fixed date range validation (2023-2024) rather than current_date() to ensure
#     idempotency — pipeline behaviour must not change based on when it runs.

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.window import Window
from itertools import chain
import re

CATALOG    = "workspace"
BRONZE_DB  = f"{CATALOG}.bronze"
SILVER_DB  = f"{CATALOG}.silver"
VALID_DATE_START = "2023-01-01"
VALID_DATE_END   = "2024-12-31"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")


# Date parser handles 4 mixed formats found in source data:
#   YYYY-MM-DD, MM/DD/YYYY, MM-DD-YYYY, DD-MM-YYYY
# Ambiguity rule for dash-separated dates where both segments <= 12:
#   default to MM-DD-YYYY (US convention) and flag the row.
# Returns a struct so both the parsed date and ambiguity flag travel together.

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


# Status mapping: lowercase first to collapse casing variants, then map.
# Values not present in the map return NULL and are quarantined.
# "compleeted" and "yes" are intentionally not mapped — ambiguous intent
# in a financial status field requires source team confirmation.

status_mapping = {
    "completed":  "completed",
    "pending":    "pending",
    "failed":     "failed",
    "complete":   "completed",
    "done":       "completed",
    "compleeted": None,
    "nil":        None,
    "null":       None,
    "unknown":    None,
    "3":          None,
    "yes":        None,
}

mapping_expr = F.create_map(
    *chain.from_iterable(
        (F.lit(k), F.lit(v)) for k, v in status_mapping.items()
    )
)


bronze_txn = spark.table(f"{BRONZE_DB}.transactions")
bronze_acc = spark.table(f"{BRONZE_DB}.accounts")

print(f"Bronze rows loaded: {bronze_txn.count():,}")

cleaned = (
    bronze_txn
    .withColumn("category",          F.trim(F.col("category")))
    .withColumn("description",       F.trim(F.col("description")))
    .withColumn("transaction_type",  F.trim(F.col("transaction_type")))
    .withColumn("status",            F.trim(F.col("status")))
    .withColumn("category",          F.initcap(F.col("category")))
    .withColumn("transaction_type",  F.lower(F.col("transaction_type")))
    .withColumn("_date_parsed",      parse_date_udf(F.col("transaction_date")))
    .withColumn("transaction_date",  F.to_date(F.col("_date_parsed.parsed_date")))
    .withColumn("_date_ambiguous",   F.col("_date_parsed.date_ambiguous"))
    .drop("_date_parsed")
    .withColumn("status_lower",      F.lower(F.col("status")))
    .withColumn("status",            mapping_expr[F.col("status_lower")])
    .drop("status_lower")
    .withColumn("amount",            F.col("amount").cast("double"))
    .withColumn("_amount_corrected", F.when(F.col("amount") < 0, True).otherwise(False))
    .withColumn("amount",            F.abs(F.col("amount")))
)

dedup_window = Window.partitionBy(
    "account_id", "transaction_date", "amount", "category"
).orderBy(F.col("_ingested_at").desc())

cleaned = cleaned.withColumn("_dedup_rank", F.row_number().over(dedup_window))

valid_accounts = bronze_acc.select(
    F.col("account_id").alias("valid_account_id")
).distinct()

cleaned = (
    cleaned
    .join(valid_accounts, cleaned.account_id == valid_accounts.valid_account_id, "left")
    .withColumn("_orphaned_account", F.col("valid_account_id").isNull())
    .drop("valid_account_id")
)

quarantine_conditions = [
    (F.col("transaction_date").isNull(),                         "NULL_DATE"),
    (
        (F.col("transaction_date") > F.lit(VALID_DATE_END).cast("date")) |
        (F.col("transaction_date") < F.lit(VALID_DATE_START).cast("date")),
        "OUT_OF_RANGE_DATE"
    ),
    (F.col("category").isNull() | (F.col("category") == ""),    "NULL_CATEGORY"),
    (F.col("amount").isNull(),                                   "NULL_AMOUNT"),
    (F.col("transaction_type").isNull(),                         "NULL_TRANSACTION_TYPE"),
    (F.col("status").isNull(),                                   "INVALID_STATUS"),
    (F.col("_orphaned_account") == True,                         "ORPHANED_ACCOUNT_ID"),
    (F.col("_dedup_rank") > 1,                                   "DUPLICATE"),
]

rejection_expr = F.coalesce(*[
    F.when(condition, F.lit(reason))
    for condition, reason in quarantine_conditions
])

cleaned = cleaned.withColumn("_rejection_reason", rejection_expr)

clean_df      = cleaned.filter(F.col("_rejection_reason").isNull())
quarantine_df = cleaned.filter(F.col("_rejection_reason").isNotNull())

(
    clean_df
    .drop("_dedup_rank", "_orphaned_account", "_rejection_reason")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{SILVER_DB}.transactions")
)

(
    quarantine_df
    .withColumn("_quarantined_at", F.current_timestamp())
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{SILVER_DB}.transactions_quarantine")
)

clean_count      = spark.table(f"{SILVER_DB}.transactions").count()
quarantine_count = spark.table(f"{SILVER_DB}.transactions_quarantine").count()
total            = bronze_txn.count()

print(f"Bronze input    : {total:>10,}")
print(f"Silver clean    : {clean_count:>10,}  ({round(clean_count/total*100,1)}%)")
print(f"Quarantined     : {quarantine_count:>10,}  ({round(quarantine_count/total*100,1)}%)")

print("\nQuarantine breakdown:")
display(
    spark.table(f"{SILVER_DB}.transactions_quarantine")
    .groupBy("_rejection_reason")
    .count()
    .orderBy(F.col("count").desc())
)
