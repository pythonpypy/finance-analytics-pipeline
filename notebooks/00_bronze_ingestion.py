# Databricks notebook source
# Bronze Layer - Raw Ingestion
#
# Loads raw CSVs from Volumes into Delta tables with no transformations.
# Adds provenance columns so every downstream row can be traced to its source.
# Bronze is never modified after ingestion - it is the permanent audit trail.

from pyspark.sql import functions as F

VOLUME_PATH  = "/Volumes/workspace/landing/raw_finance/"
CATALOG      = "workspace"
BRONZE_DB    = f"{CATALOG}.bronze"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

sources = {
    "transactions": "transactions_raw.csv",
    "accounts":     "accounts_raw.csv",
    "budget":       "budget_raw.csv",
}

for table_name, filename in sources.items():
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{VOLUME_PATH}{filename}")
    )

    df = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(filename))
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{BRONZE_DB}.{table_name}")
    )

    print(f"{BRONZE_DB}.{table_name}: {df.count():,} rows")
