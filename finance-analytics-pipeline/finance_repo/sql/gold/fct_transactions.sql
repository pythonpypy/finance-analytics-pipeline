-- fct_transactions.sql
-- Gold layer — transactional fact table
-- Grain: one row per validated, deduplicated transaction
-- Surrogate keys: SHA2-256 on natural keys (deterministic, idempotent)

CREATE OR REPLACE TABLE finance_gold.fct_transactions
USING DELTA
AS
SELECT
    SHA2(CAST(t.transaction_id AS STRING), 256)          AS transaction_key,
    SHA2(CAST(t.account_id    AS STRING), 256)           AS account_key,
    DATE_FORMAT(t.transaction_date, 'yyyyMMdd')          AS date_key,
    SHA2(LOWER(t.category), 256)                         AS category_key,

    t.amount,
    t.transaction_type,
    t.status,

    -- Audit columns — surfaced in Gold for lineage traceability
    t._ingested_at,
    t._source_file,
    t._amount_corrected

FROM finance_silver.transactions t

-- Exclude anything that didn't resolve cleanly
WHERE t._dedup_rank = 1
;
