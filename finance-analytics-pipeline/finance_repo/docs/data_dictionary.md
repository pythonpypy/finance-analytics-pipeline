# Data Dictionary

## Table Grain & Lineage

| Table | Layer | Grain | Row Count (approx) |
|---|---|---|---|
| `finance_bronze.transactions` | Bronze | One row per source file row | 2,080 |
| `finance_bronze.accounts` | Bronze | One row per source file row | 10 |
| `finance_bronze.budget` | Bronze | One row per source file row | 168 |
| `finance_silver.transactions` | Silver | One row per valid, deduplicated transaction | ~1,890 |
| `finance_silver.transactions_quarantine` | Silver | One row per rejected transaction | ~190 |
| `finance_gold.fct_transactions` | Gold | One row per transaction | ~1,890 |
| `finance_gold.dim_accounts` | Gold | One row per account | 10 |
| `finance_gold.dim_date` | Gold | One row per calendar date | 730 |
| `finance_gold.dim_category` | Gold | One row per spend category | 10 |

---

## finance_silver.transactions

| Column | Type | Description | Business Rule |
|---|---|---|---|
| `transaction_id` | BIGINT | Source transaction identifier | Natural key from source. Not unique in bronze; deduplicated in silver. |
| `account_id` | BIGINT | FK to dim_accounts | Must exist in `finance_silver.accounts`. Orphaned rows quarantined. |
| `transaction_date` | DATE | Date of transaction | Must be ≤ CURRENT_DATE(). Future dates quarantined. |
| `category` | STRING | Spend or income category | Normalised to INITCAP. One of: Salary, Rent, Groceries, Utilities, Travel, Healthcare, Entertainment, Transfer, Investment, Miscellaneous. |
| `amount` | DOUBLE | Transaction amount in USD | Must be > 0. Negative values corrected via ABS() and flagged. NULLs quarantined. |
| `transaction_type` | STRING | `credit` or `debit` | Normalised to lowercase. NULLs quarantined. |
| `description` | STRING | Free-text description | TRIM applied. No validation beyond null-safe coalescing. |
| `status` | STRING | Processing status | Canonical values: `completed`, `pending`, `failed`. Unresolvable values quarantined. |
| `_ingested_at` | TIMESTAMP | When the row landed in Bronze | Set at ingestion, propagated through layers. |
| `_source_file` | STRING | Source filename | Provenance — useful for debugging upstream feed issues. |
| `_amount_corrected` | BOOLEAN | True if ABS() was applied to a negative amount | Audit flag. |
| `_dedup_rank` | INTEGER | Row number within dedup window (kept = 1) | Dedup window: `(account_id, transaction_date, amount, category)` ordered by `_ingested_at DESC`. |

---

## finance_silver.transactions_quarantine

All columns from `finance_silver.transactions` plus:

| Column | Type | Description |
|---|---|---|
| `_rejection_reason` | STRING | Human-readable rejection cause (e.g. `NULL_AMOUNT`, `FUTURE_DATE`, `ORPHANED_ACCOUNT_ID`, `INVALID_STATUS`) |
| `_quarantined_at` | TIMESTAMP | When the row was written to quarantine |

---

## finance_gold.fct_transactions

| Column | Type | Description |
|---|---|---|
| `transaction_key` | STRING | SHA2 surrogate key on `transaction_id` |
| `account_key` | STRING | FK → dim_accounts.account_key |
| `date_key` | STRING | FK → dim_date.date_key (format: YYYYMMDD) |
| `category_key` | STRING | FK → dim_category.category_key |
| `amount` | DOUBLE | Cleaned transaction amount (USD) |
| `transaction_type` | STRING | `credit` / `debit` |
| `status` | STRING | `completed` / `pending` / `failed` |

---

## finance_gold.dim_date

Spine covers 2023-01-01 → 2024-12-31. Generated programmatically — no dependency on transaction data.

| Column | Type | Description |
|---|---|---|
| `date_key` | STRING | YYYYMMDD format — join key |
| `full_date` | DATE | Calendar date |
| `day_of_week` | STRING | Monday–Sunday |
| `day_of_month` | INT | 1–31 |
| `week_of_year` | INT | ISO week number |
| `month_num` | INT | 1–12 |
| `month_name` | STRING | January–December |
| `quarter` | INT | 1–4 |
| `year` | INT | Calendar year |
| `is_weekend` | BOOLEAN | True for Saturday/Sunday |
| `is_month_end` | BOOLEAN | True for last day of month |
