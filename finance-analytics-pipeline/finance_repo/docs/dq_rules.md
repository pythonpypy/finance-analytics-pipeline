# Data Quality Rules Catalogue

Formal specification of every DQ rule applied in the Silver layer. Rules are implemented in `notebooks/01_silver_cleaning.ipynb`.

Each rule has an ID, scope, action (correct vs quarantine), and the column written to `_rejection_reason` in the quarantine table.

---

## Rule Registry

| Rule ID | Table | Column(s) | Condition | Action | Rejection Code |
|---|---|---|---|---|---|
| DQ-001 | transactions | `transaction_date` | NULL or empty string | Quarantine | `NULL_DATE` |
| DQ-002 | transactions | `transaction_date` | Parsed date > CURRENT_DATE() | Quarantine | `FUTURE_DATE` |
| DQ-003 | transactions | `transaction_date` | Mixed format (non-ISO) | Correct — normalise to ISO | — |
| DQ-004 | transactions | `amount` | NULL or empty string | Quarantine | `NULL_AMOUNT` |
| DQ-005 | transactions | `amount` | Numeric value < 0 | Correct — apply ABS(), set `_amount_corrected = true` | — |
| DQ-006 | transactions | `category` | NULL or empty string | Quarantine | `NULL_CATEGORY` |
| DQ-007 | transactions | `category` | Non-canonical casing | Correct — INITCAP() | — |
| DQ-008 | transactions | `transaction_type` | NULL or empty string | Quarantine | `NULL_TRANSACTION_TYPE` |
| DQ-009 | transactions | `transaction_type` | Non-canonical casing | Correct — lowercase() | — |
| DQ-010 | transactions | `status` | Not in `{completed, pending, failed}` (case-insensitive) | Attempt normalisation; if unresolvable → quarantine | `INVALID_STATUS` |
| DQ-011 | transactions | `account_id` | No matching row in `finance_silver.accounts` | Quarantine | `ORPHANED_ACCOUNT_ID` |
| DQ-012 | transactions | `transaction_id` | Duplicate within window `(account_id, transaction_date, amount, category)` | Keep latest by `_ingested_at`, discard rest | — |
| DQ-013 | transactions | `description` | Leading/trailing whitespace | Correct — TRIM() | — |
| DQ-014 | accounts | `region` | Non-canonical casing | Correct — INITCAP() | — |
| DQ-015 | accounts | `customer_name` | Leading/trailing whitespace | Correct — TRIM() | — |
| DQ-016 | accounts | `account_type` | Non-canonical casing | Correct — INITCAP() | — |
| DQ-017 | accounts | `created_date` | Mixed format (non-ISO) | Correct — normalise to ISO | — |
| DQ-018 | budget | `category` | Non-canonical casing | Correct — INITCAP() | — |

---

## Status Normalisation Map (DQ-010)

The following mappings are applied before the quarantine decision:

| Raw Value | Normalised |
|---|---|
| `COMPLETED`, `complete`, `Completed`, `DONE` | `completed` |
| `PENDING`, `Pending` | `pending` |
| `FAILED`, `Failed` | `failed` |
| `nil`, `NULL`, `unknown`, `3`, `yes` | → quarantine (`INVALID_STATUS`) |

---

## DQ Metrics Tracked (03_data_quality_report.ipynb)

After each pipeline run, the following metrics are written to `finance_silver.dq_run_metrics`:

- `run_timestamp`
- `bronze_row_count`
- `silver_clean_row_count`
- `silver_quarantine_row_count`
- `quarantine_rate_pct`
- `corrections_applied_count`
- `rules_triggered` (JSON map of rule_id → count)
