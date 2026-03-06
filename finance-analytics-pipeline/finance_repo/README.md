# Finance Analytics Pipeline

End-to-end data engineering pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) on Databricks, with a synthetic but realistically messy finance dataset covering transactions, accounts, and budget targets across a 2-year window.

Built as a portfolio project to demonstrate production-grade patterns for ingestion, data quality enforcement, dimensional modelling, and self-serve analytics — not a tutorial walkthrough.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SOURCE LAYER                                │
│   transactions_raw.csv   accounts_raw.csv   budget_raw.csv         │
│                  (2,080 rows with 642 injected DQ issues)           │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  DBFS Upload
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       BRONZE LAYER  (finance_bronze)                │
│   Raw Delta tables — schema-on-read, no transformations             │
│   + _ingested_at timestamp   + _source_file provenance column       │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  PySpark — 01_silver_cleaning.ipynb
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       SILVER LAYER  (finance_silver)                │
│   • Standardised date formats (4 input formats → ISO)               │
│   • INITCAP normalisation on category / status / account_type       │
│   • TRIM on all string columns                                      │
│   • NULL imputation or quarantine                                   │
│   • Deduplication via ROW_NUMBER() on business keys                 │
│   • Invalid value rejection (negatives, future dates, bad statuses) │
│   • Referential integrity check against accounts                    │
│   → Clean table  +  _quarantine table for rejected rows             │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Spark SQL — 02_gold_modelling.ipynb
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER  (finance_gold)                   │
│   Star Schema:                                                      │
│   fct_transactions   dim_accounts   dim_date   dim_category         │
│                                                                     │
│   + Aggregated marts:  mart_monthly_spend   mart_budget_vs_actual   │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
                 Databricks SQL Dashboard
          (spend trends / budget variance / account segmentation)
```

---

## Data Quality Issues Handled

The raw dataset is intentionally messy to reflect real ingestion scenarios. The Silver layer resolves all of the following:

| Issue | Count | Resolution |
|---|---|---|
| Mixed date formats (`MM/DD/YYYY`, `DD-MM-YYYY`, `MM-DD-YYYY`, ISO) | ~2,080 rows | Regex-based parser → cast to `DATE` |
| NULL / blank critical fields (category, amount, date, type) | 120 | Quarantined to `_quarantine` table with rejection reason |
| Duplicate transaction IDs (exact + near-exact penny-off re-submissions) | 80 | `ROW_NUMBER()` dedup on `(account_id, transaction_date, amount, category)` |
| Invalid status codes (`nil`, `DONE`, `Compleeted`, `unknown`) | 40 | Mapped to canonical set; unresolvable → quarantine |
| Negative amounts on debit transactions | 49 | `ABS()` applied; flagged with `_amount_corrected = true` |
| Future-dated transactions (2025–2026 in a 2023–2024 dataset) | 30 | Rejected to quarantine |
| Orphaned account_ids (no matching account record) | 25 | Rejected to quarantine after left-anti join |
| Casing inconsistency across categorical columns | 298 | `INITCAP()` + lookup normalisation |

---

## Dimensional Model (Gold Layer)

```
                        ┌──────────────┐
                        │  dim_date    │
                        │  date_key PK │
                        └──────┬───────┘
                               │
┌──────────────┐    ┌──────────┴──────────┐    ┌───────────────┐
│ dim_accounts │    │   fct_transactions  │    │ dim_category  │
│ account_key  ├────┤   transaction_key   ├────┤ category_key  │
│ PK           │    │   account_key  FK   │    │ PK            │
└──────────────┘    │   date_key     FK   │    └───────────────┘
                    │   category_key FK   │
                    │   amount            │
                    │   transaction_type  │
                    │   status            │
                    └─────────────────────┘
```

Surrogate keys generated via `SHA2` hash on natural keys — deterministic, idempotent, no sequence dependency.

---

## Repo Structure

```
finance-analytics-pipeline/
│
├── data/
│   └── raw/                         # .gitignored — use DBFS or object storage in practice
│       └── generate_messy_data.py   # Reproducible data generation with injected DQ issues
│
├── notebooks/
│   ├── 00_bronze_ingestion.ipynb    # CSV → Delta, schema inference, provenance columns
│   ├── 01_silver_cleaning.ipynb     # All DQ logic — cleaning, dedup, quarantine
│   ├── 02_gold_modelling.ipynb      # Dimensional model + aggregated marts
│   └── 03_data_quality_report.ipynb # Row counts, quarantine stats, DQ metrics
│
├── sql/
│   ├── bronze/
│   │   └── ddl_bronze_tables.sql
│   ├── silver/
│   │   ├── stg_transactions.sql     # Standalone SQL mirrors of notebook logic
│   │   ├── stg_accounts.sql
│   │   └── stg_budget.sql
│   └── gold/
│       ├── fct_transactions.sql
│       ├── dim_accounts.sql
│       ├── dim_date.sql
│       ├── dim_category.sql
│       └── mart_budget_vs_actual.sql
│
├── dashboard/
│   └── screenshots/                 # Dashboard exports for portfolio visibility
│
├── docs/
│   ├── data_dictionary.md           # Column definitions, business rules, grain
│   └── dq_rules.md                  # Formal DQ rule catalogue
│
├── .github/
│   └── workflows/
│       └── ci_notebook_lint.yml     # nbformat validation on PR
│
├── .gitignore
└── README.md
```

---

## Stack

| Component | Technology |
|---|---|
| Platform | Databricks Community Edition |
| Storage format | Delta Lake |
| Processing | PySpark + Spark SQL |
| Orchestration | Databricks Workflows (job clusters) |
| Dashboard | Databricks SQL |
| CI | GitHub Actions |
| Language | Python 3.10 / SQL |

---

## Running the Pipeline

### Prerequisites
- Databricks workspace (Community Edition sufficient)
- Cluster runtime: DBR 13.x LTS or above

### Steps

```bash
# 1. Upload raw CSVs to DBFS
dbutils.fs.cp("file:/path/to/transactions_raw.csv", "dbfs:/FileStore/raw_finance/")
dbutils.fs.cp("file:/path/to/accounts_raw.csv",     "dbfs:/FileStore/raw_finance/")
dbutils.fs.cp("file:/path/to/budget_raw.csv",       "dbfs:/FileStore/raw_finance/")

# 2. Run notebooks in order
#    00_bronze_ingestion → 01_silver_cleaning → 02_gold_modelling → 03_data_quality_report
```

Or trigger as a Databricks Workflow job — see `docs/workflow_setup.md`.

### Validate

```sql
-- Quick sanity check after each layer
SELECT 'bronze' AS layer, COUNT(*) AS rows FROM finance_bronze.transactions
UNION ALL
SELECT 'silver_clean',     COUNT(*)         FROM finance_silver.transactions
UNION ALL
SELECT 'silver_quarantine',COUNT(*)         FROM finance_silver.transactions_quarantine
UNION ALL
SELECT 'gold_fact',        COUNT(*)         FROM finance_gold.fct_transactions;
```

Expected output after full run:

| layer | rows |
|---|---|
| bronze | 2,080 |
| silver_clean | ~1,890 |
| silver_quarantine | ~190 |
| gold_fact | ~1,890 |

---

## Key Design Decisions

**Why Delta Lake over Parquet directly?** ACID transactions matter here — the deduplication step does a merge, not an overwrite. Delta's `MERGE INTO` handles this without risking partial writes that would corrupt the Silver layer.

**Why quarantine instead of drop?** Dropped rows are invisible. A quarantine table with a `_rejection_reason` column gives the data team an audit trail and lets upstream teams fix source issues systematically rather than guessing what got lost.

**Why SHA2 surrogate keys instead of auto-increment?** Deterministic keys mean the Gold layer is fully idempotent — rerunning the pipeline on the same data produces identical keys, which matters when downstream BI tools cache dimension members.

**Why not dbt?** For a project focused on demonstrating data engineering fundamentals — ingestion, quality enforcement, orchestration — keeping the full stack inside Databricks surfaces Spark mechanics that dbt abstracts away. dbt integration is a natural next step once the lakehouse layer is stable.

---

## Data Dictionary

See [`docs/data_dictionary.md`](docs/data_dictionary.md) for full column definitions, business rules, and table grain documentation.

---

## What's Next

- [ ] Streaming ingestion via Databricks Auto Loader (replace batch CSV load)
- [ ] SCD Type 2 on `dim_accounts` to track account type changes over time  
- [ ] Great Expectations integration for contract-based DQ assertions
- [ ] dbt semantic layer on top of Gold for reusable metric definitions
- [ ] Unity Catalog migration for column-level access control

---

## Author

**[Your Name]** · [LinkedIn](https://linkedin.com/in/yourprofile) · [Medium](https://medium.com/@yourprofile)

7 years in data engineering. This project reflects the patterns I reach for when building reliable lakehouse pipelines — opinionated on auditability, idempotency, and making data quality visible rather than silent.
