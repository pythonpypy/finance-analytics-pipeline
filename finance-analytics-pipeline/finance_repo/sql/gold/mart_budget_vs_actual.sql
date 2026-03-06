-- mart_budget_vs_actual.sql
-- Aggregated mart: monthly actual spend vs budget target by category
-- Consumers: Databricks SQL Dashboard, ad-hoc finance reporting

CREATE OR REPLACE TABLE finance_gold.mart_budget_vs_actual
USING DELTA
AS
WITH actuals AS (
    SELECT
        dc.category_name,
        dd.year,
        dd.month_num,
        dd.month_name,
        SUM(f.amount)   AS actual_spend
    FROM finance_gold.fct_transactions   f
    JOIN finance_gold.dim_date          dd ON f.date_key        = dd.date_key
    JOIN finance_gold.dim_category      dc ON f.category_key    = dc.category_key
    WHERE f.transaction_type = 'debit'
    GROUP BY 1, 2, 3, 4
),

budget AS (
    SELECT
        INITCAP(category)   AS category_name,
        year,
        month               AS month_num,
        budget_amount
    FROM finance_silver.budget
)

SELECT
    a.category_name,
    a.year,
    a.month_num,
    a.month_name,
    a.actual_spend,
    b.budget_amount,
    a.actual_spend - b.budget_amount                            AS variance,
    ROUND((a.actual_spend / NULLIF(b.budget_amount, 0)) * 100, 2) AS pct_of_budget

FROM actuals   a
LEFT JOIN budget b
    ON  a.category_name = b.category_name
    AND a.year          = b.year
    AND a.month_num     = b.month_num

ORDER BY a.year, a.month_num, a.category_name
;
