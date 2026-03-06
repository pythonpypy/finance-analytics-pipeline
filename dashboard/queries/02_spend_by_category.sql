-- Spend by Category
SELECT
    c.category_group,
    c.category_name,
    d.year,
    ROUND(SUM(f.amount), 2)  AS total_spend,
    COUNT(*)                  AS transaction_count,
    ROUND(AVG(f.amount), 2)  AS avg_transaction
FROM workspace.gold.fct_transactions   f
JOIN workspace.gold.dim_date           d ON f.date_key     = d.date_key
JOIN workspace.gold.dim_category       c ON f.category_key = c.category_key
WHERE f.transaction_type = 'debit'
GROUP BY 1, 2, 3
ORDER BY d.year, total_spend DESC
