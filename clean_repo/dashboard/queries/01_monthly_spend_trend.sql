-- Monthly Spend Trend
-- Powers the line chart showing total debit spend per month by category group.
-- LPAD ensures month numbers sort chronologically as strings (01, 02 not 1, 10, 11).

SELECT
    d.year,
    d.month_num,
    d.month_name,
    CONCAT(d.year, '-', LPAD(d.month_num, 2, '0')) AS year_month,
    c.category_group,
    ROUND(SUM(f.amount), 2)                         AS total_spend,
    COUNT(*)                                         AS transaction_count
FROM workspace.gold.fct_transactions   f
JOIN workspace.gold.dim_date           d ON f.date_key     = d.date_key
JOIN workspace.gold.dim_category       c ON f.category_key = c.category_key
WHERE f.transaction_type = 'debit'
  AND d.year IN (2023, 2024)
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2
