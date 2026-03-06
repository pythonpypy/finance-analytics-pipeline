-- Spend by Region
SELECT
    a.region,
    a.account_type,
    ROUND(SUM(f.amount), 2)          AS total_spend,
    COUNT(*)                          AS transaction_count,
    COUNT(DISTINCT f.account_key)     AS unique_accounts
FROM workspace.gold.fct_transactions   f
JOIN workspace.gold.dim_accounts       a ON f.account_key = a.account_key
WHERE f.transaction_type = 'debit'
GROUP BY a.region, a.account_type
ORDER BY total_spend DESC;


-- Income vs Outflow
SELECT
    d.year,
    d.month_num,
    CONCAT(d.year, '-', LPAD(d.month_num, 2, '0'))  AS year_month,
    ROUND(SUM(CASE WHEN f.transaction_type = 'credit' THEN f.amount ELSE 0 END), 2) AS total_income,
    ROUND(SUM(CASE WHEN f.transaction_type = 'debit'  THEN f.amount ELSE 0 END), 2) AS total_outflow,
    ROUND(
        SUM(CASE WHEN f.transaction_type = 'credit' THEN f.amount ELSE 0 END) -
        SUM(CASE WHEN f.transaction_type = 'debit'  THEN f.amount ELSE 0 END)
    , 2) AS net_position
FROM workspace.gold.fct_transactions   f
JOIN workspace.gold.dim_date           d ON f.date_key = d.date_key
GROUP BY d.year, d.month_num
ORDER BY d.year, d.month_num;


-- Transaction Status Breakdown
SELECT
    f.status,
    COUNT(*)                  AS transaction_count,
    ROUND(SUM(f.amount), 2)  AS total_amount
FROM workspace.gold.fct_transactions f
GROUP BY f.status
ORDER BY transaction_count DESC;


-- Counter Widgets
SELECT COUNT(*)                                                AS total_transactions
FROM workspace.gold.fct_transactions;

SELECT ROUND(SUM(amount), 2)                                   AS total_spend
FROM workspace.gold.fct_transactions
WHERE transaction_type = 'debit';

SELECT ROUND(
    (SELECT COUNT(*) FROM workspace.silver.transactions) * 100.0 /
    (SELECT COUNT(*) FROM workspace.bronze.transactions)
, 2)                                                           AS clean_data_pct;
