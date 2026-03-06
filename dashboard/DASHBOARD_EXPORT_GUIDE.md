# Databricks Dashboard — GitHub Export Guide

Databricks Lakeview Dashboards cannot be pushed to GitHub directly as a live
interactive file. Here is how to represent your dashboard properly in the repo.

---

## What to Export

### 1. Dashboard JSON (Source of Truth)

Databricks lets you export a dashboard as a JSON file that can be re-imported
into any Databricks workspace.

Steps:
1. Open your dashboard
2. Click the three-dot menu (top right) → "Export"
3. Save the .json file
4. Add it to this folder: `dashboard/finance_analytics_dashboard.json`

This file is importable — anyone cloning the repo can recreate the exact
dashboard in their own workspace by importing this JSON.

---

### 2. Screenshots (For README and Portfolio Visibility)

Recruiters and hiring managers will not spin up Databricks to view your
dashboard. Screenshots in the README are what they actually see.

Export one screenshot per visual and save to `dashboard/screenshots/`:

```
dashboard/
  screenshots/
    01_monthly_spend_trend.png
    02_spend_by_category.png
    03_spend_by_region.png
    04_income_vs_outflow.png
    05_transaction_status.png
    06_full_dashboard.png        ← most important — full overview
```

How to take good screenshots:
- Use full screen mode in Databricks (F11)
- Make sure data is loaded before screenshotting
- Include the chart title in the frame
- `06_full_dashboard.png` should show the complete dashboard in one view

---

### 3. SQL Queries (Version Controlled)

Save each dashboard query as a `.sql` file so the logic is readable in GitHub
without needing Databricks access.

```
dashboard/
  queries/
    01_monthly_spend_trend.sql
    02_spend_by_category.sql
    03_spend_by_region.sql
    04_income_vs_outflow.sql
    05_transaction_status.sql
    06_counters.sql
```

---

## Folder Structure After Export

```
dashboard/
├── finance_analytics_dashboard.json   ← importable into Databricks
├── screenshots/
│   ├── 01_monthly_spend_trend.png
│   ├── 02_spend_by_category.png
│   ├── 03_spend_by_region.png
│   ├── 04_income_vs_outflow.png
│   ├── 05_transaction_status.png
│   └── 06_full_dashboard.png
└── queries/
    ├── 01_monthly_spend_trend.sql
    ├── 02_spend_by_category.sql
    ├── 03_spend_by_region.sql
    ├── 04_income_vs_outflow.sql
    ├── 05_transaction_status.sql
    └── 06_counters.sql
```

---

## How to Re-import the Dashboard

Anyone cloning this repo can recreate the dashboard:

1. Open Databricks workspace
2. Click Dashboards → Import
3. Upload `finance_analytics_dashboard.json`
4. Update the catalog/schema references if using a different workspace
5. Connect to a SQL warehouse and run
