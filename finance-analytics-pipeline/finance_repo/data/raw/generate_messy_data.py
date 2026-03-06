import csv, random, copy
from datetime import date, timedelta

random.seed(42)

START_DATE = date(2023, 1, 1)
END_DATE   = date(2024, 12, 31)
NUM_TRANSACTIONS = 2000

ACCOUNTS = [
    (1,  "Alice Johnson",  "Checking", "North"),
    (2,  "Bob Smith",      "Savings",  "South"),
    (3,  "Carol White",    "Checking", "East"),
    (4,  "David Lee",      "Business", "West"),
    (5,  "Eva Martinez",   "Savings",  "North"),
    (6,  "Frank Thomas",   "Business", "East"),
    (7,  "Grace Kim",      "Checking", "South"),
    (8,  "Henry Brown",    "Savings",  "West"),
    (9,  "Isla Davis",     "Business", "North"),
    (10, "James Wilson",   "Checking", "East"),
]

CATEGORIES = ["Salary","Rent","Groceries","Utilities","Travel",
               "Healthcare","Entertainment","Transfer","Investment","Miscellaneous"]

CATEGORY_AMOUNTS = {
    "Salary":        (3000, 8000),
    "Rent":          (800,  2500),
    "Groceries":     (50,   400),
    "Utilities":     (60,   300),
    "Travel":        (100,  2000),
    "Healthcare":    (50,   1500),
    "Entertainment": (20,   300),
    "Transfer":      (100,  5000),
    "Investment":    (500,  10000),
    "Miscellaneous": (10,   500),
}

CREDIT_CATEGORIES = {"Salary", "Transfer", "Investment"}

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def format_date_messy(d):
    """Return date in one of several formats to simulate inconsistency"""
    fmt = random.choice(["iso", "us", "eu", "slash_us"])
    if fmt == "iso":    return str(d)                                      # 2023-04-15
    if fmt == "us":     return d.strftime("%m/%d/%Y")                     # 04/15/2023
    if fmt == "eu":     return d.strftime("%d-%m-%Y")                     # 15-04-2023
    if fmt == "slash_us": return d.strftime("%m-%d-%Y")                   # 04-15-2023

# ── 1. Generate clean base transactions ──────────────────────────────────────
clean_rows = []
for i in range(1, NUM_TRANSACTIONS + 1):
    acc  = random.choice(ACCOUNTS)
    cat  = random.choice(CATEGORIES)
    lo, hi = CATEGORY_AMOUNTS[cat]
    amount   = round(random.uniform(lo, hi), 2)
    txn_type = "credit" if cat in CREDIT_CATEGORIES else "debit"
    txn_date = random_date(START_DATE, END_DATE)
    status   = random.choices(["completed","pending","failed"], weights=[85,10,5])[0]
    clean_rows.append({
        "transaction_id":   i,
        "account_id":       acc[0],
        "transaction_date": txn_date,
        "category":         cat,
        "amount":           amount,
        "transaction_type": txn_type,
        "description":      f"{cat} - {acc[1]}",
        "status":           status,
    })

messy_rows = [copy.deepcopy(r) for r in clean_rows]
issues_log = []

# ── 2. DUPLICATES (~4% of rows, exact or near-exact) ─────────────────────────
dup_candidates = random.sample(messy_rows[:500], 80)
for r in dup_candidates:
    dup = copy.deepcopy(r)
    # near-duplicate: sometimes slightly different amount (re-submission)
    if random.random() < 0.4:
        dup["amount"] = round(r["amount"] + random.uniform(-0.01, 0.01), 2)
    messy_rows.append(dup)
    issues_log.append(f"DUPLICATE: transaction_id={r['transaction_id']}")

# ── 3. NULLs in critical fields ───────────────────────────────────────────────
null_targets = random.sample(range(len(messy_rows)), 120)
for idx in null_targets:
    field = random.choice(["category", "amount", "transaction_date", "transaction_type"])
    messy_rows[idx][field] = ""
    issues_log.append(f"NULL: row ~{idx}, field={field}")

# ── 4. INCONSISTENT FORMATS ───────────────────────────────────────────────────
# a) Mixed date formats
for r in messy_rows:
    if r["transaction_date"] != "":
        r["transaction_date"] = format_date_messy(r["transaction_date"])

# b) Inconsistent casing in category & transaction_type & status
for idx in random.sample(range(len(messy_rows)), 300):
    r = messy_rows[idx]
    choice = random.choice(["upper","lower","mixed"])
    field  = random.choice(["category","transaction_type","status"])
    if r[field] == "":
        continue
    if choice == "upper":   r[field] = r[field].upper()
    elif choice == "lower": r[field] = r[field].lower()
    else:                   r[field] = r[field].title()
    issues_log.append(f"CASING: row ~{idx}, field={field}")

# c) Whitespace padding in description
for idx in random.sample(range(len(messy_rows)), 150):
    messy_rows[idx]["description"] = "  " + messy_rows[idx]["description"] + "  "

# ── 5. INVALID VALUES ─────────────────────────────────────────────────────────
# a) Negative amounts
for idx in random.sample(range(len(messy_rows)), 50):
    if messy_rows[idx]["amount"] != "":
        messy_rows[idx]["amount"] = -abs(float(messy_rows[idx]["amount"]))
        issues_log.append(f"NEGATIVE_AMOUNT: row ~{idx}")

# b) Future dates (beyond today)
future_dates = [date(2025,3,15), date(2025,7,22), date(2026,1,10), date(2025,11,5)]
for idx in random.sample(range(len(messy_rows)), 30):
    messy_rows[idx]["transaction_date"] = str(random.choice(future_dates))
    issues_log.append(f"FUTURE_DATE: row ~{idx}")

# c) Invalid status codes
bad_statuses = ["DONE","complete","Compleeted","nil","NULL","unknown","3","yes"]
for idx in random.sample(range(len(messy_rows)), 40):
    messy_rows[idx]["status"] = random.choice(bad_statuses)
    issues_log.append(f"INVALID_STATUS: row ~{idx}")

# d) Invalid account_ids (don't exist in accounts table)
for idx in random.sample(range(len(messy_rows)), 25):
    messy_rows[idx]["account_id"] = random.choice([99, 100, 0, -1])
    issues_log.append(f"INVALID_ACCOUNT_ID: row ~{idx}")

# ── 6. Shuffle so issues are spread throughout ────────────────────────────────
random.shuffle(messy_rows)

# Re-assign sequential IDs after shuffle
for i, r in enumerate(messy_rows, 1):
    r["transaction_id"] = i

# ── Write transactions.csv ────────────────────────────────────────────────────
fields = ["transaction_id","account_id","transaction_date","category",
          "amount","transaction_type","description","status"]
with open("/home/claude/transactions_raw.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    w.writerows(messy_rows)

# ── Write accounts.csv (with some issues too) ────────────────────────────────
messy_accounts = []
for acc in ACCOUNTS:
    created = random_date(date(2020,1,1), date(2022,12,31))
    # Inconsistent region casing
    region = acc[3]
    if random.random() < 0.3: region = region.upper()
    if random.random() < 0.2: region = region.lower()
    # Whitespace in name
    name = acc[1]
    if random.random() < 0.25: name = "  " + name
    messy_accounts.append({
        "account_id":    acc[0],
        "customer_name": name,
        "account_type":  acc[2] if random.random() > 0.2 else acc[2].lower(),
        "region":        region,
        "created_date":  format_date_messy(created),
    })

with open("/home/claude/accounts_raw.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=["account_id","customer_name","account_type","region","created_date"])
    w.writeheader()
    w.writerows(messy_accounts)

# ── Write budget.csv ──────────────────────────────────────────────────────────
BUDGET_BASE = {
    "Rent":2000,"Groceries":600,"Utilities":250,"Travel":1000,
    "Healthcare":500,"Entertainment":400,"Miscellaneous":300
}
budget_rows = []
bid = 1
for yr in [2023, 2024]:
    for mo in range(1, 13):
        for cat, base in BUDGET_BASE.items():
            variance = round(random.uniform(-0.05, 0.05) * base, 2)
            budget_rows.append({
                "budget_id":     bid,
                "category":      cat if random.random() > 0.15 else cat.upper(),
                "month":         mo,
                "year":          yr,
                "budget_amount": round(base + variance, 2),
            })
            bid += 1

with open("/home/claude/budget_raw.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=["budget_id","category","month","year","budget_amount"])
    w.writeheader()
    w.writerows(budget_rows)

# ── Summary ───────────────────────────────────────────────────────────────────
import os, collections
issue_types = collections.Counter(x.split(":")[0] for x in issues_log)
print("=" * 55)
print("MESSY DATASET GENERATED")
print("=" * 55)
for fn in ["transactions_raw.csv","accounts_raw.csv","budget_raw.csv"]:
    path = f"/home/claude/{fn}"
    with open(path) as fp: rows = sum(1 for _ in fp) - 1
    print(f"  {fn}: {rows:,} rows  ({os.path.getsize(path):,} bytes)")
print()
print("Data Quality Issues Injected:")
for issue, count in sorted(issue_types.items()):
    print(f"  {issue:<22} {count:>4} occurrences")
print(f"  {'─'*30}")
print(f"  {'TOTAL':<22} {sum(issue_types.values()):>4} issues across dataset")
