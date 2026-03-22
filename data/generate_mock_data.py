"""
Generates synthetic shareholding data mimicking S&P Global public ownership filings.
Injects realistic errors across ~18% of records for QA pipeline testing.
"""

import random
import csv
import json
from datetime import datetime, timedelta

random.seed(42)

COMPANIES = [
    ("AAPL", "Apple Inc.", "US0378331005"),
    ("MSFT", "Microsoft Corp.", "US5949181045"),
    ("GOOGL", "Alphabet Inc.", "US02079K3059"),
    ("AMZN", "Amazon.com Inc.", "US0231351067"),
    ("TSLA", "Tesla Inc.", "US88160R1014"),
    ("JPM", "JPMorgan Chase", "US46625H1005"),
    ("BRK", "Berkshire Hathaway", "US0846701086"),
    ("JNJ", "Johnson & Johnson", "US4781601046"),
    ("V", "Visa Inc.", "US92826C8394"),
    ("NVDA", "NVIDIA Corp.", "US67066G1040"),
    ("META", "Meta Platforms", "US30303M1027"),
    ("UNH", "UnitedHealth Group", "US91324P1021"),
    ("XOM", "Exxon Mobil", "US30231G1022"),
    ("WMT", "Walmart Inc.", "US9311421039"),
    ("PG", "Procter & Gamble", "US7427181091"),
]

SHAREHOLDERS = [
    "BlackRock Inc.", "Vanguard Group", "State Street Corp.",
    "Fidelity Investments", "Capital Group", "T. Rowe Price",
    "Wellington Management", "Geode Capital Management",
    "Northern Trust Corp.", "Invesco Ltd.", "Morgan Stanley",
    "Goldman Sachs Group", "JPMorgan Asset Mgmt", "UBS Group AG",
    "Bank of America Corp.", "Dimensional Fund Advisors",
    "Legal & General Group", "Norges Bank Investment",
    "Franklin Templeton", "Amundi Asset Management",
]

FILING_TYPES = ["Annual Report", "Exchange Notification", "13F Filing", "Quarterly Report"]
SOURCES = ["SEC EDGAR", "LSE Filing", "SEBI Submission", "TSX Filing", "ASX Notification"]


def random_date(start_days_ago=730):
    base = datetime.today()
    offset = random.randint(0, start_days_ago)
    return (base - timedelta(days=offset)).strftime("%Y-%m-%d")


def generate_records(n=1000):
    records = []
    record_id = 1

    for company_ticker, company_name, isin in COMPANIES:
        shareholders_for_company = random.sample(SHAREHOLDERS, random.randint(8, 15))
        total_pct = 100.0
        shares = []

        for i, shareholder in enumerate(shareholders_for_company):
            if i == len(shareholders_for_company) - 1:
                pct = round(total_pct, 4)
            else:
                pct = round(random.uniform(1.5, total_pct / (len(shareholders_for_company) - i)), 4)
                total_pct -= pct
            shares.append((shareholder, pct))

        for shareholder, ownership_pct in shares:
            record = {
                "record_id": f"REC{record_id:05d}",
                "company_ticker": company_ticker,
                "company_name": company_name,
                "isin": isin,
                "shareholder_name": shareholder,
                "ownership_pct": ownership_pct,
                "shares_held": int(ownership_pct / 100 * random.randint(1_000_000_000, 5_000_000_000)),
                "filing_type": random.choice(FILING_TYPES),
                "filing_date": random_date(),
                "source": random.choice(SOURCES),
                "currency": "USD",
                "has_error": False,
                "error_type": None,
            }
            records.append(record)
            record_id += 1

    # Pad to n records with extra entries
    while len(records) < n:
        company_ticker, company_name, isin = random.choice(COMPANIES)
        shareholder = random.choice(SHAREHOLDERS)
        record = {
            "record_id": f"REC{record_id:05d}",
            "company_ticker": company_ticker,
            "company_name": company_name,
            "isin": isin,
            "shareholder_name": shareholder,
            "ownership_pct": round(random.uniform(0.5, 8.0), 4),
            "shares_held": int(random.uniform(0.5, 8.0) / 100 * random.randint(1_000_000_000, 5_000_000_000)),
            "filing_type": random.choice(FILING_TYPES),
            "filing_date": random_date(),
            "source": random.choice(SOURCES),
            "currency": "USD",
            "has_error": False,
            "error_type": None,
        }
        records.append(record)
        record_id += 1

    # --- Inject errors into ~18% of records ---
    error_indices = random.sample(range(len(records)), int(len(records) * 0.18))

    error_types = [
        "NULL_ISIN",
        "OWNERSHIP_EXCEEDS_100",
        "DUPLICATE_FILING",
        "STALE_DATE",
        "NEGATIVE_OWNERSHIP",
        "MISSING_SHAREHOLDER",
        "INVALID_TICKER",
        "ZERO_SHARES",
    ]

    weights = [0.20, 0.15, 0.18, 0.12, 0.10, 0.10, 0.08, 0.07]

    for idx in error_indices:
        error_type = random.choices(error_types, weights=weights)[0]
        records[idx]["has_error"] = True
        records[idx]["error_type"] = error_type

        if error_type == "NULL_ISIN":
            records[idx]["isin"] = None
        elif error_type == "OWNERSHIP_EXCEEDS_100":
            records[idx]["ownership_pct"] = round(random.uniform(101, 150), 4)
        elif error_type == "DUPLICATE_FILING":
            if idx > 0:
                records[idx]["record_id"] = records[idx - 1]["record_id"]
        elif error_type == "STALE_DATE":
            records[idx]["filing_date"] = "1995-01-01"
        elif error_type == "NEGATIVE_OWNERSHIP":
            records[idx]["ownership_pct"] = round(random.uniform(-50, -0.1), 4)
        elif error_type == "MISSING_SHAREHOLDER":
            records[idx]["shareholder_name"] = None
        elif error_type == "INVALID_TICKER":
            records[idx]["company_ticker"] = "?????"
        elif error_type == "ZERO_SHARES":
            records[idx]["shares_held"] = 0

    return records


def save_csv(records, path="data/sample_shareholding.csv"):
    fieldnames = list(records[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"Saved {len(records)} records to {path}")


def save_json(records, path="data/sample_shareholding.json"):
    with open(path, "w") as f:
        json.dump(records, f, indent=2)
    print(f"Saved JSON to {path}")


if __name__ == "__main__":
    records = generate_records(1000)
    save_csv(records)
    save_json(records)

    error_count = sum(1 for r in records if r["has_error"])
    print(f"\nSummary:")
    print(f"  Total records : {len(records)}")
    print(f"  Error records : {error_count} ({error_count/len(records)*100:.1f}%)")
    print(f"  Clean records : {len(records) - error_count}")
