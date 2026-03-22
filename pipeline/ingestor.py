"""
Ingestion layer — reads shareholding CSV/JSON and loads into SQLite.
Handles deduplication, type coercion, and ingestion logging.
"""

import sqlite3
import csv
import json
import os
from datetime import datetime


DB_PATH = os.environ.get("DB_PATH", "db/equity_qa.db")
SCHEMA_PATH = "db/schema.sql"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    conn.execute("""CREATE TABLE IF NOT EXISTS shareholding_raw (
        record_id TEXT PRIMARY KEY, company_ticker TEXT, company_name TEXT,
        isin TEXT, shareholder_name TEXT, ownership_pct REAL, shares_held INTEGER,
        filing_type TEXT, filing_date TEXT, source TEXT, currency TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS defect_log (
        defect_id INTEGER PRIMARY KEY AUTOINCREMENT,
        record_id TEXT, company_ticker TEXT, rule_id TEXT, rule_name TEXT,
        severity TEXT, description TEXT, field_affected TEXT, field_value TEXT,
        detected_at TEXT DEFAULT (datetime('now')),
        status TEXT DEFAULT 'Open' CHECK(status IN ('Open','Resolved','Waived')),
        resolved_at TEXT, resolution_notes TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS audit_runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT, run_at TEXT,
        records_scanned INTEGER, defects_found INTEGER,
        defect_rate_pct REAL, rules_applied INTEGER, triggered_by TEXT DEFAULT 'manual')""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_defect_ticker ON defect_log(company_ticker)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_defect_severity ON defect_log(severity)")
    conn.commit()
    conn.close()


def load_from_csv(csv_path: str) -> dict:
    conn = get_connection()
    cursor = conn.cursor()

    inserted = 0
    skipped = 0

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO shareholding_raw
                    (record_id, company_ticker, company_name, isin,
                     shareholder_name, ownership_pct, shares_held,
                     filing_type, filing_date, source, currency)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row["record_id"],
                        row["company_ticker"] or None,
                        row["company_name"],
                        row["isin"] if row["isin"] not in ("", "None") else None,
                        row["shareholder_name"] if row["shareholder_name"] not in ("", "None") else None,
                        float(row["ownership_pct"]) if row["ownership_pct"] not in ("", "None") else None,
                        int(row["shares_held"]) if row["shares_held"] not in ("", "None") else 0,
                        row["filing_type"],
                        row["filing_date"],
                        row["source"],
                        row.get("currency", "USD"),
                    ),
                )
                if cursor.rowcount:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                skipped += 1

    conn.commit()
    conn.close()

    return {
        "status": "success",
        "source": csv_path,
        "inserted": inserted,
        "skipped_duplicates": skipped,
        "ingested_at": datetime.now().isoformat(),
    }


def load_from_json(json_path: str) -> dict:
    with open(json_path) as f:
        records = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0
    skipped = 0

    for row in records:
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO shareholding_raw
                (record_id, company_ticker, company_name, isin,
                 shareholder_name, ownership_pct, shares_held,
                 filing_type, filing_date, source, currency)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["record_id"],
                    row.get("company_ticker"),
                    row.get("company_name"),
                    row.get("isin"),
                    row.get("shareholder_name"),
                    row.get("ownership_pct"),
                    row.get("shares_held", 0),
                    row.get("filing_type"),
                    row.get("filing_date"),
                    row.get("source"),
                    row.get("currency", "USD"),
                ),
            )
            if cursor.rowcount:
                inserted += 1
            else:
                skipped += 1
        except Exception:
            skipped += 1

    conn.commit()
    conn.close()

    return {
        "status": "success",
        "source": json_path,
        "inserted": inserted,
        "skipped_duplicates": skipped,
        "ingested_at": datetime.now().isoformat(),
    }


def get_record_count() -> int:
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM shareholding_raw").fetchone()[0]
    conn.close()
    return count


if __name__ == "__main__":
    init_db()
    result = load_from_csv("data/sample_shareholding.csv")
    print(f"Ingestion complete: {result}")
    print(f"Total records in DB: {get_record_count()}")
