"""
QA Engine — runs 12 business validation rules against shareholding_raw.
Classifies defects by severity and writes to defect_log table.
Mirrors real-world quality assurance workflows in investment data operations.
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "db/equity_qa.db")

VALID_TICKERS = {
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
    "JPM", "BRK", "JNJ", "V", "NVDA",
    "META", "UNH", "XOM", "WMT", "PG",
}

VALID_FILING_TYPES = {
    "Annual Report", "Exchange Notification", "13F Filing", "Quarterly Report"
}

RULES = [
    {
        "rule_id": "QR-01",
        "rule_name": "Null or missing ISIN",
        "severity": "Critical",
        "field": "isin",
        "description": "ISIN is required for all shareholding records. Missing ISIN prevents mapping to global security master.",
    },
    {
        "rule_id": "QR-02",
        "rule_name": "Ownership percentage exceeds 100%",
        "severity": "Critical",
        "field": "ownership_pct",
        "description": "A single shareholder cannot own more than 100% of a company.",
    },
    {
        "rule_id": "QR-03",
        "rule_name": "Negative ownership percentage",
        "severity": "Critical",
        "field": "ownership_pct",
        "description": "Ownership percentage cannot be negative. Likely a sign inversion error in source filing.",
    },
    {
        "rule_id": "QR-04",
        "rule_name": "Missing shareholder name",
        "severity": "High",
        "field": "shareholder_name",
        "description": "Shareholder name is required for entity mapping and beneficial ownership analysis.",
    },
    {
        "rule_id": "QR-05",
        "rule_name": "Zero or negative shares held",
        "severity": "High",
        "field": "shares_held",
        "description": "Shares held must be a positive integer. Zero or negative values indicate a data entry error.",
    },
    {
        "rule_id": "QR-06",
        "rule_name": "Stale filing date (pre-2000)",
        "severity": "High",
        "field": "filing_date",
        "description": "Filing dates before year 2000 are considered stale and likely erroneous for current ownership data.",
    },
    {
        "rule_id": "QR-07",
        "rule_name": "Duplicate record ID",
        "severity": "High",
        "field": "record_id",
        "description": "Record IDs must be unique. Duplicates cause double-counting in ownership aggregations.",
    },
    {
        "rule_id": "QR-08",
        "rule_name": "Invalid company ticker",
        "severity": "Medium",
        "field": "company_ticker",
        "description": "Ticker symbol not found in known company master list. May indicate a new company requiring onboarding.",
    },
    {
        "rule_id": "QR-09",
        "rule_name": "ISIN format violation",
        "severity": "Medium",
        "field": "isin",
        "description": "ISIN must be exactly 12 alphanumeric characters per ISO 6166 standard.",
    },
    {
        "rule_id": "QR-10",
        "rule_name": "Future filing date",
        "severity": "Medium",
        "field": "filing_date",
        "description": "Filing date is in the future, which is not valid for historical ownership records.",
    },
    {
        "rule_id": "QR-11",
        "rule_name": "Company total ownership exceeds 105%",
        "severity": "Medium",
        "field": "ownership_pct",
        "description": "Aggregate ownership across all shareholders for a company exceeds 105%. Indicates duplicate or inflated records.",
    },
    {
        "rule_id": "QR-12",
        "rule_name": "Invalid filing type",
        "severity": "Medium",
        "field": "filing_type",
        "description": "Filing type not in approved taxonomy. May affect source credibility scoring.",
    },
]


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def clear_defects():
    conn = get_connection()
    conn.execute("DELETE FROM defect_log")
    conn.commit()
    conn.close()


def _insert_defects(conn, defects: list):
    conn.executemany(
        """
        INSERT INTO defect_log
          (record_id, company_ticker, rule_id, rule_name, severity,
           description, field_affected, field_value)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        defects,
    )


def run_qr01(conn) -> list:
    rows = conn.execute(
        "SELECT record_id, company_ticker, isin FROM shareholding_raw WHERE isin IS NULL OR isin = ''"
    ).fetchall()
    rule = next(r for r in RULES if r["rule_id"] == "QR-01")
    return [
        (r["record_id"], r["company_ticker"], rule["rule_id"], rule["rule_name"],
         rule["severity"], rule["description"], rule["field"], str(r["isin"]))
        for r in rows
    ]


def run_qr02(conn) -> list:
    rows = conn.execute(
        "SELECT record_id, company_ticker, ownership_pct FROM shareholding_raw WHERE ownership_pct > 100"
    ).fetchall()
    rule = next(r for r in RULES if r["rule_id"] == "QR-02")
    return [
        (r["record_id"], r["company_ticker"], rule["rule_id"], rule["rule_name"],
         rule["severity"], rule["description"], rule["field"], str(r["ownership_pct"]))
        for r in rows
    ]


def run_qr03(conn) -> list:
    rows = conn.execute(
        "SELECT record_id, company_ticker, ownership_pct FROM shareholding_raw WHERE ownership_pct < 0"
    ).fetchall()
    rule = next(r for r in RULES if r["rule_id"] == "QR-03")
    return [
        (r["record_id"], r["company_ticker"], rule["rule_id"], rule["rule_name"],
         rule["severity"], rule["description"], rule["field"], str(r["ownership_pct"]))
        for r in rows
    ]


def run_qr04(conn) -> list:
    rows = conn.execute(
        "SELECT record_id, company_ticker FROM shareholding_raw WHERE shareholder_name IS NULL OR shareholder_name = ''"
    ).fetchall()
    rule = next(r for r in RULES if r["rule_id"] == "QR-04")
    return [
        (r["record_id"], r["company_ticker"], rule["rule_id"], rule["rule_name"],
         rule["severity"], rule["description"], rule["field"], "NULL")
        for r in rows
    ]


def run_qr05(conn) -> list:
    rows = conn.execute(
        "SELECT record_id, company_ticker, shares_held FROM shareholding_raw WHERE shares_held <= 0"
    ).fetchall()
    rule = next(r for r in RULES if r["rule_id"] == "QR-05")
    return [
        (r["record_id"], r["company_ticker"], rule["rule_id"], rule["rule_name"],
         rule["severity"], rule["description"], rule["field"], str(r["shares_held"]))
        for r in rows
    ]


def run_qr06(conn) -> list:
    rows = conn.execute(
        "SELECT record_id, company_ticker, filing_date FROM shareholding_raw WHERE filing_date < '2000-01-01'"
    ).fetchall()
    rule = next(r for r in RULES if r["rule_id"] == "QR-06")
    return [
        (r["record_id"], r["company_ticker"], rule["rule_id"], rule["rule_name"],
         rule["severity"], rule["description"], rule["field"], str(r["filing_date"]))
        for r in rows
    ]


def run_qr07(conn) -> list:
    rows = conn.execute(
        "SELECT record_id FROM shareholding_raw GROUP BY record_id HAVING COUNT(*) > 1"
    ).fetchall()
    rule = next(r for r in RULES if r["rule_id"] == "QR-07")
    defects = []
    for r in rows:
        dupes = conn.execute(
            "SELECT record_id, company_ticker FROM shareholding_raw WHERE record_id = ?",
            (r["record_id"],)
        ).fetchall()
        for d in dupes:
            defects.append(
                (d["record_id"], d["company_ticker"], rule["rule_id"], rule["rule_name"],
                 rule["severity"], rule["description"], rule["field"], d["record_id"])
            )
    return defects


def run_qr08(conn) -> list:
    rows = conn.execute("SELECT DISTINCT record_id, company_ticker FROM shareholding_raw").fetchall()
    rule = next(r for r in RULES if r["rule_id"] == "QR-08")
    return [
        (r["record_id"], r["company_ticker"], rule["rule_id"], rule["rule_name"],
         rule["severity"], rule["description"], rule["field"], str(r["company_ticker"]))
        for r in rows if r["company_ticker"] not in VALID_TICKERS
    ]


def run_qr09(conn) -> list:
    rows = conn.execute(
        "SELECT record_id, company_ticker, isin FROM shareholding_raw WHERE isin IS NOT NULL AND isin != ''"
    ).fetchall()
    rule = next(r for r in RULES if r["rule_id"] == "QR-09")
    defects = []
    for r in rows:
        isin = r["isin"]
        if len(isin) != 12 or not isin.isalnum():
            defects.append(
                (r["record_id"], r["company_ticker"], rule["rule_id"], rule["rule_name"],
                 rule["severity"], rule["description"], rule["field"], isin)
            )
    return defects


def run_qr10(conn) -> list:
    rows = conn.execute(
        "SELECT record_id, company_ticker, filing_date FROM shareholding_raw WHERE filing_date > date('now')"
    ).fetchall()
    rule = next(r for r in RULES if r["rule_id"] == "QR-10")
    return [
        (r["record_id"], r["company_ticker"], rule["rule_id"], rule["rule_name"],
         rule["severity"], rule["description"], rule["field"], str(r["filing_date"]))
        for r in rows
    ]


def run_qr11(conn) -> list:
    rows = conn.execute(
        """
        SELECT company_ticker, SUM(ownership_pct) AS total_pct
        FROM shareholding_raw
        WHERE ownership_pct > 0
        GROUP BY company_ticker
        HAVING total_pct > 105
        """
    ).fetchall()
    rule = next(r for r in RULES if r["rule_id"] == "QR-11")
    defects = []
    for r in rows:
        # Flag a representative record for this company
        rep = conn.execute(
            "SELECT record_id FROM shareholding_raw WHERE company_ticker = ? LIMIT 1",
            (r["company_ticker"],)
        ).fetchone()
        if rep:
            defects.append(
                (rep["record_id"], r["company_ticker"], rule["rule_id"], rule["rule_name"],
                 rule["severity"], rule["description"], rule["field"], f"{r['total_pct']:.2f}%")
            )
    return defects


def run_qr12(conn) -> list:
    rows = conn.execute(
        "SELECT record_id, company_ticker, filing_type FROM shareholding_raw"
    ).fetchall()
    rule = next(r for r in RULES if r["rule_id"] == "QR-12")
    return [
        (r["record_id"], r["company_ticker"], rule["rule_id"], rule["rule_name"],
         rule["severity"], rule["description"], rule["field"], str(r["filing_type"]))
        for r in rows if r["filing_type"] not in VALID_FILING_TYPES
    ]


RULE_RUNNERS = [
    run_qr01, run_qr02, run_qr03, run_qr04, run_qr05, run_qr06,
    run_qr07, run_qr08, run_qr09, run_qr10, run_qr11, run_qr12,
]


def run_full_audit(clear_previous: bool = True) -> dict:
    conn = get_connection()

    if clear_previous:
        conn.execute("DELETE FROM defect_log")
        conn.commit()

    total_records = conn.execute("SELECT COUNT(*) FROM shareholding_raw").fetchone()[0]
    all_defects = []

    for runner in RULE_RUNNERS:
        defects = runner(conn)
        all_defects.extend(defects)

    _insert_defects(conn, all_defects)

    run_at = datetime.now().isoformat()
    defect_count = len(all_defects)
    defect_rate = round(defect_count / total_records * 100, 2) if total_records else 0

    conn.execute(
        """
        INSERT INTO audit_runs (run_at, records_scanned, defects_found, defect_rate_pct, rules_applied)
        VALUES (?,?,?,?,?)
        """,
        (run_at, total_records, defect_count, defect_rate, len(RULE_RUNNERS)),
    )
    conn.commit()

    # Summary by severity
    severity_counts = {}
    for defect in all_defects:
        sev = defect[4]
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

    # Summary by rule
    rule_counts = {}
    for defect in all_defects:
        rid = defect[2]
        rule_counts[rid] = rule_counts.get(rid, 0) + 1

    conn.close()

    return {
        "run_at": run_at,
        "records_scanned": total_records,
        "defects_found": defect_count,
        "defect_rate_pct": defect_rate,
        "rules_applied": len(RULE_RUNNERS),
        "by_severity": severity_counts,
        "by_rule": rule_counts,
    }


if __name__ == "__main__":
    result = run_full_audit()
    print("\n=== Audit Complete ===")
    for k, v in result.items():
        print(f"  {k}: {v}")
