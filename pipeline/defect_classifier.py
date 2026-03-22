"""
Defect Classifier — enriches raw defects with priority scores and
detects systematic error patterns across the dataset.
Used for root cause analysis and preventive action recommendations.
"""

import sqlite3
import os
from collections import defaultdict

DB_PATH = os.environ.get("DB_PATH", "db/equity_qa.db")

SEVERITY_WEIGHT = {"Critical": 10, "High": 6, "Medium": 3}

PREVENTIVE_ACTIONS = {
    "QR-01": "Enforce ISIN validation at source filing ingestion. Cross-reference with ANNA DSB master file.",
    "QR-02": "Add pre-ingestion cap check: flag any record with ownership_pct > 100 before DB write.",
    "QR-03": "Implement sign check in parser. Negative percentages may result from incorrect absolute-to-relative conversion.",
    "QR-04": "Require shareholder_name as mandatory field in filing template. Reject blanks at API level.",
    "QR-05": "Validate shares_held > 0 at ingestor. Zero shares may indicate a corporate action not yet processed.",
    "QR-06": "Apply date range filter: reject filing_date < '2000-01-01'. Review source for data migration errors.",
    "QR-07": "Implement upsert logic with deduplication hash on (company_ticker, shareholder_name, filing_date).",
    "QR-08": "Maintain a live company ticker master list. Auto-flag new tickers for onboarding review.",
    "QR-09": "Validate ISIN against ISO 6166 checksum algorithm at ingestion.",
    "QR-10": "Add future-date guard in ingestor. Reject filing_date > today with a source notification.",
    "QR-11": "Run cross-record ownership sum check per company after each batch load. Alert if sum > 105%.",
    "QR-12": "Enforce filing_type enum at API schema level using Pydantic validators.",
}


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def compute_priority_scores() -> list[dict]:
    """Assign a priority score to each defect based on severity weight."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT defect_id, rule_id, severity, company_ticker FROM defect_log WHERE status = 'Open'"
    ).fetchall()
    conn.close()

    scored = []
    for r in rows:
        score = SEVERITY_WEIGHT.get(r["severity"], 1)
        scored.append({
            "defect_id": r["defect_id"],
            "rule_id": r["rule_id"],
            "severity": r["severity"],
            "company_ticker": r["company_ticker"],
            "priority_score": score,
        })

    return sorted(scored, key=lambda x: x["priority_score"], reverse=True)


def detect_systematic_patterns() -> list[dict]:
    """
    Identify rules that appear across multiple companies — a sign of
    systematic upstream data quality issues vs one-off anomalies.
    """
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT rule_id, rule_name, severity, COUNT(DISTINCT company_ticker) AS companies_affected,
               COUNT(*) AS total_occurrences
        FROM defect_log
        WHERE status = 'Open'
        GROUP BY rule_id
        ORDER BY companies_affected DESC, total_occurrences DESC
        """
    ).fetchall()
    conn.close()

    patterns = []
    for r in rows:
        pattern_type = "Systematic" if r["companies_affected"] >= 3 else "Isolated"
        patterns.append({
            "rule_id": r["rule_id"],
            "rule_name": r["rule_name"],
            "severity": r["severity"],
            "companies_affected": r["companies_affected"],
            "total_occurrences": r["total_occurrences"],
            "pattern_type": pattern_type,
            "preventive_action": PREVENTIVE_ACTIONS.get(r["rule_id"], "Review source data pipeline."),
        })

    return patterns


def get_company_risk_profile() -> list[dict]:
    """Rank companies by total defect burden — useful for prioritizing QA reviews."""
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT company_ticker,
               COUNT(*) AS total_defects,
               SUM(CASE WHEN severity = 'Critical' THEN 1 ELSE 0 END) AS critical_count,
               SUM(CASE WHEN severity = 'High' THEN 1 ELSE 0 END) AS high_count,
               SUM(CASE WHEN severity = 'Medium' THEN 1 ELSE 0 END) AS medium_count
        FROM defect_log
        WHERE status = 'Open'
        GROUP BY company_ticker
        ORDER BY critical_count DESC, high_count DESC, total_defects DESC
        """
    ).fetchall()
    conn.close()

    profiles = []
    for r in rows:
        risk_score = (r["critical_count"] * 10) + (r["high_count"] * 6) + (r["medium_count"] * 3)
        risk_label = "High Risk" if risk_score >= 20 else "Medium Risk" if risk_score >= 8 else "Low Risk"
        profiles.append({
            "company_ticker": r["company_ticker"],
            "total_defects": r["total_defects"],
            "critical": r["critical_count"],
            "high": r["high_count"],
            "medium": r["medium_count"],
            "risk_score": risk_score,
            "risk_label": risk_label,
        })

    return profiles


def get_classification_summary() -> dict:
    patterns = detect_systematic_patterns()
    systematic = [p for p in patterns if p["pattern_type"] == "Systematic"]
    isolated = [p for p in patterns if p["pattern_type"] == "Isolated"]

    return {
        "total_rules_triggered": len(patterns),
        "systematic_patterns": len(systematic),
        "isolated_patterns": len(isolated),
        "top_systematic_issues": systematic[:3],
        "company_risk_profiles": get_company_risk_profile()[:5],
    }


if __name__ == "__main__":
    print("\n=== Systematic Error Patterns ===")
    for p in detect_systematic_patterns():
        print(f"  [{p['pattern_type']}] {p['rule_id']} — {p['rule_name']}: {p['companies_affected']} companies, {p['total_occurrences']} occurrences")

    print("\n=== Company Risk Profiles (Top 5) ===")
    for c in get_company_risk_profile()[:5]:
        print(f"  {c['company_ticker']}: {c['risk_label']} | Score: {c['risk_score']} | Defects: {c['total_defects']}")
