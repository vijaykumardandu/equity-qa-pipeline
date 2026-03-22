"""
Test suite for Equity QA Pipeline.
Tests QA engine rules, ingestion logic, and defect classification.
Run: pytest tests/ -v
"""

import pytest
import sqlite3
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Use an in-memory / temp DB for tests
TEST_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
os.environ["DB_PATH"] = TEST_DB


def setup_test_db():
    conn = sqlite3.connect(TEST_DB)
    conn.execute("""CREATE TABLE IF NOT EXISTS shareholding_raw (
        record_id TEXT PRIMARY KEY, company_ticker TEXT, company_name TEXT,
        isin TEXT, shareholder_name TEXT, ownership_pct REAL, shares_held INTEGER,
        filing_type TEXT, filing_date TEXT, source TEXT, currency TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS defect_log (
        defect_id INTEGER PRIMARY KEY AUTOINCREMENT,
        record_id TEXT, company_ticker TEXT, rule_id TEXT, rule_name TEXT,
        severity TEXT, description TEXT, field_affected TEXT, field_value TEXT,
        detected_at TEXT, status TEXT DEFAULT 'Open',
        resolved_at TEXT, resolution_notes TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS audit_runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT, run_at TEXT,
        records_scanned INTEGER, defects_found INTEGER,
        defect_rate_pct REAL, rules_applied INTEGER, triggered_by TEXT)""")
    conn.commit()
    conn.close()


def insert_record(record_id, isin="US1234567890", company_ticker="AAPL",
                  shareholder_name="BlackRock Inc.", ownership_pct=5.0,
                  shares_held=1000000, filing_type="Annual Report",
                  filing_date="2023-06-01", source="SEC EDGAR"):
    conn = sqlite3.connect(TEST_DB)
    conn.execute(
        """INSERT OR REPLACE INTO shareholding_raw
           (record_id, company_ticker, company_name, isin, shareholder_name,
            ownership_pct, shares_held, filing_type, filing_date, source, currency)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (record_id, company_ticker, "Apple Inc.", isin, shareholder_name,
         ownership_pct, shares_held, filing_type, filing_date, source, "USD"),
    )
    conn.commit()
    conn.close()


def clear_tables():
    conn = sqlite3.connect(TEST_DB)
    conn.execute("DELETE FROM shareholding_raw")
    conn.execute("DELETE FROM defect_log")
    conn.commit()
    conn.close()


# ── Setup ──────────────────────────────────────────────────────────────────────
setup_test_db()


# ── Ingestion Tests ────────────────────────────────────────────────────────────
class TestIngestion:
    def setup_method(self):
        clear_tables()

    def test_valid_record_inserts(self):
        insert_record("REC00001")
        conn = sqlite3.connect(TEST_DB)
        count = conn.execute("SELECT COUNT(*) FROM shareholding_raw").fetchone()[0]
        conn.close()
        assert count == 1

    def test_duplicate_record_skipped(self):
        insert_record("REC00001")
        insert_record("REC00001")  # duplicate
        conn = sqlite3.connect(TEST_DB)
        count = conn.execute("SELECT COUNT(*) FROM shareholding_raw").fetchone()[0]
        conn.close()
        assert count == 1

    def test_null_isin_stored(self):
        insert_record("REC00002", isin=None)
        conn = sqlite3.connect(TEST_DB)
        row = conn.execute("SELECT isin FROM shareholding_raw WHERE record_id='REC00002'").fetchone()
        conn.close()
        assert row[0] is None


# ── QA Engine Tests ────────────────────────────────────────────────────────────
class TestQAEngine:
    def setup_method(self):
        clear_tables()

    def _run_single_rule(self, runner_func):
        conn = sqlite3.connect(TEST_DB)
        conn.row_factory = sqlite3.Row
        result = runner_func(conn)
        conn.close()
        return result

    def test_qr01_catches_null_isin(self):
        insert_record("REC00010", isin=None)
        from pipeline.qa_engine import run_qr01
        defects = self._run_single_rule(run_qr01)
        assert len(defects) == 1
        assert defects[0][2] == "QR-01"
        assert defects[0][4] == "Critical"

    def test_qr01_passes_valid_isin(self):
        insert_record("REC00011", isin="US0378331005")
        from pipeline.qa_engine import run_qr01
        defects = self._run_single_rule(run_qr01)
        assert len(defects) == 0

    def test_qr02_catches_ownership_over_100(self):
        insert_record("REC00020", ownership_pct=110.0)
        from pipeline.qa_engine import run_qr02
        defects = self._run_single_rule(run_qr02)
        assert len(defects) == 1
        assert defects[0][4] == "Critical"

    def test_qr02_passes_valid_ownership(self):
        insert_record("REC00021", ownership_pct=8.5)
        from pipeline.qa_engine import run_qr02
        defects = self._run_single_rule(run_qr02)
        assert len(defects) == 0

    def test_qr03_catches_negative_ownership(self):
        insert_record("REC00030", ownership_pct=-5.0)
        from pipeline.qa_engine import run_qr03
        defects = self._run_single_rule(run_qr03)
        assert len(defects) == 1

    def test_qr04_catches_missing_shareholder(self):
        insert_record("REC00040", shareholder_name=None)
        from pipeline.qa_engine import run_qr04
        defects = self._run_single_rule(run_qr04)
        assert len(defects) == 1
        assert defects[0][4] == "High"

    def test_qr05_catches_zero_shares(self):
        insert_record("REC00050", shares_held=0)
        from pipeline.qa_engine import run_qr05
        defects = self._run_single_rule(run_qr05)
        assert len(defects) == 1

    def test_qr06_catches_stale_date(self):
        insert_record("REC00060", filing_date="1995-01-01")
        from pipeline.qa_engine import run_qr06
        defects = self._run_single_rule(run_qr06)
        assert len(defects) == 1

    def test_qr06_passes_valid_date(self):
        insert_record("REC00061", filing_date="2023-06-15")
        from pipeline.qa_engine import run_qr06
        defects = self._run_single_rule(run_qr06)
        assert len(defects) == 0

    def test_qr08_catches_invalid_ticker(self):
        insert_record("REC00080", company_ticker="????")
        from pipeline.qa_engine import run_qr08
        defects = self._run_single_rule(run_qr08)
        assert any(d[1] == "????" for d in defects)

    def test_qr08_passes_valid_ticker(self):
        insert_record("REC00081", company_ticker="AAPL")
        from pipeline.qa_engine import run_qr08
        defects = self._run_single_rule(run_qr08)
        assert len(defects) == 0

    def test_full_audit_run(self):
        # Insert mix of clean and dirty records
        insert_record("REC00100")                              # clean
        insert_record("REC00101", isin=None)                  # QR-01
        insert_record("REC00102", ownership_pct=120.0)        # QR-02
        insert_record("REC00103", shareholder_name=None)      # QR-04
        insert_record("REC00104", shares_held=0)              # QR-05

        from pipeline.qa_engine import run_full_audit
        result = run_full_audit()

        assert result["records_scanned"] == 5
        assert result["defects_found"] >= 4
        assert result["defect_rate_pct"] > 0
        assert result["rules_applied"] == 12
        assert "Critical" in result["by_severity"]


# ── Defect Classifier Tests ────────────────────────────────────────────────────
class TestDefectClassifier:
    def setup_method(self):
        clear_tables()
        # Seed some defects across multiple companies
        for i, ticker in enumerate(["AAPL", "MSFT", "GOOGL", "AMZN"], start=1):
            insert_record(f"REC0{i}00", company_ticker=ticker, isin=None)
        from pipeline.qa_engine import run_full_audit
        run_full_audit()

    def test_systematic_pattern_detected(self):
        from pipeline.defect_classifier import detect_systematic_patterns
        patterns = detect_systematic_patterns()
        systematic = [p for p in patterns if p["pattern_type"] == "Systematic"]
        # QR-01 (null ISIN) fired for 4 companies → should be Systematic
        assert any(p["rule_id"] == "QR-01" for p in systematic)

    def test_company_risk_profiles_generated(self):
        from pipeline.defect_classifier import get_company_risk_profile
        profiles = get_company_risk_profile()
        assert len(profiles) > 0
        assert all("risk_score" in p for p in profiles)
        assert all("risk_label" in p for p in profiles)

    def test_preventive_actions_mapped(self):
        from pipeline.defect_classifier import PREVENTIVE_ACTIONS
        assert "QR-01" in PREVENTIVE_ACTIONS
        assert len(PREVENTIVE_ACTIONS["QR-01"]) > 20


# ── Severity Classification Tests ─────────────────────────────────────────────
class TestSeverityClassification:
    def setup_method(self):
        clear_tables()

    def test_critical_severity_for_null_isin(self):
        insert_record("REC00200", isin=None)
        from pipeline.qa_engine import run_full_audit
        run_full_audit()
        conn = sqlite3.connect(TEST_DB)
        row = conn.execute(
            "SELECT severity FROM defect_log WHERE rule_id='QR-01' LIMIT 1"
        ).fetchone()
        conn.close()
        assert row[0] == "Critical"

    def test_high_severity_for_missing_shareholder(self):
        insert_record("REC00201", shareholder_name=None)
        from pipeline.qa_engine import run_full_audit
        run_full_audit()
        conn = sqlite3.connect(TEST_DB)
        row = conn.execute(
            "SELECT severity FROM defect_log WHERE rule_id='QR-04' LIMIT 1"
        ).fetchone()
        conn.close()
        assert row[0] == "High"

    def test_medium_severity_for_invalid_filing_type(self):
        insert_record("REC00202", filing_type="Random Filing")
        from pipeline.qa_engine import run_full_audit
        run_full_audit()
        conn = sqlite3.connect(TEST_DB)
        row = conn.execute(
            "SELECT severity FROM defect_log WHERE rule_id='QR-12' LIMIT 1"
        ).fetchone()
        conn.close()
        assert row[0] == "Medium"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
