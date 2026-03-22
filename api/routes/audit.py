"""
Audit routes — trigger QA runs, view audit history, ingest data.
"""

from fastapi import APIRouter, HTTPException
from pipeline.qa_engine import run_full_audit
from pipeline.ingestor import load_from_csv, load_from_json, get_record_count
from pipeline.defect_classifier import detect_systematic_patterns, get_company_risk_profile, get_classification_summary
from api.schemas import AuditRunResponse, AuditSummaryResponse, IngestRequest
import sqlite3, os

DB_PATH = os.environ.get("DB_PATH", "db/equity_qa.db")
router = APIRouter()


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@router.post("/run", response_model=AuditRunResponse, summary="Trigger a full QA audit pass")
def trigger_audit(clear_previous: bool = True):
    """
    Runs all 12 validation rules against the current shareholding dataset.
    Returns defect counts by severity and rule. Previous defects are cleared by default.
    """
    try:
        result = run_full_audit(clear_previous=clear_previous)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary", response_model=AuditSummaryResponse, summary="Current audit KPI summary")
def get_summary():
    """
    Returns the current quality KPI snapshot:
    total records, open defects, defect rate, and breakdown by severity.
    """
    conn = _get_conn()
    total = conn.execute("SELECT COUNT(*) FROM shareholding_raw").fetchone()[0]

    latest_run = conn.execute(
        "SELECT run_at FROM audit_runs ORDER BY run_id DESC LIMIT 1"
    ).fetchone()

    sev = conn.execute(
        """
        SELECT severity, COUNT(*) AS cnt FROM defect_log
        WHERE status = 'Open' GROUP BY severity
        """
    ).fetchall()
    sev_map = {r["severity"]: r["cnt"] for r in sev}

    open_total = conn.execute(
        "SELECT COUNT(*) FROM defect_log WHERE status = 'Open'"
    ).fetchone()[0]

    resolved = conn.execute(
        "SELECT COUNT(*) FROM defect_log WHERE status = 'Resolved'"
    ).fetchone()[0]

    conn.close()

    defect_rate = round(open_total / total * 100, 2) if total else 0

    return {
        "latest_run_at": latest_run["run_at"] if latest_run else None,
        "total_records": total,
        "open_defects": open_total,
        "defect_rate_pct": defect_rate,
        "critical_count": sev_map.get("Critical", 0),
        "high_count": sev_map.get("High", 0),
        "medium_count": sev_map.get("Medium", 0),
        "resolved_count": resolved,
    }


@router.get("/history", summary="Audit run history")
def get_history(limit: int = 10):
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM audit_runs ORDER BY run_id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/patterns", summary="Systematic error patterns across companies")
def get_patterns():
    """
    Identifies rules that appear across 3+ companies — a sign of
    upstream data pipeline issues vs isolated filing errors.
    """
    return detect_systematic_patterns()


@router.get("/risk-profiles", summary="Company risk ranking by defect burden")
def get_risk_profiles():
    return get_company_risk_profile()


@router.get("/classification", summary="Full classification summary")
def get_classification():
    return get_classification_summary()


@router.post("/ingest", summary="Load data file into the pipeline")
def ingest_data(req: IngestRequest):
    """
    Ingests a CSV or JSON shareholding data file.
    Run /audit/run after ingestion to execute QA checks.
    """
    if not os.path.exists(req.file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {req.file_path}")
    try:
        if req.file_type.lower() == "csv":
            result = load_from_csv(req.file_path)
        elif req.file_type.lower() == "json":
            result = load_from_json(req.file_path)
        else:
            raise HTTPException(status_code=400, detail="file_type must be 'csv' or 'json'")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
