"""
Defects routes — query, filter, resolve, and export defect records.
"""

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from api.schemas import DefectResponse, ResolveDefectRequest
import sqlite3, os, csv, io
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "db/equity_qa.db")
router = APIRouter()


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@router.get("/", summary="List all defects with optional filters")
def list_defects(
    severity: str = Query(None, description="Filter by severity: Critical, High, Medium"),
    status: str = Query("Open", description="Filter by status: Open, Resolved, Waived"),
    rule_id: str = Query(None, description="Filter by rule ID e.g. QR-01"),
    limit: int = Query(100, le=1000),
    offset: int = 0,
):
    conn = _get_conn()
    query = "SELECT * FROM defect_log WHERE 1=1"
    params = []

    if severity:
        query += " AND severity = ?"
        params.append(severity)
    if status:
        query += " AND status = ?"
        params.append(status)
    if rule_id:
        query += " AND rule_id = ?"
        params.append(rule_id)

    query += " ORDER BY defect_id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/export/csv", summary="Export defect log as CSV")
def export_defects_csv(status: str = "Open"):
    """Downloads the full defect log as a CSV file for offline review."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM defect_log WHERE status = ? ORDER BY defect_id", (status,)
    ).fetchall()
    conn.close()

    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows([dict(r) for r in rows])

    output.seek(0)
    filename = f"defect_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/{company_ticker}", summary="Get defects for a specific company")
def get_company_defects(company_ticker: str, status: str = "Open"):
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM defect_log WHERE company_ticker = ? AND status = ? ORDER BY severity",
        (company_ticker.upper(), status),
    ).fetchall()
    conn.close()

    if not rows:
        return {"company_ticker": company_ticker.upper(), "defects": [], "count": 0}

    return {
        "company_ticker": company_ticker.upper(),
        "count": len(rows),
        "defects": [dict(r) for r in rows],
    }


@router.patch("/{defect_id}/resolve", summary="Mark a defect as resolved")
def resolve_defect(defect_id: int, body: ResolveDefectRequest):
    conn = _get_conn()
    row = conn.execute("SELECT * FROM defect_log WHERE defect_id = ?", (defect_id,)).fetchone()

    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Defect {defect_id} not found")

    conn.execute(
        """
        UPDATE defect_log
        SET status = 'Resolved', resolved_at = ?, resolution_notes = ?
        WHERE defect_id = ?
        """,
        (datetime.now().isoformat(), body.resolution_notes, defect_id),
    )
    conn.commit()
    conn.close()
    return {"defect_id": defect_id, "status": "Resolved", "resolution_notes": body.resolution_notes}


@router.patch("/{defect_id}/waive", summary="Waive a defect with justification")
def waive_defect(defect_id: int, body: ResolveDefectRequest):
    conn = _get_conn()
    row = conn.execute("SELECT * FROM defect_log WHERE defect_id = ?", (defect_id,)).fetchone()

    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Defect {defect_id} not found")

    conn.execute(
        """
        UPDATE defect_log
        SET status = 'Waived', resolved_at = ?, resolution_notes = ?
        WHERE defect_id = ?
        """,
        (datetime.now().isoformat(), body.resolution_notes, defect_id),
    )
    conn.commit()
    conn.close()
    return {"defect_id": defect_id, "status": "Waived", "resolution_notes": body.resolution_notes}


@router.get("/stats/by-rule", summary="Defect count breakdown by rule")
def stats_by_rule():
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT rule_id, rule_name, severity, COUNT(*) AS count
        FROM defect_log WHERE status = 'Open'
        GROUP BY rule_id ORDER BY count DESC
        """
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/stats/by-source", summary="Defect count breakdown by data source")
def stats_by_source():
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT s.source, COUNT(d.defect_id) AS defect_count,
               COUNT(DISTINCT d.company_ticker) AS companies_affected
        FROM shareholding_raw s
        LEFT JOIN defect_log d ON s.record_id = d.record_id AND d.status = 'Open'
        GROUP BY s.source ORDER BY defect_count DESC
        """
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
