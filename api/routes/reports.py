"""
Reports routes — generate and download audit summary reports.
"""

from fastapi import APIRouter
from fastapi.responses import FileResponse
import os

router = APIRouter()


@router.get("/generate", summary="Generate audit summary Excel report")
def generate_report():
    """Triggers Excel report generation and returns download path."""
    try:
        from reports.audit_report_generator import generate_audit_report
        path = generate_audit_report()
        return {"status": "generated", "file": path}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@router.get("/download", summary="Download the latest generated report")
def download_report():
    path = "reports/audit_summary.xlsx"
    if not os.path.exists(path):
        from reports.audit_report_generator import generate_audit_report
        path = generate_audit_report()
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="equity_qa_audit_report.xlsx",
    )
