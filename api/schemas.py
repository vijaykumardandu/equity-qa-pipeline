from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class AuditRunResponse(BaseModel):
    run_at: str
    records_scanned: int
    defects_found: int
    defect_rate_pct: float
    rules_applied: int
    by_severity: dict
    by_rule: dict


class AuditSummaryResponse(BaseModel):
    latest_run_at: Optional[str]
    total_records: int
    open_defects: int
    defect_rate_pct: float
    critical_count: int
    high_count: int
    medium_count: int
    resolved_count: int


class DefectResponse(BaseModel):
    defect_id: int
    record_id: str
    company_ticker: Optional[str]
    rule_id: str
    rule_name: str
    severity: str
    description: str
    field_affected: str
    field_value: Optional[str]
    detected_at: str
    status: str
    resolved_at: Optional[str]
    resolution_notes: Optional[str]


class ResolveDefectRequest(BaseModel):
    resolution_notes: str


class IngestRequest(BaseModel):
    file_path: str
    file_type: str = "csv"
