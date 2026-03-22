"""
Equity QA Pipeline — FastAPI Application
Exposes audit triggers, defect queries, and report downloads.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import audit, defects, reports
from pipeline.ingestor import init_db

app = FastAPI(
    title="Equity QA Pipeline API",
    description=(
        "Quality assurance API for public ownership shareholding data. "
        "Validates filings against 12 business rules, classifies defects by severity, "
        "and exposes findings for downstream reporting and remediation workflows."
    ),
    version="1.0.0",
    contact={"name": "QA Operations Team"},
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(audit.router, prefix="/audit", tags=["Audit"])
app.include_router(defects.router, prefix="/defects", tags=["Defects"])
app.include_router(reports.router, prefix="/reports", tags=["Reports"])


@app.on_event("startup")
def startup():
    init_db()


@app.get("/", tags=["Health"])
def root():
    return {
        "service": "Equity QA Pipeline",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": ["/audit", "/defects", "/reports", "/docs"],
    }


@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok"}
