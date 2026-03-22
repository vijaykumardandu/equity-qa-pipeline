# Equity Data Quality Audit Pipeline

A quality assurance system for public ownership shareholding data — built to understand how investment data firms like S&P Global validate filings before they reach downstream analytics.

The idea came from reading about S&P Global's Public Ownership domain, which collects shareholding data from annual reports, 13F filings, and exchange notifications globally. Bad data at ingestion cascades into wrong ownership percentages, broken ISIN mappings, and ultimately incorrect fund analytics. This project automates the checks that would normally be done manually.

---

## What it does

Takes raw shareholding records (CSV/JSON), runs them through 12 validation rules, flags defects by severity, and surfaces everything through a REST API and monitoring dashboard.

The test dataset has 1,000 records with ~18% intentional errors injected — null ISINs, ownership percentages over 100%, stale filing dates, duplicate record IDs, and so on. The pipeline caught 156 defects and identified 7 error patterns that appeared across multiple companies (not just one-off filing mistakes — actual upstream data pipeline issues).

**Numbers that came out of the test run:**
- 963 records ingested (37 filtered as duplicates)
- 156 defects found → 16.2% defect rate
- 77 Critical / 52 High / 27 Medium
- 7 systematic patterns across companies
- 21/21 unit tests passing

---

## Tech stack

- **Python** — data generation, QA engine, classification logic
- **SQLite** — stores raw records, defect log, audit run history
- **FastAPI** — 14 REST endpoints for audit triggers, defect queries, report downloads
- **Streamlit** — 6-page monitoring dashboard
- **openpyxl** — 4-sheet formatted Excel audit report
- **pytest** — 21 unit tests covering all 12 validation rules

---

## Project structure

```
equity-qa-pipeline/
├── data/
│   ├── generate_mock_data.py      # generates 1,000 records with injected errors
│   ├── sample_shareholding.csv
│   └── sample_shareholding.json
├── db/
│   └── schema.sql                 # table definitions + 12 validation query templates
├── pipeline/
│   ├── ingestor.py                # loads CSV/JSON into SQLite, handles deduplication
│   ├── qa_engine.py               # runs all 12 rules, writes to defect_log table
│   └── defect_classifier.py       # detects systematic patterns, builds risk profiles
├── api/
│   ├── main.py
│   ├── schemas.py
│   └── routes/
│       ├── audit.py               # /audit/run, /audit/summary, /audit/patterns
│       ├── defects.py             # /defects/, /defects/{company}, export CSV
│       └── reports.py             # /reports/generate, /reports/download
├── dashboard/
│   └── streamlit_app.py           # overview, defect log, rule performance, company risk
├── reports/
│   └── audit_report_generator.py  # generates Excel report with KPIs and defect breakdown
├── tests/
│   └── test_qa_engine.py
├── requirements.txt
└── README.md
```

---

## Running it locally

```bash
# create and activate virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# install dependencies
pip install -r requirements.txt

# generate mock data
python data/generate_mock_data.py

# initialize DB and ingest
python -c "from pipeline.ingestor import init_db, load_from_csv; init_db(); load_from_csv('data/sample_shareholding.csv')"

# run QA audit
python pipeline/qa_engine.py

# generate Excel report
python reports/audit_report_generator.py

# start API (terminal 1)
uvicorn api.main:app --reload --port 8000

# start dashboard (terminal 2)
streamlit run dashboard/streamlit_app.py
```

API docs at `http://localhost:8000/docs`  
Dashboard at `http://localhost:8501`

On Windows, prefix report and API commands with `$env:PYTHONPATH = "."` if you get module import errors.

---

## The 12 validation rules

| Rule | Severity | What it catches |
|------|----------|-----------------|
| QR-01 | Critical | Null or missing ISIN |
| QR-02 | Critical | Ownership % > 100 |
| QR-03 | Critical | Negative ownership % |
| QR-04 | High | Missing shareholder name |
| QR-05 | High | Zero or negative shares held |
| QR-06 | High | Filing date before year 2000 |
| QR-07 | High | Duplicate record ID |
| QR-08 | Medium | Ticker not in known company master |
| QR-09 | Medium | ISIN not 12 alphanumeric chars (ISO 6166) |
| QR-10 | Medium | Future filing date |
| QR-11 | Medium | Company aggregate ownership > 105% |
| QR-12 | Medium | Filing type not in approved taxonomy |

The classifier also separates **systematic patterns** (same rule firing across 3+ companies — upstream pipeline issue) from **isolated incidents** (single company — one-off filing error). QR-01 fired across 14 companies in the test run, which points to a missing ISIN validation step at the source ingestion layer.

---

## API endpoints

```
POST  /audit/run                  trigger full QA pass
GET   /audit/summary              defect rate + KPI snapshot
GET   /audit/patterns             systematic error patterns
GET   /audit/risk-profiles        companies ranked by defect burden
GET   /defects/                   list defects (filter by severity, rule, status)
GET   /defects/{company_ticker}   all defects for one company
GET   /defects/export/csv         download full defect log
PATCH /defects/{id}/resolve       mark defect resolved with notes
PATCH /defects/{id}/waive         waive with justification
GET   /defects/stats/by-rule      defect counts per rule
GET   /defects/stats/by-source    defect counts per filing source
GET   /reports/generate           generate Excel audit report
GET   /reports/download           download latest report
```

---

## Background / domain context

- **ISIN** — 12-character ISO 6166 identifier used to map a shareholding record to a security in the global master file. Without it, the record is unmappable.
- **13F filing** — quarterly SEC submission by institutional investment managers disclosing equity holdings over $100M
- **Beneficial owner** — the entity that actually controls or benefits from shares, which may differ from the registered holder
- **Corporate action** — stock splits, mergers, dividends — events that require ownership percentages to be recalculated across all affected records

---
## Screenshots
<img width="2879" height="1502" alt="Screenshot 2026-03-22 161551" src="https://github.com/user-attachments/assets/1450f5ce-2d46-4c51-a904-435e8a0cbdd7" />

