-- =============================================================
-- Equity QA Pipeline — Database Schema
-- Mimics S&P Global Public Ownership domain structure
-- =============================================================

CREATE TABLE IF NOT EXISTS shareholding_raw (
    record_id        TEXT PRIMARY KEY,
    company_ticker   TEXT,
    company_name     TEXT,
    isin             TEXT,
    shareholder_name TEXT,
    ownership_pct    REAL,
    shares_held      INTEGER,
    filing_type      TEXT,
    filing_date      TEXT,
    source           TEXT,
    currency         TEXT DEFAULT 'USD',
    ingested_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS defect_log (
    defect_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id        TEXT,
    company_ticker   TEXT,
    rule_id          TEXT,
    rule_name        TEXT,
    severity         TEXT CHECK(severity IN ('Critical','High','Medium')),
    description      TEXT,
    field_affected   TEXT,
    field_value      TEXT,
    detected_at      TEXT DEFAULT (datetime('now')),
    status           TEXT DEFAULT 'Open' CHECK(status IN ('Open','Resolved','Waived')),
    resolved_at      TEXT,
    resolution_notes TEXT
);

CREATE TABLE IF NOT EXISTS audit_runs (
    run_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at         TEXT DEFAULT (datetime('now')),
    records_scanned INTEGER,
    defects_found  INTEGER,
    defect_rate_pct REAL,
    rules_applied  INTEGER,
    triggered_by   TEXT DEFAULT 'manual'
);

CREATE INDEX IF NOT EXISTS idx_defect_ticker ON defect_log(company_ticker);
CREATE INDEX IF NOT EXISTS idx_defect_severity ON defect_log(severity);
CREATE INDEX IF NOT EXISTS idx_defect_status ON defect_log(status);

-- =============================================================
-- VALIDATION QUERIES (12 business rules)
-- =============================================================

-- Rule QR-01 | Critical: ISIN is null or empty
-- SELECT record_id, company_ticker, 'NULL_ISIN' AS rule
-- FROM shareholding_raw WHERE isin IS NULL OR isin = '';

-- Rule QR-02 | Critical: Ownership percentage exceeds 100%
-- SELECT record_id, company_ticker, ownership_pct
-- FROM shareholding_raw WHERE ownership_pct > 100;

-- Rule QR-03 | Critical: Negative ownership percentage
-- SELECT record_id, company_ticker, ownership_pct
-- FROM shareholding_raw WHERE ownership_pct < 0;

-- Rule QR-04 | High: Missing shareholder name
-- SELECT record_id, company_ticker
-- FROM shareholding_raw WHERE shareholder_name IS NULL OR shareholder_name = '';

-- Rule QR-05 | High: Zero or negative shares held
-- SELECT record_id, company_ticker, shares_held
-- FROM shareholding_raw WHERE shares_held <= 0;

-- Rule QR-06 | High: Filing date before year 2000 (stale/erroneous)
-- SELECT record_id, company_ticker, filing_date
-- FROM shareholding_raw WHERE filing_date < '2000-01-01';

-- Rule QR-07 | High: Duplicate record_id
-- SELECT record_id, COUNT(*) AS cnt
-- FROM shareholding_raw GROUP BY record_id HAVING cnt > 1;

-- Rule QR-08 | Medium: Company ticker is invalid (not in known list)
-- SELECT DISTINCT company_ticker FROM shareholding_raw
-- WHERE company_ticker NOT IN ('AAPL','MSFT','GOOGL','AMZN','TSLA',
--   'JPM','BRK','JNJ','V','NVDA','META','UNH','XOM','WMT','PG');

-- Rule QR-09 | Medium: ISIN format invalid (not 12 chars, not alphanumeric)
-- SELECT record_id, isin FROM shareholding_raw
-- WHERE isin IS NOT NULL AND (LENGTH(isin) != 12 OR isin GLOB '*[^A-Z0-9]*');

-- Rule QR-10 | Medium: Filing date is in the future
-- SELECT record_id, filing_date FROM shareholding_raw
-- WHERE filing_date > date('now');

-- Rule QR-11 | Medium: Total ownership per company exceeds 105% (cross-record check)
-- SELECT company_ticker, SUM(ownership_pct) AS total_pct
-- FROM shareholding_raw GROUP BY company_ticker HAVING total_pct > 105;

-- Rule QR-12 | Medium: Filing type not in allowed list
-- SELECT record_id, filing_type FROM shareholding_raw
-- WHERE filing_type NOT IN (
--   'Annual Report','Exchange Notification','13F Filing','Quarterly Report'
-- );
