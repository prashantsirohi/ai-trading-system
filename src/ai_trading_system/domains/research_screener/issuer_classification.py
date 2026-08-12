from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date, datetime
from pathlib import Path


FINANCIAL_SECTORS = {"Finance", "Insurance"}
MARKET_INFRASTRUCTURE_INDUSTRIES = {
    "Depositories, Clearing Houses and Other Intermediaries",
    "Exchange and Data Platform",
}


def route_company_type(sector: str | None, industry: str | None) -> str:
    """Route an issuer only from an explicit mastered sector/industry pair."""
    normalized_sector = str(sector or "").strip()
    normalized_industry = str(industry or "").strip()
    if not normalized_sector or not normalized_industry:
        return "UNCLASSIFIED"
    if normalized_sector == "Banks":
        return "BANK"
    if normalized_sector == "Capital Markets":
        if normalized_industry in MARKET_INFRASTRUCTURE_INDUSTRIES:
            return "MARKET_INFRASTRUCTURE"
        return "FINANCIAL_INSTITUTION"
    if normalized_sector in FINANCIAL_SECTORS:
        return "FINANCIAL_INSTITUTION"
    return "INDUSTRIAL"


class ExistingIssuerClassificationProvider:
    """Freeze exact-ISIN current-master classification without changing the store."""

    def __init__(self, masterdata_path: str | Path):
        self.masterdata_path = Path(masterdata_path)

    def snapshot(self, cohort: list[dict], as_of_date: date) -> tuple[dict[str, dict], bytes]:
        if not self.masterdata_path.is_file():
            rows_by_isin: dict[str, list[tuple]] = {}
        else:
            connection = sqlite3.connect(
                f"file:{self.masterdata_path}?mode=ro", uri=True,
            )
            try:
                rows = connection.execute(
                    """SELECT upper(isin), symbol_id, symbol_name, sector, industry, last_updated
                       FROM symbols WHERE isin IS NOT NULL AND trim(isin) <> ''"""
                ).fetchall()
            finally:
                connection.close()
            rows_by_isin = {}
            for row in rows:
                rows_by_isin.setdefault(str(row[0]), []).append(row)

        snapshot_rows: list[dict] = []
        classifications: dict[str, dict] = {}
        for member in sorted(cohort, key=lambda row: (row["isin"], row["symbol"])):
            isin = str(member["isin"]).upper()
            matches = rows_by_isin.get(isin, [])
            if len(matches) != 1:
                reason = "EXACT_ISIN_NOT_MASTERED" if not matches else "EXACT_ISIN_AMBIGUOUS"
                result = {
                    "state": "UNRESOLVED", "company_type": "UNCLASSIFIED",
                    "sector": None, "industry": None, "source": "masterdata.db.symbols",
                    "source_row_hash": None, "reason": reason,
                }
            else:
                row = matches[0]
                observed_date = _observed_date(row[5])
                company_type = route_company_type(row[3], row[4])
                if observed_date is not None and observed_date > as_of_date:
                    state, reason = "UNRESOLVED", "CLASSIFICATION_OBSERVED_AFTER_CUTOFF"
                    company_type = "UNCLASSIFIED"
                elif company_type == "UNCLASSIFIED":
                    state, reason = "UNRESOLVED", "SECTOR_OR_INDUSTRY_MISSING"
                else:
                    state, reason = "PRESENT", "EXACT_ISIN_CURRENT_MASTER_MATCH"
                evidence = {
                    "isin": isin, "symbol_id": row[1], "symbol_name": row[2],
                    "sector": row[3], "industry": row[4], "last_updated": row[5],
                }
                result = {
                    "state": state, "company_type": company_type,
                    "sector": row[3], "industry": row[4],
                    "source": "masterdata.db.symbols",
                    "source_row_hash": hashlib.sha256(
                        _canonical_json(evidence).encode("utf-8")
                    ).hexdigest(),
                    "reason": reason, "observed_at": row[5],
                }
            classifications[isin] = result
            snapshot_rows.append({
                "isin": isin, "symbol": member["symbol"], **result,
            })
        raw = (_canonical_json({
            "schema_version": "issuer-classification-snapshot-v1",
            "as_of_date": as_of_date.isoformat(), "rows": snapshot_rows,
        }) + "\n").encode("utf-8")
        return classifications, raw


def _observed_date(value: object) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
