from __future__ import annotations

from datetime import date
from math import isfinite

from .models import Disposition


def deduplicate_security_rows(rows: list[dict]) -> list[dict]:
    """Deduplicate exchange listings at security level without dropping listings."""
    grouped: dict[str, dict] = {}
    for row in rows:
        isin = str(row.get("isin") or "").strip().upper()
        if not isin:
            raise ValueError("security row has no ISIN")
        security = grouped.setdefault(isin, {"isin": isin, "listings": []})
        listing = {key: row.get(key) for key in ("exchange", "symbol", "series", "bse_code", "valid_from", "valid_to")}
        if listing not in security["listings"]:
            security["listings"].append(listing)
    return list(grouped.values())


def choose_statement_scope(scope_stats: list[dict]) -> dict:
    """Prefer usable consolidated statements; otherwise use best standalone, never splice."""
    consolidated = next((row for row in scope_stats if row["scope"] == "consolidated" and row["completeness"] >= 0.70), None)
    if consolidated:
        return consolidated | {"reason": "consolidated_usable"}
    standalone = next((row for row in scope_stats if row["scope"] == "standalone" and row["completeness"] > 0), None)
    if standalone:
        return standalone | {"reason": "standalone_fallback_no_splicing"}
    return {"scope": "SCOPE_UNRESOLVED", "completeness": 0.0, "reason": "no_usable_scope"}


def available_period_cagr(values: list[tuple[date, float | None]]) -> dict:
    usable = sorted((when, float(value)) for when, value in values if value is not None and float(value) > 0)
    if len(usable) < 2:
        return {"value": None, "observation_count": len(usable), "period_count": 0, "state": "INSUFFICIENT_HISTORY"}
    years = (usable[-1][0] - usable[0][0]).days / 365.2425
    if years <= 0:
        return {"value": None, "observation_count": len(usable), "period_count": 0, "state": "INSUFFICIENT_HISTORY"}
    value = (usable[-1][1] / usable[0][1]) ** (1 / years) - 1
    return {"value": value, "observation_count": len(usable), "period_count": len(usable) - 1, "state": "PRESENT"}


def point_in_time_rows(rows: list[dict], as_of_date: date) -> list[dict]:
    output = []
    for row in rows:
        available = row.get("available_at") or row.get("published_at")
        if available is None:
            continue
        available_date = date.fromisoformat(str(available)[:10])
        if available_date <= as_of_date:
            output.append(row)
    return output


def terminal_disposition(*, identity_status: str, board_status: str, market_cap_cr: float | None,
                         min_market_cap_cr: float, max_market_cap_cr: float,
                         fundamental_completeness: float | None) -> Disposition:
    if identity_status != "RESOLVED":
        return Disposition.DATA_REPAIR_REQUIRED
    if board_status not in {"MAIN", "ELIGIBLE"}:
        return Disposition.INELIGIBLE_BOARD_OR_INSTRUMENT if board_status != "BOARD_UNKNOWN" else Disposition.ELIGIBILITY_UNKNOWN
    if market_cap_cr is None or not isfinite(market_cap_cr) or market_cap_cr <= 0:
        return Disposition.ELIGIBILITY_UNKNOWN
    if not min_market_cap_cr <= market_cap_cr <= max_market_cap_cr:
        return Disposition.INELIGIBLE_MARKET_CAP
    if fundamental_completeness is None or fundamental_completeness < 0.70:
        return Disposition.DATA_REPAIR_REQUIRED
    return Disposition.BOUNDARY_REVIEW
