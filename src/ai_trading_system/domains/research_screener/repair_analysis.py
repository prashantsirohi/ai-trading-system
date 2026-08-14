from __future__ import annotations

from collections import Counter
from datetime import date
import re


def fundamental_repair_lane(member: dict) -> str:
    fundamentals = member["inputs"]["fundamentals"]
    if fundamentals.get("scope") == "SCOPE_UNRESOLVED":
        return "STATEMENT_SCOPE_UNRESOLVED"
    disclosed = fundamentals.get("latest_disclosed_periods", {})
    parsed = fundamentals.get("latest_parsed_periods", {})
    if any(disclosed.get(kind) != parsed.get(kind) for kind in ("annual", "quarterly")):
        return "LATEST_DISCLOSED_DOCUMENT_NOT_VALIDATED"
    missing = fundamentals.get("missing_target_periods", {})
    missing_periods = [
        value for kind in ("annual", "quarterly") for value in missing.get(kind, [])
    ]
    listing_date = _earliest_listing_date(member)
    if missing_periods and listing_date is not None and all(
        date.fromisoformat(str(period)) < listing_date for period in missing_periods
    ):
        return "GENUINE_POST_LISTING_HISTORY_GAP"
    if missing_periods:
        return "MISSING_HISTORICAL_FILING_PERIODS"
    return "FILED_METRIC_COMPLETENESS_GAP"


def corporate_action_repair_lanes(member: dict) -> list[dict]:
    validation = member["inputs"].get("corporate_action_validation", {})
    rows = []
    for event in validation.get("unmatched_events", []):
        action_type = str(event.get("action_type") or "").lower()
        if action_type in {"split", "bonus"}:
            lane = "SPLIT_BONUS_OPERATIONAL_BACKFILL_CANDIDATE"
        elif action_type == "rights":
            lane = "RIGHTS_TERMS_AND_PRICE_BASIS_REQUIRED"
        elif action_type in {"demerger", "merger"}:
            lane = "SCHEME_AND_SUCCESSOR_PRICE_BASIS_REQUIRED"
        elif action_type == "consolidation":
            lane = "CONSOLIDATION_TERMS_AND_PRICE_BASIS_REQUIRED"
        else:
            lane = "UNSUPPORTED_ACTION_TAXONOMY_REVIEW"
        price_factor, share_factor = parse_split_bonus_terms(event)
        rows.append({
            "symbol": member["symbol"], "isin": member["isin"],
            "action_type": action_type, "ex_date": str(event.get("ex_date")),
            "repair_lane": lane, "source_row_hash": event.get("source_row_hash"),
            "parsed_price_factor": price_factor,
            "parsed_share_factor": share_factor,
        })
    return rows


def parse_split_bonus_terms(event: dict) -> tuple[float | None, float | None]:
    """Parse only unambiguous split/bonus terms from the official subject."""
    action_type = str(event.get("action_type") or "").lower()
    subject = str(event.get("raw_subject") or "")
    if action_type == "split":
        match = re.search(
            r"from\s+rs\.?\s*(\d+(?:\.\d+)?)\s*/?-?\s*to\s+rs\.?\s*(\d+(?:\.\d+)?)",
            subject, re.IGNORECASE,
        )
        if not match:
            return None, None
        old_face_value, new_face_value = map(float, match.groups())
        if old_face_value <= 0 or new_face_value <= 0 or new_face_value >= old_face_value:
            return None, None
        return new_face_value / old_face_value, old_face_value / new_face_value
    if action_type == "bonus":
        match = re.search(
            r"bonus\s+issue\s+(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)",
            subject, re.IGNORECASE,
        )
        if not match:
            return None, None
        new_shares, existing_shares = map(float, match.groups())
        if new_shares <= 0 or existing_shares <= 0:
            return None, None
        share_factor = (new_shares + existing_shares) / existing_shares
        return 1.0 / share_factor, share_factor
    return None, None


def combined_price_factor(events: list[dict]) -> float | None:
    """Combine same-date split/bonus factors; fail when any term is ambiguous."""
    factor = 1.0
    if not events:
        return None
    for event in events:
        price_factor, _ = parse_split_bonus_terms(event)
        if price_factor is None:
            return None
        factor *= price_factor
    return factor


def repair_profile(members: list[dict]) -> dict:
    fundamental = []
    actions = []
    for member in members:
        reason_codes = {row["code"] for row in member.get("reasons", [])}
        if "FUNDAMENTAL_PROVENANCE_OR_COMPLETENESS_FAILED" in reason_codes:
            fundamental.append({
                "symbol": member["symbol"], "isin": member["isin"],
                "repair_lane": fundamental_repair_lane(member),
            })
        if "CORPORATE_ACTION_CONTINUITY_FAILED" in reason_codes:
            actions.extend(corporate_action_repair_lanes(member))
    return {
        "fundamental_member_count": len(fundamental),
        "fundamental_lane_counts": dict(sorted(Counter(
            row["repair_lane"] for row in fundamental
        ).items())),
        "corporate_action_member_count": len({row["isin"] for row in actions}),
        "corporate_action_event_count": len(actions),
        "corporate_action_lane_counts": dict(sorted(Counter(
            row["repair_lane"] for row in actions
        ).items())),
        "fundamental_rows": fundamental,
        "corporate_action_rows": actions,
    }


def _earliest_listing_date(member: dict) -> date | None:
    dates = []
    for listing in member.get("identity", {}).get("listings", []):
        value = listing.get("listing_date")
        if value:
            try:
                dates.append(date.fromisoformat(str(value)))
            except ValueError:
                continue
    return min(dates) if dates else None
