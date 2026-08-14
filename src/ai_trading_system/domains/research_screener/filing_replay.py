from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from .filings import completeness, parse_xbrl_statement


def rebuild_fundamentals(
    prior: dict, artifacts: list[dict], *, company_type: str, as_of_date: date,
) -> dict:
    """Re-normalize checksum-verified 1.1 XBRLs under a successor metric contract."""
    snapshots = {
        provider: _provider_snapshot(
            provider, artifacts, company_type=company_type, as_of_date=as_of_date,
        )
        for provider in ("NSE", "BSE")
    }
    primary, fallback = snapshots["NSE"], snapshots["BSE"]
    selected_name = "fallback" if _quality(fallback) > _quality(primary) else "primary"
    selected = dict(fallback if selected_name == "fallback" else primary)
    selected["provider_selection"] = {
        "policy": "ordered-official-snapshot-v1-no-period-splicing",
        "primary_provider": primary["provenance_validation"]["provider"],
        "fallback_provider": fallback["provenance_validation"]["provider"],
        "selected": selected_name,
        "primary_quality": _quality(primary),
        "fallback_quality": _quality(fallback),
        "evidence_replay": "checksum-verified-filing-discovery-v1.1.0",
    }
    _merge_prior_issuer_repairs(selected, prior, company_type=company_type)
    selected["source_artifact_ids"] = [
        artifact["artifact_id"] for artifact in artifacts
        if "corporate_actions" not in str(artifact.get("source_key") or "")
    ]
    return selected


def _provider_snapshot(provider: str, artifacts: list[dict], *, company_type: str,
                       as_of_date: date) -> dict:
    candidates = [
        artifact for artifact in artifacts
        if artifact.get("source_key") == "filing_xbrl" and artifact.get("provider") == provider
    ]
    if not candidates:
        return _empty(f"no {provider} filing artifacts exist in the predecessor evidence")
    scopes = {
        str(artifact.get("metadata", {}).get("scope") or "SCOPE_UNRESOLVED")
        for artifact in candidates
    }
    scopes.discard("SCOPE_UNRESOLVED")
    if len(scopes) != 1:
        return _empty(f"{provider} predecessor evidence has unresolved or competing scopes")
    scope = scopes.pop()
    latest_disclosed = {
        period_type: max(
            (_as_date(artifact.get("effective_date")) for artifact in candidates
             if artifact.get("metadata", {}).get("period_type") == period_type),
            default=None,
        )
        for period_type in ("annual", "quarterly")
    }
    statements = []
    seen = set()
    for artifact in candidates:
        if artifact.get("validation_status") != "VALID":
            continue
        period_type = str(artifact.get("metadata", {}).get("period_type") or "")
        period_end = _as_date(artifact.get("effective_date"))
        key = (period_type, period_end, artifact.get("content_hash"))
        if period_type not in {"annual", "quarterly"} or period_end is None or key in seen:
            continue
        seen.add(key)
        raw_path = artifact.get("_raw_path")
        if not raw_path:
            continue
        raw = Path(raw_path).read_bytes()
        parsed = parse_xbrl_statement(raw, period_end=period_end, period_type=period_type)
        published = artifact.get("published_at") or artifact.get("metadata", {}).get("published_at")
        statements.append({
            "period_type": period_type, "period_end": period_end,
            "published_at": published, "scope": scope,
            "source_document_url": artifact.get("source_url"),
            "source_provider": f"{provider}_XBRL_REPLAY",
            "identity_evidence": "PREDECESSOR_CHECKSUM_AND_IDENTITY_VALIDATED",
            "document_revision_id": artifact.get("content_hash"),
            "source_artifact_id": artifact["artifact_id"], **parsed,
        })
    return _finalize_snapshot(
        statements, scope=scope, scope_reason="predecessor_whole_provider_scope",
        latest_disclosed=latest_disclosed, company_type=company_type,
        as_of_date=as_of_date, provider=f"{provider}_XBRL_REPLAY",
    )


def _merge_prior_issuer_repairs(selected: dict, prior: dict, *, company_type: str) -> None:
    if selected["scope"] == "SCOPE_UNRESOLVED":
        return
    for period_type in ("annual", "quarterly"):
        key = f"{period_type}_statements"
        existing = {str(row["period_end"]) for row in selected[key]}
        for row in prior.get(key, []):
            if row.get("formula_version") != "issuer-pdf-curated-v1":
                continue
            if row.get("scope") != selected["scope"] or str(row.get("period_end")) in existing:
                continue
            copied = dict(row)
            copied["period_end"] = _as_date(copied["period_end"])
            selected[key].append(copied)
            existing.add(str(copied["period_end"]))
    refreshed = _finalize_snapshot(
        selected["annual_statements"] + selected["quarterly_statements"],
        scope=selected["scope"], scope_reason=selected["scope_reason"],
        latest_disclosed=selected["latest_disclosed_periods"],
        company_type=company_type, as_of_date=date.max,
        provider=selected["provenance_validation"]["provider"],
    )
    provider_selection = selected.get("provider_selection")
    source_ids = selected.get("source_artifact_ids")
    selected.update(refreshed)
    if provider_selection is not None:
        selected["provider_selection"] = provider_selection
    if source_ids is not None:
        selected["source_artifact_ids"] = source_ids


def _finalize_snapshot(statements: list[dict], *, scope: str, scope_reason: str,
                       latest_disclosed: dict, company_type: str, as_of_date: date,
                       provider: str | list[str]) -> dict:
    annual = sorted(
        (row for row in statements if row["period_type"] == "annual"),
        key=lambda row: row["period_end"], reverse=True,
    )[:6]
    quarterly = sorted(
        (row for row in statements if row["period_type"] == "quarterly"),
        key=lambda row: row["period_end"], reverse=True,
    )[:12]
    targets = {
        "annual": _target_period_ends(latest_disclosed.get("annual"), "annual", 6),
        "quarterly": _target_period_ends(latest_disclosed.get("quarterly"), "quarterly", 12),
    }
    annual_completeness = completeness(
        annual, company_type=company_type, period_type="annual", periods=6,
        target_period_ends=targets["annual"],
    )
    quarterly_completeness = completeness(
        quarterly, company_type=company_type, period_type="quarterly", periods=12,
        target_period_ends=targets["quarterly"],
    )
    latest_parsed = {
        "annual": max((row["period_end"] for row in annual), default=None),
        "quarterly": max((row["period_end"] for row in quarterly), default=None),
    }
    latest_match = all(
        latest_disclosed.get(kind) is not None
        and latest_parsed[kind] == latest_disclosed[kind]
        for kind in ("annual", "quarterly")
    )
    usable = latest_match and min(annual_completeness, quarterly_completeness) >= 0.70
    if not latest_match:
        reason = f"latest disclosed filing XBRL could not be validated: disclosed={latest_disclosed}, parsed={latest_parsed}"
    elif not usable:
        reason = f"official filing XBRL completeness below 70%: annual={annual_completeness}, quarterly={quarterly_completeness}"
    else:
        reason = "official filing XBRL normalized with required period coverage"
    period_sets = {
        "annual": {row["period_end"] for row in annual},
        "quarterly": {row["period_end"] for row in quarterly},
    }
    providers = provider if isinstance(provider, list) else [provider]
    published_dates = [_as_datetime(row.get("published_at")) for row in annual + quarterly]
    return {
        "scope": scope, "scope_reason": scope_reason,
        "annual_completeness": annual_completeness,
        "quarterly_completeness": quarterly_completeness,
        "annual_period_count": len(annual), "quarterly_period_count": len(quarterly),
        "annual_statements": annual, "quarterly_statements": quarterly,
        "latest_disclosed_periods": latest_disclosed,
        "latest_parsed_periods": latest_parsed, "target_periods": targets,
        "missing_target_periods": {
            kind: [period for period in targets[kind] if period not in period_sets[kind]]
            for kind in ("annual", "quarterly")
        },
        "state": "PRESENT" if usable else "DATA_REPAIR_REQUIRED",
        "provenance_validation": {
            "provider": providers,
            "available_at": bool(published_dates) and all(
                value is not None and value.date() <= as_of_date for value in published_dates
            ),
            "source_row_hash": bool(statements) and all(row.get("source_row_hash") for row in statements),
            "filing_source": bool(statements), "reason": reason,
            "evidence_replay": "checksum-verified-filing-discovery-v1.1.0",
        },
    }


def _quality(snapshot: dict) -> tuple[bool, bool, float, float]:
    disclosed = snapshot.get("latest_disclosed_periods", {})
    parsed = snapshot.get("latest_parsed_periods", {})
    latest_matched = all(
        disclosed.get(kind) is not None and parsed.get(kind) == disclosed.get(kind)
        for kind in ("annual", "quarterly")
    )
    annual = float(snapshot.get("annual_completeness") or 0.0)
    quarterly = float(snapshot.get("quarterly_completeness") or 0.0)
    return snapshot.get("state") == "PRESENT", latest_matched, min(annual, quarterly), annual + quarterly


def _target_period_ends(latest: date | None, period_type: str, count: int) -> list[date]:
    if latest is None:
        return []
    if period_type == "annual":
        return [date(latest.year - offset, latest.month, latest.day) for offset in range(count)]
    result = []
    start_month_index = latest.year * 12 + latest.month - 1
    quarter_days = {3: 31, 6: 30, 9: 30, 12: 31}
    for offset in range(count):
        year, zero_based_month = divmod(start_month_index - 3 * offset, 12)
        month = zero_based_month + 1
        result.append(date(year, month, quarter_days[month]))
    return result


def _empty(reason: str) -> dict:
    return {
        "scope": "SCOPE_UNRESOLVED", "scope_reason": "filing_source_unavailable",
        "annual_completeness": 0.0, "quarterly_completeness": 0.0,
        "annual_period_count": 0, "quarterly_period_count": 0,
        "annual_statements": [], "quarterly_statements": [],
        "latest_disclosed_periods": {"annual": None, "quarterly": None},
        "latest_parsed_periods": {"annual": None, "quarterly": None},
        "target_periods": {"annual": [], "quarterly": []},
        "missing_target_periods": {"annual": [], "quarterly": []},
        "state": "DATA_REPAIR_REQUIRED",
        "provenance_validation": {
            "provider": [], "available_at": False, "source_row_hash": False,
            "filing_source": False, "reason": reason,
            "evidence_replay": "checksum-verified-filing-discovery-v1.1.0",
        },
    }


def _as_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _as_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
