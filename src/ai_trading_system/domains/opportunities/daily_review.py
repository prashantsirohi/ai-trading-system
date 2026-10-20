"""Versioned, persistence-free operator attention projection. Never a trade gate."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from hashlib import sha256
import json
from math import isfinite
from typing import Any

POLICY_VERSION = "daily-operator-review-v1"
SOURCE_TYPES = {
    "rank": ("rank", "ranked_universe"),
    "shortlist": ("rank", "ranked_signals"),
    "investigator": ("investigator", "investigator_scores"),
    "investigator_active": ("investigator", "active_watchlist"),
    "fundamental": ("fundamental_discovery", "fundamental_thesis_universe"),
    "patterns": ("pattern_lane_scan", "pattern_lane_assessments"),
    "convergence": ("opportunities", "opportunity_convergence_view"),
    "technical": ("opportunities", "technical_evidence_labels"),
    "final_universe": ("opportunities", "final_review_universe"),
    "final_list": ("opportunities", "final_review_list"),
    "final_summary": ("opportunities", "final_review_summary"),
    "tracker": ("candidate_tracker", "candidate_tracker_current"),
    "positions": ("execute", "positions"),
    "execution_summary": ("execute", "execute_summary"),
    "monitor": ("opportunities", "position_monitor_reconciliation"),
}
MATERIAL_FIELDS = (
    "queue",
    "sources_json",
    "attention_reasons_json",
    "stage",
    "investigator_state",
    "fundamental_state",
    "fundamental_thesis",
    "pattern_state",
    "final_review_selected",
    "readiness",
    "trigger",
    "invalidation",
    "sma20_break_state",
    "tracker_status",
    "has_position",
    "position_mode",
    "position_status",
    "position_details_json",
    "blockers_json",
    "episode_ids_json",
)
POLICY = {
    "version": POLICY_VERSION,
    "identity": "exchange_symbol_session",
    "queues": ["POSITION_ATTENTION", "ENTRY_REVIEW", "INVESTIGATE", "WATCH"],
    "inclusion": [
        "shortlist",
        "investigator",
        "investigator_active",
        "fundamental",
        "patterns",
        "final_list",
        "tracker",
        "positions",
    ],
    "investigator_primary_score": 65,
    "investigator_primary_trigger": "WEEKLY_GAINER",
    "closed_states": ["ARCHIVED", "DROPPED", "CLOSED", "INVALIDATED", "EXPIRED"],
    "risk_tracker_states": [
        "TECHNICAL_FAILURE",
        "RESULT_FAILURE",
        "REMOVE_FROM_TRACKING",
    ],
    "sort": "queue_material_change_final_readiness_rank_identity",
    "removals": "only_between_complete_membership_coverage_under_identical_policy",
    "technical_labels": "context_only",
    "rank": "broad_rank_only_never_investigator_score",
    "authority": "shadow_presentation_only",
    "source_artifacts": SOURCE_TYPES,
    "material_fields": MATERIAL_FIELDS,
    "tracker_identity": "unique_explicit_current_listing_only",
    "investigator_identity": "NSE_when_exchange_absent_per_intake_contract",
    "entry_review": "final_list_SETUP_REVIEW_without_conflict_or_recorded_risk",
    "position_mode": "PAPER_from_ExecuteStage_PaperExecutionAdapter",
    "missing_position_feed": "retain_prior_position_as_unverified_attention_never_current_holdings",
}


def json_text(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False
    )


def digest(value: Any) -> str:
    return sha256(json_text(value).encode()).hexdigest()


POLICY_HASH = digest(POLICY)
FIELDS = (
    "priority",
    "exchange",
    "symbol",
    "session",
    "queue",
    "change",
    "sources_json",
    "attention_reasons_json",
    "evidence_status",
    "stage",
    "sector",
    "rank_score",
    "rank_shortlisted",
    "investigator_state",
    "investigator_score",
    "fundamental_state",
    "fundamental_thesis",
    "pattern_state",
    "convergence_cohort",
    "final_review_selected",
    "final_review_priority",
    "readiness",
    "trigger",
    "invalidation",
    "extension_pct",
    "near_high_state",
    "above_sma20_state",
    "sma20_break_state",
    "tracker_status",
    "has_position",
    "position_mode",
    "position_status",
    "position_scope",
    "position_details_json",
    "episode_ids_json",
    "blockers_json",
    "source_dates_json",
    "evidence_links_json",
    "details_json",
    "material_hash",
    "policy_version",
    "policy_hash",
)
CHANGE_FIELDS = (
    "exchange",
    "symbol",
    "session",
    "change",
    "previous_queue",
    "queue",
    "reason",
)


@dataclass
class Source:
    rows: list[dict] = field(default_factory=list)
    lineage: dict = field(default_factory=dict)
    issue: str | None = None


def number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if isfinite(result) else None
    except (ValueError, TypeError):
        return None


def truth(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "1.0", "yes"}


def text(row: dict, *names: str) -> str:
    return next(
        (str(row[n]).strip() for n in names if row.get(n) not in (None, "")), ""
    )


def strings(value: Any) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value]
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
        if isinstance(parsed, list):
            return [str(v) for v in parsed]
    except (ValueError, TypeError):
        pass
    return [str(value)]


def row_dates(row: dict) -> list[str]:
    return [
        text(row, name)[:10]
        for name in (
            "session",
            "session_date",
            "trade_date",
            "as_of",
            "observed_session",
            "snapshot_date",
            "source_available_at",
        )
        if text(row, name)
    ]


def build_daily_review(
    *,
    session: str,
    sources: dict[str, Source],
    final_summary: dict,
    execution_summary: dict,
    previous: list[dict] | None = None,
    previous_summary: dict | None = None,
) -> tuple[list[dict], list[dict], dict]:
    """Union source-owned memberships without changing their policies or scores."""
    grouped: dict[str, dict[tuple[str, str], list[dict]]] = {}
    identity_exceptions = []
    known: dict[str, set[str]] = defaultdict(set)
    # Tracker is symbol-only today. Resolve only against explicit current sources;
    # dual-exchange collisions remain exceptions rather than guessed NSE matches.
    for source in sources.values():
        if source.issue:
            continue
        for row in source.rows:
            symbol = text(row, "symbol", "symbol_id").upper()
            exchange = text(row, "exchange").upper()
            if (
                symbol
                and exchange in {"NSE", "BSE"}
                and not any(d > session for d in row_dates(row))
            ):
                known[symbol].add(exchange)
    for name, source in sources.items():
        groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
        if not source.issue:
            for row in source.rows:
                symbol = text(row, "symbol", "symbol_id").upper()
                exchange = text(row, "exchange").upper()
                if not exchange and name in {"investigator", "investigator_active"}:
                    exchange = "NSE"  # Declared Investigator intake domain.
                if not exchange and name == "tracker" and len(known[symbol]) == 1:
                    exchange = next(iter(known[symbol]))
                if not symbol or exchange not in {"NSE", "BSE"}:
                    identity_exceptions.append(
                        {
                            "source": name,
                            "symbol": symbol,
                            "reason": "UNRESOLVED_LISTING_IDENTITY",
                        }
                    )
                    continue
                dates = row_dates(row)
                if any(d > session for d in dates):
                    identity_exceptions.append(
                        {
                            "source": name,
                            "exchange": exchange,
                            "symbol": symbol,
                            "reason": "FUTURE_ROW",
                        }
                    )
                    continue
                groups[(exchange, symbol)].append(row)
        grouped[name] = groups

    issues = {name: source.issue for name, source in sources.items() if source.issue}
    calendar_ok = (
        str((final_summary.get("calendar") or {}).get("status", "")).upper() == "PASS"
    )
    status = "complete"
    if issues or identity_exceptions or final_summary.get("status") != "complete":
        status = "degraded"
    if not calendar_ok or sources.get("rank", Source(issue="MISSING")).issue:
        status = "failed"
    prior_ok = (
        previous is not None
        and (previous_summary or {}).get("policy_hash") == POLICY_HASH
    )
    prior = (
        {(r["exchange"], r["symbol"]): r for r in (previous or [])} if prior_ok else {}
    )
    membership_complete = (
        not issues
        and not identity_exceptions
        and calendar_ok
        and final_summary.get("status") in {"complete", "degraded"}
    )
    comparable = (
        prior_ok
        and membership_complete
        and previous_summary.get("membership_coverage_complete") is True
    )
    rows = []
    keys = sorted(set().union(*(set(g) for g in grouped.values())))
    for key in keys:
        entries = {name: groups.get(key, []) for name, groups in grouped.items()}
        # Repeated neutral observation references are harmless only when identical.
        for name in ("technical",):
            entries[name] = list(
                {json_text(v): v for v in entries.get(name, [])}.values()
            )
        one = {
            name: values[0] if len(values) == 1 else {}
            for name, values in entries.items()
        }
        conflicts = [
            f"{name}:DUPLICATE_ROWS"
            for name, values in entries.items()
            if len(values) > 1 and name not in {"tracker", "positions", "monitor"}
        ]
        if conflicts:
            membership_complete = False
            comparable = False
        r, inv, active = (
            one.get("rank", {}),
            one.get("investigator", {}),
            one.get("investigator_active", {}),
        )
        f, p = one.get("fundamental", {}), one.get("patterns", {})
        review, selected = one.get("final_universe", {}), one.get("final_list", {})
        tech = one.get("technical", {})
        position_rows = [
            v
            for v in entries.get("positions", [])
            if (number(v.get("quantity")) or 0) != 0
        ]
        tracker_rows = [v for v in entries.get("tracker", []) if truth(v.get("active"))]
        include = []
        if entries.get("shortlist"):
            include.append("RANK")
        if any(
            text(v, "trigger_reason").upper() == POLICY["investigator_primary_trigger"]
            and (number(v.get("final_score")) or 0)
            >= POLICY["investigator_primary_score"]
            and text(v, "trade_date")[:10] in {"", session}
            for v in entries.get("investigator", [])
        ):
            include.append("INVESTIGATOR")
        if any(
            text(v, "status").upper() not in POLICY["closed_states"]
            for v in entries.get("investigator_active", [])
        ):
            include.append("INVESTIGATOR_ACTIVE")
        if any(
            text(v, "classification_status").upper() == "QUALIFIED"
            for v in entries.get("fundamental", [])
        ):
            include.append("FUNDAMENTAL")  # Daily admission blockers remain visible.
        if any(
            text(v, "pattern_evaluation_state") == "KNOWN"
            and text(v, "lane_freshness") == "FRESH"
            and truth(v.get("pattern_member"))
            and text(v, "session_date", "as_of_date")[:10] in {"", session}
            for v in entries.get("patterns", [])
        ):
            include.append("PATTERN")
        if entries.get("final_list"):
            include.append("FINAL_REVIEW")
        if tracker_rows:
            include.append("TRACKER")
        if position_rows:
            include.append("POSITION")
        if not include:
            continue
        blockers = sorted(
            set(
                conflicts
                + strings(review.get("reasons_json"))
                + strings(review.get("adapter_reasons_json"))
                + strings(f.get("admission_blockers"))
            )
        )
        tracker_status = sorted({text(v, "status") for v in tracker_rows})
        risk = [v for v in tracker_status if v in POLICY["risk_tracker_states"]]
        if text(inv, "verdict") == "NOISE_TRAP" or truth(
            inv.get("long_upper_wick_trap")
        ):
            risk.append("INVESTIGATOR_TRAP")
        if text(tech, "sma20_break_state") == "MET":
            risk.append("SMA20_BREAK")
        readiness = text(review, "readiness") or "NOT_EVALUATED"
        if readiness == "DEFER":
            risk.append("FINAL_REVIEW_DEFER")
        if position_rows:
            queue = "POSITION_ATTENTION"
            reasons = ["OPEN_POSITION_REVIEW", *risk]
            monitors = entries.get("monitor", [])
            if not monitors:
                reasons.append("POSITION_MONITOR_NOT_OBSERVED")
            elif any(not truth(v.get("route_data_covered")) for v in monitors):
                reasons.append("RECORDED_POSITION_MONITOR_GAP")
        elif selected and readiness == "SETUP_REVIEW" and not conflicts and not risk:
            queue, reasons = "ENTRY_REVIEW", ["SHADOW_FINAL_SETUP_REVIEW"]
        elif (
            risk
            or conflicts
            or inv
            or (
                active
                and text(active, "status")
                in {"NEW_TRIGGER", "ACTIVE_RESEARCH", "HIGH_CONVICTION"}
            )
            or "RANK" in include
        ):
            queue, reasons = "INVESTIGATE", risk or conflicts or ["DISCOVERY_REVIEW"]
        else:
            queue, reasons = "WATCH", ["CONTINUING_EVIDENCE"]
        # Evidence-level returns/scores are never blended. Detail keeps all episodes.
        lineage = {
            name: sources[name].lineage for name, values in entries.items() if values
        }
        mode = "PAPER"  # ExecuteStage's producer is explicitly PaperExecutionAdapter.
        row = dict.fromkeys(FIELDS, "")
        row.update(
            exchange=key[0],
            symbol=key[1],
            session=session,
            queue=queue,
            sources_json=json_text(include),
            attention_reasons_json=json_text(reasons),
            evidence_status="CONFLICT" if conflicts else status.upper(),
            stage=text(review, "stage"),
            sector=text(r, "sector", "sector_name"),
            rank_score=number(r.get("rank_score", r.get("composite_score"))),
            rank_shortlisted="RANK" in include,
            investigator_state=text(active, "status")
            or text(inv, "verdict")
            or "NOT_EVALUATED",
            investigator_score=number(inv.get("final_score")),
            fundamental_state=text(review, "fundamental_state")
            or text(f, "classification_status")
            or "NOT_EVALUATED",
            fundamental_thesis=text(f, "primary_thesis"),
            pattern_state=text(review, "pattern_state")
            or text(p, "pattern_evaluation_state")
            or "NOT_EVALUATED",
            convergence_cohort=text(one.get("convergence", {}), "convergence_cohort")
            or "UNKNOWN",
            final_review_selected=bool(selected),
            final_review_priority=selected.get("review_priority", ""),
            readiness=readiness,
            trigger=review.get("trigger", ""),
            invalidation=review.get("invalidation", ""),
            extension_pct=review.get("extension_pct", ""),
            near_high_state=text(tech, "near_52w_high_10_state") or "UNKNOWN",
            above_sma20_state=text(tech, "above_sma20_state") or "UNKNOWN",
            sma20_break_state=text(tech, "sma20_break_state") or "UNKNOWN",
            tracker_status=";".join(tracker_status),
            has_position=bool(position_rows),
            position_mode=mode if position_rows else "NOT_APPLICABLE",
            position_status="CURRENT" if position_rows else "NONE",
            position_scope="execute.positions_after" if position_rows else "none",
            position_details_json=json_text(position_rows),
            episode_ids_json=json_text(
                sorted(
                    {
                        text(v, "episode_id", "candidate_id")
                        for values in entries.values()
                        for v in values
                        if text(v, "episode_id", "candidate_id")
                    }
                )
            ),
            blockers_json=json_text(blockers),
            source_dates_json=json_text(
                {n: v.get("session") for n, v in lineage.items()}
            ),
            evidence_links_json=json_text(lineage),
            details_json=json_text({n: v for n, v in entries.items() if v}),
            policy_version=POLICY_VERSION,
            policy_hash=POLICY_HASH,
        )
        row["material_hash"] = digest({n: row[n] for n in MATERIAL_FIELDS})
        rows.append(row)
    unverified_positions = set()
    if sources.get("positions", Source(issue="MISSING")).issue:
        for key, old in prior.items():
            if (
                not truth(old.get("has_position"))
                and old.get("position_status") != "UNVERIFIED_PRIOR"
            ):
                continue
            current = next(
                (r for r in rows if (r["exchange"], r["symbol"]) == key), None
            )
            row = current if current is not None else dict(old)
            row.update(
                session=session,
                has_position=None,
                position_status="UNVERIFIED_PRIOR",
                position_scope="previous_session_unverified",
                queue="POSITION_ATTENTION",
                position_mode=old["position_mode"],
                position_details_json=old["position_details_json"],
                evidence_status="STALE",
                sources_json=json_text(
                    [
                        *(strings(row["sources_json"]) if current is not None else []),
                        "PRIOR_POSITION_UNVERIFIED",
                    ]
                ),
                attention_reasons_json='["POSITION_SOURCE_UNAVAILABLE"]',
            )
            links = json.loads(row["evidence_links_json"])
            prior_links = json.loads(old["evidence_links_json"])
            links["prior_positions"] = prior_links.get("positions") or prior_links.get(
                "prior_positions"
            )
            row["evidence_links_json"] = json_text(links)
            dates = json.loads(row["source_dates_json"])
            old_dates = json.loads(old["source_dates_json"])
            dates["prior_positions"] = (
                old_dates.get("positions")
                or old_dates.get("prior_positions")
                or old["session"]
            )
            row["source_dates_json"] = json_text(dates)
            row["material_hash"] = digest({n: row[n] for n in MATERIAL_FIELDS})
            if current is None:
                rows.append(row)
            unverified_positions.add(key)
    if not membership_complete and status == "complete":
        status = "degraded"
    for row in rows:
        old = prior.get((row["exchange"], row["symbol"]))
        row["change"] = (
            "UNKNOWN"
            if (row["exchange"], row["symbol"]) in unverified_positions
            else "UNCHANGED"
            if old and old.get("material_hash") == row["material_hash"]
            else "CHANGED"
            if old
            else "NEW"
            if comparable
            else "BASELINE"
            if not prior_ok
            else "UNKNOWN"
        )
    queues = {q: i for i, q in enumerate(POLICY["queues"])}
    ready = {"SETUP_REVIEW": 0, "DEVELOPING_WATCH": 1, "DEFER": 2}
    rows.sort(
        key=lambda r: (
            queues[r["queue"]],
            r["change"] == "UNCHANGED",
            ready.get(r["readiness"], 3),
            r["rank_score"] is None,
            -(r["rank_score"] or 0),
            r["exchange"],
            r["symbol"],
        )
    )
    changes = []
    for i, row in enumerate(rows, 1):
        row["priority"] = i
        if row["change"] != "UNCHANGED":
            old = prior.get((row["exchange"], row["symbol"]), {})
            changes.append(
                dict(
                    exchange=row["exchange"],
                    symbol=row["symbol"],
                    session=session,
                    change=row["change"],
                    previous_queue=old.get("queue", ""),
                    queue=row["queue"],
                    reason=";".join(strings(row["attention_reasons_json"])),
                )
            )
    current_keys = {(r["exchange"], r["symbol"]) for r in rows}
    absent = sorted(set(prior) - current_keys)
    for key in absent:
        changes.append(
            dict(
                exchange=key[0],
                symbol=key[1],
                session=session,
                change="REMOVED" if comparable else "UNCONFIRMED_ABSENCE",
                previous_queue=prior[key]["queue"],
                queue="",
                reason="NO_CURRENT_SOURCE_MEMBERSHIP"
                if comparable
                else "INCOMPLETE_SOURCE_COVERAGE",
            )
        )
    summary = dict(
        status=status,
        shadow_only=True,
        session=session,
        policy_version=POLICY_VERSION,
        policy_hash=POLICY_HASH,
        calendar=final_summary.get("calendar", {}),
        rows=len(rows),
        changes=len(changes),
        final_review_status=final_summary.get("status", "MISSING"),
        final_review_universe_states=final_summary.get("universe_states", {}),
        final_review_snapshot_issues=final_summary.get("snapshot_issues", []),
        queue_counts=dict(Counter(r["queue"] for r in rows)),
        source_issues=issues,
        source_rows={name: len(s.rows) for name, s in sources.items()},
        included_by_source=dict(
            Counter(n for r in rows for n in strings(r["sources_json"]))
        ),
        identity_exceptions=identity_exceptions,
        conflict_rows=sum(r["evidence_status"] == "CONFLICT" for r in rows),
        unverified_prior_positions=len(unverified_positions),
        source_lineage={name: s.lineage for name, s in sources.items()},
        comparison_status="COMPLETE"
        if comparable
        else "BASELINE"
        if not prior_ok
        else "PARTIAL",
        previous_session=(previous_summary or {}).get("session"),
        previous_run_id=(previous_summary or {}).get("run_id"),
        membership_coverage_complete=membership_complete,
        position_scope="execution ledger only; actual journal is separate; no balance pooling",
        execution_preview=truth(execution_summary.get("preview_only")),
        authority="shadow presentation; no admission, lifecycle, execution or broker authority",
    )
    summary["decision_content_hash"] = digest(
        {"rows": rows, "changes": changes, "summary": summary}
    )
    return rows, changes, summary
