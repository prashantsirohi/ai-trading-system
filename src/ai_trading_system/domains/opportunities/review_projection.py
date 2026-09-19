"""Materialize review-only decisions from validated source snapshots."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, timedelta
import json
from math import isfinite
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.domains.opportunities.review_policy import (
    POLICY,
    EvidenceStamp,
    FundamentalEvidence,
    LateBaseMetrics,
    PatternEvidence,
    PatternSignal,
    RankEvidence,
    ReviewInput,
    SetupEvidence,
    UniverseEvidence,
    evaluate_review,
    ordered_review_list,
)
from ai_trading_system.domains.opportunities.review_sources import (
    ReviewSource,
    ReviewSourceBundle,
    digest_bytes,
    utc,
)
from ai_trading_system.domains.opportunities.policy_snapshot import PolicySnapshot

ADAPTER_VERSION = "final-review-adapter-v2"
REVIEW_FIELDS = (
    "review_priority",
    "exchange",
    "symbol",
    "session",
    "stage",
    "stage_status",
    "transition",
    "universe_state",
    "selected",
    "combination",
    "readiness",
    "rank_score",
    "position_attention_required",
    "policy_version",
    "policy_hash",
    "pattern_state",
    "pattern_qualified",
    "pattern_reasons",
    "fundamental_state",
    "fundamental_qualified",
    "fundamental_reasons",
    "reasons_json",
    "setup_signal_id",
    "trigger",
    "invalidation",
    "extension_pct",
    "extension_policy",
    "adapter_reasons_json",
    "position_cycle_ids_json",
    "position_scope",
    "liquidity_percentile",
    "close",
    "late_base_metrics_json",
    "pattern_details_json",
    "fundamental_thesis",
    "rank_source_session",
    "adapter_policy_version",
)
ADAPTER_POLICY = {
    "version": ADAPTER_VERSION,
    "extension_policy": "review-setup-extension-v1",
    "extension_low_max_pct": 5.0,
    "extension_medium_max_pct": 10.0,
    "setup_selection": "confirmed_then_priority_then_score_then_signal_id",
    "market_window_bars": 220,
    "price_basis": "complete_adjusted_ohlc_else_raw_only_if_no_adjustment",
    "calendar": "trusted_broad_NSE_sessions_with_holidays_and_NIFTY_50_diagnostics",
    "source_vintage": "captured_current_store_not_historical_vintage",
    "pattern_basis": "reject_nonunit_adjustments_inside_signal_window",
    "rank_field": "composite_score_adjusted_else_composite_score",
    "universe": "union_market_master_all_sources_positions",
}


def json_text(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False
    )


def policy_snapshot() -> PolicySnapshot:
    content = {POLICY.version: asdict(POLICY), ADAPTER_VERSION: ADAPTER_POLICY}
    hashes = {
        key: digest_bytes(json_text(value).encode()) for key, value in content.items()
    }
    return PolicySnapshot(digest_bytes(json_text(hashes).encode()), hashes, content)


def _number(value: Any) -> float | None:
    if isinstance(value, (bool, np.bool_)):
        return None
    try:
        v = float(value)
        return v if isfinite(v) else None
    except (ValueError, TypeError):
        return None


def _bool(value: Any) -> bool | None:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    text = str(value).strip().lower()
    return (
        True
        if text in ("true", "1", "1.0")
        else False
        if text in ("false", "0", "0.0")
        else None
    )


def _date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None


def _list(value: Any) -> list:
    if isinstance(value, (list, tuple)):
        return list(value)
    result = json.loads(str(value))
    if not isinstance(result, list):
        raise ValueError("Expected a JSON array")
    return result


def _groups(source: ReviewSource) -> tuple[dict, list[dict]]:
    groups: dict[tuple[str, str], list[dict]] = {}
    rejected = []
    if source.issue:
        return groups, rejected
    for row in source.frame.to_dict("records"):
        key = (
            str(row.get("exchange", "")).strip().upper(),
            str(row.get("symbol_id", "")).strip().upper(),
        )
        if key[0] not in ("NSE", "BSE") or not key[1]:
            rejected.append(
                {"reason": "INVALID_LISTING_IDENTITY", "row": json_text(row)}
            )
        else:
            groups.setdefault(key, []).append(row)
    return groups, rejected


def _stamp(
    bundle: ReviewSourceBundle,
    key: tuple[str, str],
    source: ReviewSource | None,
    session: date | None,
    version: str,
) -> EvidenceStamp:
    meta = source.metadata if source else {}
    return EvidenceStamp(
        *key,
        session or date.min,
        utc(meta.get("available_at", bundle.decision_at)),
        str(meta.get("content_hash") or bundle.snapshot_hashes.get("market", "")),
        version,
    )


def calendar_context(bundle: ReviewSourceBundle) -> tuple[tuple[date, ...], dict]:
    reference_dates = tuple(_date(v) for v in bundle.indices.get("date", []))
    reference_valid = (
        bool(reference_dates)
        and None not in reference_dates
        and len(set(reference_dates)) == len(reference_dates)
    )
    reference_dates = tuple(sorted(reference_dates)) if reference_valid else ()
    recent_start = bundle.session - timedelta(days=45)
    years = {d.year for d in bundle.holidays}
    recent_expected = {
        d.date()
        for d in pd.bdate_range(recent_start, bundle.session)
        if d.date() not in bundle.holidays
    }
    stock_dates = bundle.market.loc[bundle.market.exchange.eq("NSE")].copy()
    stock_dates["day"] = pd.to_datetime(stock_dates.timestamp).dt.date
    broad = {
        _date(day)
        for day in stock_dates.groupby("day")
        .symbol_id.nunique()
        .loc[lambda s: s >= 100]
        .index
    }
    broad.discard(None)
    recent_actual = {d for d in broad if d >= recent_start}
    recent_reference = {d for d in reference_dates if d >= recent_start}
    missing = sorted(recent_expected - recent_actual)
    # A special weekend session needs independent NIFTY corroboration.
    extra = sorted((recent_actual - recent_expected) - recent_reference)
    reference_missing = sorted(recent_expected - recent_reference)
    valid = (
        bundle.session in recent_actual
        and not missing
        and not extra
        and recent_start.year in years
        and bundle.session.year in years
    )
    market_dates = tuple(sorted(d for d in broad if d <= bundle.session))
    historical_start = market_dates[-220] if len(market_dates) >= 220 else None
    history_covered = (
        all(y in years for y in range(historical_start.year, bundle.session.year + 1))
        if historical_start
        else False
    )
    historical_expected = {
        d.date()
        for d in pd.bdate_range(historical_start or bundle.session, bundle.session)
        if d.date() not in bundle.holidays
    }
    history_complete = (
        len(market_dates) >= 220
        and history_covered
        and historical_expected.issubset(broad)
    )
    reference_future = False
    if "validated_at" in bundle.indices:
        versions = pd.to_datetime(
            bundle.indices.validated_at, errors="coerce", utc=True
        )
        if versions.gt(bundle.decision_at).any():
            reference_future = True
    degraded = bool(reference_missing or not reference_valid or reference_future)
    return market_dates if valid else (), {
        "status": "DEGRADED" if valid and degraded else "PASS" if valid else "FAILED",
        "calendar_authority": "trusted_broad_NSE_bhavcopy_population",
        "diagnostic_reference": "NIFTY_50",
        "exchange": "NSE",
        "recent_schedule_days": 45,
        "missing_sessions": [str(d) for d in missing],
        "diagnostic_reference_missing_sessions": [str(d) for d in reference_missing],
        "diagnostic_reference_valid": reference_valid,
        "diagnostic_reference_future_version": reference_future,
        "unsupported_extra_sessions": [str(d) for d in extra],
        "full_metric_window_calendar_verified": history_complete,
        "metric_window_sessions": min(len(market_dates), 220),
        "holiday_years": sorted(years),
        "limitation": "BSE calendar unsupported",
    }


def _market_context(
    frame: pd.DataFrame,
    bundle: ReviewSourceBundle,
    key: tuple[str, str],
    calendar: tuple[date, ...],
    quarantine: pd.DataFrame,
) -> tuple[dict, list[str]]:
    issues = []
    if frame.empty:
        return {
            "history_bars": 0,
            "close": None,
            "late_base": None,
            "technical_stage2": None,
        }, ["MARKET_MISSING"]
    f = frame.sort_values("timestamp").copy()
    f["day"] = pd.to_datetime(f.timestamp).dt.date
    if f.day.duplicated().any():
        issues.append("DUPLICATE_MARKET_SESSIONS")
    if f.day.iloc[-1] != bundle.session:
        issues.append("LATEST_MARKET_SESSION_MISSING")
    window = f.tail(220).copy()
    for col in ("open", "high", "low", "close"):
        adjusted = pd.to_numeric(window[f"adjusted_{col}"], errors="coerce")
        factor = pd.to_numeric(window.adjustment_factor, errors="coerce")
        raw = pd.to_numeric(window[col], errors="coerce")
        if (adjusted.isna() & factor.notna() & factor.ne(1)).any():
            issues.append("INCOMPLETE_ADJUSTED_PRICE_BASIS")
        if (
            adjusted.notna()
            & factor.notna()
            & ~np.isclose(adjusted, raw * factor, rtol=1e-6, atol=1e-8)
        ).any():
            issues.append("ADJUSTMENT_FACTOR_MISMATCH")
        if (
            adjusted.notna()
            & factor.isna()
            & ~np.isclose(adjusted, raw, rtol=1e-6, atol=1e-8)
        ).any():
            issues.append("INCOMPLETE_ADJUSTED_PRICE_BASIS")
        window[col] = adjusted.where(adjusted.notna(), raw)
    price = window[["open", "high", "low", "close"]].apply(
        pd.to_numeric, errors="coerce"
    )
    valid = np.isfinite(price).all(axis=1) & price.gt(0).all(axis=1)
    valid &= price.high.ge(price[["open", "close", "low"]].max(axis=1)) & price.low.le(
        price[["open", "close", "high"]].min(axis=1)
    )
    volume = pd.to_numeric(window.volume, errors="coerce")
    valid &= volume.notna() & np.isfinite(volume) & volume.ge(0)
    if not valid.all():
        issues.append("INVALID_MARKET_BARS")
    providers = {"NSE": "nse_bhavcopy", "BSE": "bse_bhavcopy"}
    trusted = window.provider.eq(providers[key[0]]) & window.validation_status.isin(
        ("trusted_primary", "trusted_repaired")
    )
    trusted &= ~window.provider_discrepancy_flag.fillna(False).astype(bool)
    if not trusted.all():
        issues.append("MARKET_PROVENANCE_UNTRUSTED")
    for col in ("ingestion_ts", "adjusted_at"):
        known = pd.to_datetime(window[col], errors="coerce", utc=True)
        if known.gt(bundle.decision_at).any():
            issues.append("MARKET_VERSION_AFTER_CUTOFF")
    q = quarantine
    for field in ("created_at", "resolved_at"):
        if (
            not q.empty
            and field in q
            and pd.to_datetime(q[field], errors="coerce", utc=True)
            .gt(bundle.decision_at)
            .any()
        ):
            issues.append("MARKET_VERSION_AFTER_CUTOFF")
    if (
        not q.empty
        and (
            (q.exchange.eq(key[0]) & q.symbol_id.eq(key[1]) & ~q.status.eq("resolved"))
            & (pd.to_datetime(q.trade_date).dt.date >= window.day.iloc[0])
        ).any()
    ):
        issues.append("UNRESOLVED_MARKET_QUARANTINE")
    if key[0] != "NSE" or not calendar:
        issues.append("EXCHANGE_CALENDAR_UNVERIFIED")
    elif not set(
        d for d in calendar if window.day.iloc[0] <= d <= bundle.session
    ).issubset(set(window.day)):
        issues.append("MISSING_REFERENCE_MARKET_SESSIONS")
    close = _number(window.close.iloc[-1])
    info: dict[str, Any] = {
        "history_bars": int(len(f)),
        "close": close,
        "late_base": None,
        "technical_stage2": None,
    }
    if issues or len(window) < 220:
        return info, issues
    c = window.close.astype(float).reset_index(drop=True)
    sma150 = c.rolling(150, min_periods=150).mean()
    sma200 = c.rolling(200, min_periods=200).mean()
    ma150, ma200 = float(sma150.iloc[-1]), float(sma200.iloc[-1])
    slope150, slope200 = (
        (ma150 / sma150.iloc[-21] - 1) * 100,
        (ma200 / sma200.iloc[-21] - 1) * 100,
    )
    base = window.tail(65)
    base_high, base_low = float(base.high.max()), float(base.low.min())
    ranges = window.high - window.low
    previous_range, previous_volume = (
        float(ranges.iloc[-40:-20].median()),
        float(volume.iloc[-40:-20].median()),
    )
    info["late_base"] = LateBaseMetrics(
        close / ma150,
        float(slope150),
        (base_high - base_low) / base_high * 100,
        float(ranges.tail(20).median()) / previous_range
        if previous_range > 0
        else None,
        max(0, base_high - close) / base_high * 100,
        float((close / c.iloc[-21] - close / c.iloc[-61]) * 100),
        float(volume.tail(20).median()) / previous_volume
        if previous_volume > 0
        else None,
        close / ma200,
        float(slope200),
    )
    info["technical_stage2"] = bool(
        close > ma150 > ma200
        and slope200 > 0
        and close >= float(window.high.max()) * 0.75
    )
    return info, issues


def _pattern(bundle, key, source, rows, signal_source, signal_rows, market):
    if source.issue or len(rows) != 1:
        return None, [], [source.issue or "PATTERN_ASSESSMENT_CARDINALITY"]
    row = rows[0]
    issues = []
    try:
        ids = _list(row.get("signal_ids_json", "[]"))
        actual = [str(s.get("signal_id", "")) for s in signal_rows]
        if (
            signal_source.issue
            or len(set(actual)) != len(actual)
            or sorted(ids) != sorted(actual)
            or len(ids) != _number(row.get("signal_count"))
            or _bool(row.get("pattern_member")) is not bool(ids)
        ):
            raise ValueError("Signal receipt mismatch")
        positive = []
        for signal in signal_rows:
            if (
                _date(signal.get("as_of_date")) != bundle.session
                or _date(signal.get("signal_date")) is None
                or _date(signal["signal_date"]) > bundle.session
                or signal.get("lane_policy_version") != row.get("source_policy_version")
            ):
                raise ValueError("Signal cutoff mismatch")
            start = _date(signal.get("pattern_start"))
            if start is None:
                raise ValueError("Missing signal window")
            touched = market.loc[pd.to_datetime(market.timestamp).dt.date >= start]
            factors = pd.to_numeric(touched.adjustment_factor, errors="coerce")
            if factors.dropna().ne(1).any():
                raise ValueError("Unverified signal price basis after corporate action")
            positive.append(
                PatternSignal(
                    str(signal["signal_id"]),
                    str(signal.get("pattern_family", "")),
                    str(signal.get("pattern_state", "")),
                    str(signal.get("signal_direction", "")),
                    str(signal.get("r1a_evidence_class", "")),
                )
            )
        stamp = _stamp(
            bundle,
            key,
            source,
            _date(row.get("session_date")),
            str(row.get("source_policy_version", "")),
        )
        # Bind both assessment and linked signal bytes and publication cutoffs.
        stamp = EvidenceStamp(
            *key,
            stamp.session,
            max(stamp.available_at, utc(signal_source.metadata["available_at"])),
            digest_bytes(
                (
                    stamp.evidence_hash + str(signal_source.metadata["content_hash"])
                ).encode()
            ),
            stamp.policy_version,
        )
        return (
            PatternEvidence(
                stamp,
                str(row.get("pattern_evaluation_state", "")),
                str(row.get("lane_freshness", "")),
                tuple(positive),
            ),
            signal_rows,
            issues,
        )
    except (ValueError, TypeError, KeyError) as exc:
        issues.append(f"PATTERN_LINK_ERROR:{exc}")
        return (
            PatternEvidence(
                _stamp(
                    bundle,
                    key,
                    source,
                    _date(row.get("session_date")),
                    str(row.get("source_policy_version", "")),
                ),
                "ERROR",
                "UNKNOWN",
            ),
            [],
            issues,
        )


def _fundamental(bundle, key, source, rows):
    if source.issue or len(rows) != 1:
        return None, [
            source.issue
            or ("FUNDAMENTAL_MISSING" if not rows else "FUNDAMENTAL_DUPLICATE_ROWS")
        ]
    row = rows[0]
    try:
        blockers = tuple(str(v) for v in _list(row.get("admission_blockers_json", "")))
    except (ValueError, TypeError):
        return None, ["FUNDAMENTAL_BLOCKERS_MALFORMED"]
    return FundamentalEvidence(
        _stamp(
            bundle,
            key,
            source,
            _date(row.get("as_of")),
            str(row.get("admission_version", "")),
        ),
        str(row.get("classification_status", "")),
        str(row.get("primary_thesis", "")),
        _bool(row.get("admission_eligible")),
        blockers,
        str(row.get("statement_basis", "")),
        _date(row.get("source_available_at")),
        str(row.get("source_data_hash", "")),
    ), []


def _setup(bundle, key, pattern, signals, close):
    if pattern is None or pattern.evaluation_state == "ERROR":
        return None, {}
    current = pattern.stamp.session == bundle.session and pattern.freshness == "FRESH"
    if not current:
        return None, {}
    qualifying = [
        s
        for s in signals
        if str(s.get("pattern_family")) in POLICY.pattern_families
        and s.get("signal_direction") == "bullish"
        and s.get("pattern_state") in ("confirmed", "watchlist")
        and s.get("r1a_evidence_class") in POLICY.pattern_evidence_classes
    ]
    stamp = EvidenceStamp(
        *key,
        bundle.session,
        max(pattern.stamp.available_at, bundle.decision_at),
        digest_bytes(
            (pattern.stamp.evidence_hash + bundle.snapshot_hashes["market"]).encode()
        ),
        ADAPTER_VERSION,
    )
    if not qualifying:
        return (
            (SetupEvidence(stamp, "NONE"), {})
            if pattern.evaluation_state == "NONE"
            else (None, {})
        )
    chosen = sorted(
        qualifying,
        key=lambda s: (
            s.get("pattern_state") != "confirmed",
            -(_number(s.get("pattern_priority_score")) or 0),
            -(_number(s.get("pattern_score")) or 0),
            str(s["signal_id"]),
        ),
    )[0]
    trigger = _number(
        chosen.get(
            "breakout_level"
            if chosen["pattern_state"] == "confirmed"
            else "watchlist_trigger_level"
        )
    )
    extension = (
        max(0.0, (close / trigger - 1) * 100)
        if close is not None and trigger is not None and trigger > 0
        else None
    )
    risk = (
        "UNKNOWN"
        if extension is None
        else "LOW"
        if extension <= ADAPTER_POLICY["extension_low_max_pct"]
        else "MEDIUM"
        if extension <= ADAPTER_POLICY["extension_medium_max_pct"]
        else "HIGH"
    )
    blockers = (
        ("BEARISH_SUPPRESSION_PRESENT",)
        if any(
            s.get("signal_direction") == "bearish"
            or s.get("r1a_evidence_class") == "suppression_only"
            for s in signals
        )
        else ()
    )
    return SetupEvidence(
        stamp,
        str(chosen["pattern_state"]),
        trigger,
        _number(chosen.get("invalidation_price")),
        risk,
        blockers,
    ), {
        "setup_signal_id": chosen["signal_id"],
        "trigger": trigger,
        "invalidation": _number(chosen.get("invalidation_price")),
        "extension_pct": extension,
        "extension_policy": ADAPTER_POLICY["extension_policy"],
    }


@dataclass
class ReviewProjection:
    universe: list[dict]
    ordered: list[dict]
    summary: dict
    inputs: tuple[ReviewInput, ...]


def build_review_projection(bundle: ReviewSourceBundle) -> ReviewProjection:
    grouped, rejected = {}, []
    for name, source in bundle.sources.items():
        grouped[name], errors = _groups(source)
        rejected.extend({"source": name, **r} for r in errors)
    markets = {
        (str(e), str(s)): f
        for (e, s), f in bundle.market.groupby(["exchange", "symbol_id"], sort=True)
    }
    masters = {
        (str(e), str(s)): f
        for (e, s), f in bundle.master.groupby(["exchange", "symbol_id"], sort=True)
    }
    keys = set(markets) | set(masters)
    for group in grouped.values():
        keys.update(group)
    calendar, calendar_report = calendar_context(bundle)
    recent = bundle.market.loc[
        pd.to_datetime(bundle.market.timestamp).dt.date.eq(bundle.session)
    ].copy()
    duplicate_keys = recent.duplicated(["exchange", "symbol_id"], keep=False)
    recent = recent.loc[~duplicate_keys].copy()
    turnover = pd.to_numeric(recent.close, errors="coerce") * pd.to_numeric(
        recent.volume, errors="coerce"
    )
    recent["liquidity"] = turnover.where(np.isfinite(turnover) & turnover.ge(0)).rank(
        pct=True, method="average"
    )
    liquidity = {
        (str(r.exchange), str(r.symbol_id)): _number(r.liquidity)
        for r in recent.itertuples()
    }
    dq_ok = bool(bundle.dq_rows) and not any(
        r.get("status") == "failed" and r.get("band") in ("red_block", "red_repairable")
        for r in bundle.dq_rows
    )
    quarantines = {
        (str(e), str(s)): f
        for (e, s), f in bundle.quarantine.groupby(
            ["exchange", "symbol_id"], sort=False
        )
    }
    inputs, details = [], {}
    for key in sorted(keys):
        market = markets.get(key, bundle.market.iloc[:0])
        info, issues = _market_context(
            market,
            bundle,
            key,
            calendar,
            quarantines.get(key, bundle.quarantine.iloc[:0]),
        )
        stage_rows = grouped["stage"].get(key, [])
        governed = bundle.governed.get(key, {})
        governance_ok = bool(
            len(stage_rows) == 1
            and governed
            and not bundle.governance_issue
        )
        # The correction-aware terminal payload owns stage semantics. The
        # promoted producer row proves the listing was in the declared source
        # population, but its pre-correction hash need not equal the terminal
        # governed observation hash.
        stage = governed if governance_ok else {}
        if not governance_ok:
            issues.append("STAGE_GOVERNANCE_MISSING_OR_MISMATCH")
        stage_date = _date(stage.get("source_week_end"))
        age = (
            sum(d > stage_date for d in calendar)
            if stage_date is not None and calendar and stage_date >= calendar[0]
            else None
        )
        label = {
            "stage_1_basing": "S1",
            "stage_2_advancing": "S2",
            "stage_3_topping": "S3",
            "stage_4_declining": "S4",
            "transition_1_to_2": "S2",
            "transition_4_to_1": "S1",
        }.get(str(stage.get("effective_stage", "")), "UNKNOWN")
        stage_names = {
            "stage_1_basing": "S1",
            "stage_2_advancing": "S2",
            "stage_3_topping": "S3",
            "stage_4_declining": "S4",
        }
        transition_aliases = {
            f"{old}_to_{new}".upper(): f"{left}_TO_{right}"
            for old, left in stage_names.items()
            for new, right in stage_names.items()
            if left != right
        }
        transition_raw = str(stage.get("stage_transition", "NONE")).upper()
        transition_raw = transition_aliases.get(transition_raw, transition_raw)
        transition = (
            transition_raw
            if transition_raw
            in ("NONE", "S1_TO_S2", "S4_TO_S1", "S2_TO_S3", "S3_TO_S4")
            else "NONE"
            if transition_raw in ("", "NO_CHANGE")
            else transition_raw
        )
        master = masters.get(key, bundle.master.iloc[:0])
        identity = (
            len(master) == 1
            and str(master.iloc[0]["isin"]).startswith("IN")
            and len(str(master.iloc[0]["isin"])) == 12
            and str(master.iloc[0].instrument_type).upper() in ("EQ", "EQUITY")
        )
        if len(master) == 1:
            updated = master.iloc[0].get("last_updated")
            if updated and not pd.isna(updated):
                try:
                    if utc(updated) > bundle.decision_at:
                        identity = False
                        issues.append("MASTER_VERSION_AFTER_CUTOFF")
                except (ValueError, TypeError):
                    identity = False
                    issues.append("MASTER_AVAILABILITY_INVALID")
        if not calendar_report.get("full_metric_window_calendar_verified"):
            info["late_base"] = None
            if label == "S1":
                issues.append("LATE_BASE_CALENDAR_HISTORY_UNVERIFIED")
        p, signals, pi = _pattern(
            bundle,
            key,
            bundle.sources["pattern"],
            grouped["pattern"].get(key, []),
            bundle.sources["signals"],
            grouped["signals"].get(key, []),
            market,
        )
        f, fi = _fundamental(
            bundle,
            key,
            bundle.sources["fundamental"],
            grouped["fundamental"].get(key, []),
        )
        setup, setup_details = _setup(bundle, key, p, signals, info["close"])
        rank_rows = grouped["rank"].get(key, [])
        rank = None
        if len(rank_rows) == 1:
            r = rank_rows[0]
            score = _number(r.get("composite_score_adjusted"))
            rank = RankEvidence(
                _stamp(
                    bundle,
                    key,
                    bundle.sources["rank"],
                    _date(r.get("timestamp")),
                    "upstream-rank-context",
                ),
                score if score is not None else _number(r.get("composite_score")),
            )
        pos = grouped["positions"].get(key, [])
        valid_pos = [
            r
            for r in pos
            if str(r.get("position_cycle_id", ""))
            and _date(r.get("as_of")) == bundle.session
        ]
        market_ok = (
            not any(
                s in issues
                for s in (
                    "MARKET_MISSING",
                    "DUPLICATE_MARKET_SESSIONS",
                    "LATEST_MARKET_SESSION_MISSING",
                    "INCOMPLETE_ADJUSTED_PRICE_BASIS",
                    "ADJUSTMENT_FACTOR_MISMATCH",
                    "INVALID_MARKET_BARS",
                    "MARKET_PROVENANCE_UNTRUSTED",
                    "MARKET_VERSION_AFTER_CUTOFF",
                    "UNRESOLVED_MARKET_QUARANTINE",
                    "EXCHANGE_CALENDAR_UNVERIFIED",
                    "MISSING_REFERENCE_MARKET_SESSIONS",
                )
            )
            and dq_ok
        )
        row = ReviewInput(
            *key,
            bundle.session,
            bundle.decision_at,
            UniverseEvidence(
                _stamp(bundle, key, None, bundle.session, ADAPTER_VERSION),
                _stamp(
                    bundle,
                    key,
                    bundle.sources["stage"],
                    stage_date,
                    str(stage.get("classifier_version", "")),
                ),
                label,
                age,
                info["history_bars"],
                info["close"],
                liquidity.get(key),
                identity,
                market_ok,
                governance_ok,
                stage_status=str(stage.get("stage_status", "UNKNOWN")),
                transition=transition,
                late_base=info["late_base"],
                technical_stage2=info["technical_stage2"],
            ),
            p,
            f,
            setup,
            rank,
            bool(valid_pos),
        )
        inputs.append(row)
        details[key] = {
            **setup_details,
            "adapter_reasons_json": json_text(sorted(set(issues + pi + fi))),
            "position_cycle_ids_json": json_text(
                sorted(str(r["position_cycle_id"]) for r in valid_pos)
            ),
            "position_scope": "scan_router_pre_execution" if valid_pos else "none",
            "liquidity_percentile": liquidity.get(key),
            "close": info["close"],
            "late_base_metrics_json": json_text(asdict(info["late_base"]))
            if info["late_base"]
            else "null",
            "pattern_details_json": json_text([asdict(s) for s in p.signals])
            if p
            else "[]",
            "fundamental_thesis": f.primary_thesis if f else None,
            "rank_source_session": str(rank.stamp.session) if rank else None,
        }
    universe = []
    for row in inputs:
        result = evaluate_review(row)
        payload = asdict(result)
        for name in ("pattern", "fundamental"):
            value = payload.pop(name)
            payload.update(
                {
                    f"{name}_{k}": json_text(v) if k == "reasons" else v
                    for k, v in value.items()
                }
            )
        payload["reasons_json"] = json_text(payload.pop("reasons"))
        payload.update(details[(row.exchange, row.symbol)])
        payload["review_priority"] = None
        payload["adapter_policy_version"] = ADAPTER_VERSION
        universe.append(payload)
    by_key = {(r["exchange"], r["symbol"]): r for r in universe}
    ordered = []
    for i, decision in enumerate(ordered_review_list(inputs), 1):
        payload = by_key[(decision.exchange, decision.symbol)]
        payload["review_priority"] = i
        ordered.append(payload)
    pos_rows = grouped["positions"]
    expected_cycles = {
        str(r.get("position_cycle_id", ""))
        for rows in pos_rows.values()
        for r in rows
        if str(r.get("position_cycle_id", ""))
    }
    observed_cycles = {
        cycle for r in universe for cycle in json.loads(r["position_cycle_ids_json"])
    }
    missing_cycles = sorted(expected_cycles - observed_cycles)
    exceptions = sum(
        r["universe_state"] == "EVIDENCE_EXCEPTION"
        or (r["selected"] and r["readiness"] == "EVIDENCE_EXCEPTION")
        for r in universe
    )
    source_issues = {k: v.issue for k, v in bundle.sources.items() if v.issue}
    summary = {
        "schema_version": "final-review-artifacts-v1",
        "policy_snapshot": policy_snapshot().metadata(),
        "session": bundle.session.isoformat(),
        "decision_at": bundle.decision_at.isoformat(),
        "source_vintage": ADAPTER_POLICY["source_vintage"],
        "shadow_only": True,
        "status": "degraded"
        if source_issues
        or exceptions
        or rejected
        or missing_cycles
        or not calendar_report.get("full_metric_window_calendar_verified")
        else "complete",
        "universe_rows": len(universe),
        "ordered_rows": len(ordered),
        "selected_rows": sum(r["selected"] for r in universe),
        "evidence_exception_rows": exceptions,
        "universe_states": dict(Counter(r["universe_state"] for r in universe)),
        "lane_combinations_selected": dict(
            Counter(r["combination"] for r in universe if r["selected"])
        ),
        "readiness_selected": dict(
            Counter(r["readiness"] for r in universe if r["selected"])
        ),
        "pattern_states": dict(Counter(r["pattern_state"] for r in universe)),
        "fundamental_states": dict(Counter(r["fundamental_state"] for r in universe)),
        "source_rows": {k: len(v.frame) for k, v in bundle.sources.items()},
        "source_issues": source_issues,
        "source_metadata": {k: v.metadata for k, v in bundle.sources.items()},
        "snapshot_hashes": bundle.snapshot_hashes,
        "snapshot_issues": bundle.snapshot_issues,
        "calendar": calendar_report,
        "dq_accepted": dq_ok,
        "identity_rejections": rejected,
        "position_scope": "registered_scan_router_pre_execution_cycles",
        "position_expected_cycles": len(expected_cycles),
        "position_represented_cycles": len(observed_cycles),
        "position_missing_cycles": missing_cycles,
        "position_coverage_known": not bundle.sources["positions"].issue,
        "truncated_rows": 0,
        "execution_or_lifecycle_authority_changed": False,
    }
    summary["decision_content_hash"] = digest_bytes(json_text(universe).encode())
    return ReviewProjection(universe, ordered, summary, tuple(inputs))
