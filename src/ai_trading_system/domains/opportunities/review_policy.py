"""U1 review-only policy. No I/O, registry writes, or operational consumers.

Inputs are normalized, listing-bound evidence, not raw CSV rows. U2 adapters
must validate promoted-attempt lineage and calculate exchange-session ages.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from hashlib import sha256
import json
from math import isfinite
from typing import Iterable


@dataclass(frozen=True)
class ReviewPolicy:
    version: str = "stage-universe-review-v1"
    min_history_bars: int = 180
    min_close: float = 20.0
    min_liquidity_percentile: float = 0.20
    max_stage_age_sessions: int = 10
    max_fundamental_age_days: int = 550
    stage_policy: str = "weekly-stage-v2"
    pattern_policy: str = "pattern-lane-r0-policy-v1"
    fundamental_policy: str = "fundamental-thesis-admission-v1.1"
    # Bounds use percentage points except ratios explicitly named as such.
    late_base_bounds: tuple[tuple[str, float, float | None], ...] = (
        ("close_to_sma150_ratio", 0.85, 1.15),
        ("sma150_slope_pct", -2.0, 2.0),
        ("base_depth_pct", 0.0, 35.0),
        ("range_contraction_ratio", 0.0, 0.90),
        ("pivot_distance_pct", 0.0, 10.0),
        ("return_delta_pct", 0.0, None),
        ("volume_dry_up_ratio", 0.0, 0.90),
        ("close_to_sma200_ratio", 0.85, None),
        ("sma200_slope_pct", -1.0, None),
    )
    pattern_families: tuple[str, ...] = (
        "cup_handle",
        "round_bottom",
        "double_bottom",
        "flag",
        "high_tight_flag",
        "ascending_triangle",
        "symmetrical_triangle",
        "ascending_base",
        "vcp",
        "flat_base",
        "stage2_reclaim",
        "darvas_box",
        "pocket_pivot",
        "inside_week_breakout",
        "three_weeks_tight",
        "inside_day",
    )
    pattern_evidence_classes: tuple[str, ...] = (
        "evidence_supported",
        "evidence_supported_smaller_sample",
        "evidence_supported_low_volume",
        "observational",
        "negative_evidence",
        "insufficient_evidence",
    )
    thesis_families: tuple[str, ...] = (
        "QUALITY_COMPOUNDER",
        "HIGH_GROWTH_EMERGING",
        "EARNINGS_ACCELERATION",
        "UNDERVALUED_QUALITY",
        "CASHFLOW_BALANCE_SHEET_INFLECTION",
        "TURNAROUND_CYCLICAL_RECOVERY",
        "CAPITAL_RETURN_INCOME",
    )

    @property
    def content_hash(self) -> str:
        return sha256(
            json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


POLICY = ReviewPolicy()


@dataclass(frozen=True)
class EvidenceStamp:
    exchange: str
    symbol: str
    session: date
    available_at: datetime
    evidence_hash: str
    policy_version: str


@dataclass(frozen=True)
class LateBaseMetrics:
    close_to_sma150_ratio: float | None = None
    sma150_slope_pct: float | None = None
    base_depth_pct: float | None = None
    range_contraction_ratio: float | None = None
    pivot_distance_pct: float | None = None
    return_delta_pct: float | None = None
    volume_dry_up_ratio: float | None = None
    close_to_sma200_ratio: float | None = None
    sma200_slope_pct: float | None = None


@dataclass(frozen=True)
class UniverseEvidence:
    market_stamp: EvidenceStamp
    stage_stamp: EvidenceStamp
    stage: str
    stage_age_sessions: int | None
    history_bars: int
    close: float | None
    liquidity_percentile: float | None
    identity_verified: bool | None
    market_trusted: bool | None
    governance_resolved: bool | None
    stage_status: str = "UNKNOWN"
    transition: str = "NONE"
    late_base: LateBaseMetrics | None = None
    technical_stage2: bool | None = None


@dataclass(frozen=True)
class PatternSignal:
    signal_id: str
    family: str
    state: str
    direction: str
    evidence_class: str


@dataclass(frozen=True)
class PatternEvidence:
    stamp: EvidenceStamp
    evaluation_state: str
    freshness: str
    signals: tuple[PatternSignal, ...] = ()


@dataclass(frozen=True)
class FundamentalEvidence:
    stamp: EvidenceStamp
    classification: str
    primary_thesis: str | None
    admission_eligible: bool | None
    blockers: tuple[str, ...]
    statement_basis: str
    source_available_date: date | None
    source_data_hash: str


@dataclass(frozen=True)
class SetupEvidence:
    stamp: EvidenceStamp
    state: str  # NONE, WATCHLIST, CONFIRMED; never a canonical lifecycle state.
    trigger: float | None = None
    invalidation: float | None = None
    extension_risk: str = "UNKNOWN"
    blockers: tuple[str, ...] = ()


@dataclass(frozen=True)
class RankEvidence:
    stamp: EvidenceStamp
    score: float | None


@dataclass(frozen=True)
class ReviewInput:
    exchange: str
    symbol: str
    session: date
    decision_at: datetime
    universe: UniverseEvidence
    pattern: PatternEvidence | None = None
    fundamental: FundamentalEvidence | None = None
    setup: SetupEvidence | None = None
    rank: RankEvidence | None = None
    has_open_position: bool = False


@dataclass(frozen=True)
class LaneDecision:
    state: str
    qualified: bool
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReviewDecision:
    exchange: str
    symbol: str
    session: date
    stage: str
    stage_status: str
    transition: str
    universe_state: str
    selected: bool
    combination: str
    readiness: str
    pattern: LaneDecision
    fundamental: LaneDecision
    rank_score: float | None
    position_attention_required: bool
    reasons: tuple[str, ...]
    policy_version: str = POLICY.version
    policy_hash: str = POLICY.content_hash


def _number(value: object) -> bool:
    return type(value) in (int, float) and isfinite(value)


def _stamp_issue(
    row: ReviewInput, stamp: EvidenceStamp, *, current: bool = True
) -> str | None:
    if (stamp.exchange, stamp.symbol) != (row.exchange, row.symbol):
        return "IDENTITY_MISMATCH"
    if not stamp.evidence_hash or not stamp.policy_version:
        return "MISSING_LINEAGE"
    if stamp.available_at.tzinfo is None or stamp.available_at.utcoffset() is None:
        return "UNKNOWN_AVAILABILITY"
    if stamp.available_at > row.decision_at or stamp.session > row.session:
        return "FUTURE_EVIDENCE"
    if current and stamp.session != row.session:
        return "STALE_EVIDENCE"
    return None


def _universe(row: ReviewInput) -> tuple[str, list[str]]:
    u = row.universe
    issues = []
    for prefix, stamp, current in (
        ("MARKET", u.market_stamp, True),
        ("STAGE", u.stage_stamp, False),
    ):
        issue = _stamp_issue(row, stamp, current=current)
        if issue:
            issues.append(f"{prefix}_{issue}")
    for name, value in (
        ("IDENTITY", u.identity_verified),
        ("TRUST", u.market_trusted),
        ("GOVERNANCE", u.governance_resolved),
    ):
        if value is not True:
            issues.append(f"{name}_NOT_VERIFIED")
    if u.stage_stamp.policy_version != POLICY.stage_policy:
        issues.append("UNSUPPORTED_STAGE_POLICY")
    if (
        type(u.stage_age_sessions) is not int
        or not 0 <= u.stage_age_sessions <= POLICY.max_stage_age_sessions
    ):
        issues.append("STAGE_AGE_UNUSABLE")
    if u.stage_status not in ("locked", "provisional"):
        issues.append("UNKNOWN_STAGE_STATUS")
    if u.stage not in ("S1", "S2", "S3", "S4"):
        issues.append("UNKNOWN_STAGE")
    if u.transition not in ("NONE", "S1_TO_S2", "S4_TO_S1", "S2_TO_S3", "S3_TO_S4"):
        issues.append("UNKNOWN_TRANSITION")
    elif u.transition != "NONE" and u.transition.split("_TO_")[1] != u.stage:
        issues.append("STAGE_TRANSITION_CONFLICT")
    if not _number(u.close) or u.close <= 0:
        issues.append("INVALID_CLOSE")
    if not _number(u.liquidity_percentile) or not 0 <= u.liquidity_percentile <= 1:
        issues.append("LIQUIDITY_UNKNOWN")
    if type(u.history_bars) is not int or u.history_bars < 0:
        issues.append("INVALID_HISTORY_COUNT")
    if issues:
        return "EVIDENCE_EXCEPTION", issues
    if u.history_bars < POLICY.min_history_bars:
        return "EXCLUDED", ["INSUFFICIENT_STRUCTURAL_HISTORY"]
    if (
        u.close < POLICY.min_close
        or u.liquidity_percentile < POLICY.min_liquidity_percentile
    ):
        return "EXCLUDED", ["LIQUIDITY_FAILED"]
    if u.stage not in ("S1", "S2"):
        return "EXCLUDED", ["STAGE_OUTSIDE_REVIEW_UNIVERSE"]
    if u.stage == "S2":
        return "ELIGIBLE", ["GOVERNED_STAGE2"]
    if u.late_base is None:
        return "EVIDENCE_EXCEPTION", ["LATE_BASE_METRICS_MISSING"]
    metrics = asdict(u.late_base)
    missing = [name for name, value in metrics.items() if not _number(value)]
    if missing:
        return "EVIDENCE_EXCEPTION", [
            f"LATE_BASE_MISSING_{name.upper()}" for name in missing
        ]
    failed = [
        name
        for name, low, high in POLICY.late_base_bounds
        if metrics[name] < low or (high is not None and metrics[name] > high)
    ]
    if failed:
        return "EXCLUDED", [f"LATE_BASE_FAILED_{name.upper()}" for name in failed]
    return "ELIGIBLE", ["LATE_STAGE1_STRUCTURE_PASS"]


def _pattern(row: ReviewInput) -> LaneDecision:
    p = row.pattern
    if p is None:
        return LaneDecision("MISSING", False)
    issue = _stamp_issue(row, p.stamp)
    if issue:
        return LaneDecision(issue, False)
    if p.stamp.policy_version != POLICY.pattern_policy:
        return LaneDecision("UNSUPPORTED_POLICY", False)
    if p.freshness != "FRESH":
        return LaneDecision("UNUSABLE_FRESHNESS", False, (p.freshness,))
    if p.evaluation_state not in ("KNOWN", "NONE", "NOT_ELIGIBLE", "ERROR"):
        return LaneDecision("ERROR", False, ("UNKNOWN_PATTERN_STATE",))
    ids = [s.signal_id for s in p.signals]
    if len(set(ids)) != len(ids) or any(not value for value in ids):
        return LaneDecision("ERROR", False, ("INVALID_SIGNAL_IDENTITIES",))
    if p.evaluation_state != "KNOWN":
        if p.signals:
            return LaneDecision("ERROR", False, ("SIGNALS_CONTRADICT_ASSESSMENT",))
        return LaneDecision(p.evaluation_state, False)
    if not p.signals:
        return LaneDecision("ERROR", False, ("KNOWN_WITHOUT_SIGNALS",))
    positive = any(
        s.family in POLICY.pattern_families
        and s.state in ("watchlist", "confirmed")
        and s.direction == "bullish"
        and s.evidence_class in POLICY.pattern_evidence_classes
        for s in p.signals
    )
    annotations = tuple(
        sorted(
            {f"PATTERN_EVIDENCE_CLASS_{s.evidence_class.upper()}" for s in p.signals}
        )
    )
    return LaneDecision(
        "QUALIFIED" if positive else "NO_POSITIVE_SETUP", positive, annotations
    )


def _fundamental(row: ReviewInput) -> LaneDecision:
    f = row.fundamental
    if f is None:
        return LaneDecision("MISSING", False)
    issue = _stamp_issue(row, f.stamp)
    if issue:
        return LaneDecision(issue, False)
    if f.stamp.policy_version != POLICY.fundamental_policy:
        return LaneDecision("UNSUPPORTED_POLICY", False)
    if (
        f.statement_basis not in ("standalone", "consolidated")
        or not f.source_data_hash
    ):
        return LaneDecision("INVALID_SOURCE", False)
    if f.source_available_date is None:
        return LaneDecision("MISSING_SOURCE_DATE", False)
    age = (row.session - f.source_available_date).days
    if age < 0:
        return LaneDecision("FUTURE_EVIDENCE", False)
    if age > POLICY.max_fundamental_age_days:
        return LaneDecision("STALE_SOURCE", False)
    if f.classification != "QUALIFIED":
        return LaneDecision(f.classification or "UNKNOWN", False, f.blockers)
    if f.primary_thesis not in POLICY.thesis_families:
        return LaneDecision("UNKNOWN_THESIS", False)
    if f.admission_eligible is not True or f.blockers:
        return LaneDecision("DAILY_CONTEXT_BLOCKED", False, f.blockers)
    return LaneDecision("QUALIFIED", True)


def _readiness(row: ReviewInput) -> tuple[str, tuple[str, ...]]:
    s = row.setup
    if s is None:
        return "EVIDENCE_EXCEPTION", ("SETUP_NOT_EVALUATED",)
    issue = _stamp_issue(row, s.stamp)
    if issue:
        return "EVIDENCE_EXCEPTION", (f"SETUP_{issue}",)
    if s.blockers:
        return "DEFER", s.blockers
    if s.state == "NONE":
        return "DEVELOPING_WATCH", ("AWAITING_TECHNICAL_SETUP",)
    if s.state not in ("watchlist", "confirmed"):
        return "EVIDENCE_EXCEPTION", ("UNKNOWN_SETUP_STATE",)
    if (
        not _number(s.trigger)
        or not _number(s.invalidation)
        or not 0 < s.invalidation < s.trigger
    ):
        return "EVIDENCE_EXCEPTION", ("INVALID_TRIGGER_OR_INVALIDATION",)
    if row.universe.close <= s.invalidation:
        return "DEFER", ("SETUP_INVALIDATED_AT_CLOSE",)
    if s.extension_risk in ("MEDIUM", "HIGH"):
        return "DEFER", ("EXTENSION_NOT_LOW",)
    if s.extension_risk != "LOW":
        return "EVIDENCE_EXCEPTION", ("EXTENSION_UNKNOWN",)
    if row.universe.technical_stage2 is False and row.universe.stage == "S2":
        return "DEFER", ("GOVERNED_TECHNICAL_STAGE_DISAGREEMENT",)
    if s.state == "watchlist" or row.universe.close < s.trigger:
        return "DEVELOPING_WATCH", ("AWAITING_TRIGGER",)
    return "SETUP_REVIEW", ("FRESH_CONFIRMED_SETUP_LOW_EXTENSION",)


def evaluate_review(row: ReviewInput) -> ReviewDecision:
    """Evaluate one listing; successful qualification cannot authorize an order."""
    if (
        row.exchange not in ("NSE", "BSE")
        or not row.symbol
        or row.symbol != row.symbol.strip().upper()
    ):
        raise ValueError("Review input requires explicit NSE/BSE and canonical symbol")
    if row.decision_at.tzinfo is None or row.decision_at.utcoffset() is None:
        raise ValueError("Review decision timestamp must be timezone-aware")
    if row.decision_at.date() < row.session:
        raise ValueError("Decision cannot precede the evidence session")
    if type(row.has_open_position) is not bool:
        raise ValueError("Position coverage flag must be an explicit boolean")
    universe, reasons = _universe(row)
    if row.universe.technical_stage2 is True and row.universe.stage != "S2":
        reasons.append("TECHNICAL_STAGE2_DIFFERS_FROM_GOVERNED_STAGE")
    p, f = _pattern(row), _fundamental(row)
    selected = universe == "ELIGIBLE" and (p.qualified or f.qualified)
    combination = {
        (True, True): "P+F",
        (True, False): "P_ONLY",
        (False, True): "F_ONLY",
        (False, False): "NONE",
    }[(p.qualified, f.qualified)]
    readiness = "NOT_SELECTED"
    if selected:
        readiness, setup_reasons = _readiness(row)
        reasons.extend(setup_reasons)
    elif universe == "ELIGIBLE":
        reasons.append("NO_QUALIFYING_LANE")
    score = None
    if row.rank is not None:
        issue = _stamp_issue(row, row.rank.stamp)
        if issue or not _number(row.rank.score):
            reasons.append(f"RANK_{issue or 'INVALID_SCORE'}")
        else:
            score = float(row.rank.score)
    else:
        reasons.append("RANK_MISSING")
    return ReviewDecision(
        row.exchange,
        row.symbol,
        row.session,
        row.universe.stage,
        row.universe.stage_status,
        row.universe.transition,
        universe,
        selected,
        combination,
        readiness,
        p,
        f,
        score,
        row.has_open_position,
        tuple(reasons),
    )


def ordered_review_list(rows: Iterable[ReviewInput]) -> tuple[ReviewDecision, ...]:
    """One session only; duplicate listings fail instead of hiding conflicting rows.

    Qualified evidence exceptions remain auditable via evaluate_review, but have
    no ordinal review priority. There is no outside-universe top-N backfill.
    """
    inputs = tuple(rows)
    if len({row.decision_at for row in inputs}) > 1:
        raise ValueError("Cannot order mixed decision cutoffs")
    decisions = tuple(evaluate_review(row) for row in inputs)
    keys = {(d.exchange, d.symbol) for d in decisions}
    if len(keys) != len(decisions):
        raise ValueError("Duplicate listing in review input")
    if len({d.session for d in decisions}) > 1:
        raise ValueError("Cannot order mixed review sessions")
    groups = {"SETUP_REVIEW": 0, "DEVELOPING_WATCH": 1, "DEFER": 2}
    return tuple(
        sorted(
            (d for d in decisions if d.selected and d.readiness in groups),
            key=lambda d: (
                groups[d.readiness],
                d.rank_score is None,
                -(d.rank_score or 0.0),
                d.exchange,
                d.symbol,
            ),
        )
    )
