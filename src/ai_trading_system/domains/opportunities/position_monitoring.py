"""Pure Phase 3C-3 position coverage, compatibility, and recovery contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, Iterable, Mapping

from ai_trading_system.domains.opportunities.routing import ScanTier

POSITION_COVERAGE_POLICY_VERSION = "position-coverage-policy-v1"
POSITION_COMPATIBILITY_POLICY_VERSION = "position-episode-compatibility-v1"
POSITION_RECOVERY_POLICY_VERSION = "position-recovery-policy-v1"


class PositionCoverageStatus(str, Enum):
    FULLY_MONITORED = "FULLY_MONITORED"
    ROUTED_WITH_INCOMPLETE_DATA = "ROUTED_WITH_INCOMPLETE_DATA"
    MISSING_ROUTING = "MISSING_ROUTING"
    INCOMPATIBLE_EPISODE = "INCOMPATIBLE_EPISODE"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
    HARD_EXCLUSION = "HARD_EXCLUSION"


class PositionEpisodeCompatibility(str, Enum):
    COMPATIBLE = "compatible"
    NO_OPEN_EPISODE = "no_open_episode"
    INCOMPATIBLE_SETUP_FAMILY = "incompatible_setup_family"
    TEMPORAL_MISMATCH = "temporal_mismatch"
    AMBIGUOUS_MULTIPLE_EPISODES = "ambiguous_multiple_episodes"
    CLOSED_EPISODE = "closed_episode"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


class PositionRecoveryMode(str, Enum):
    REPORT_ONLY = "report_only"
    REVIEWED = "reviewed"
    AUTOMATIC = "automatic"


@dataclass(frozen=True, slots=True)
class PositionCoverageRecord:
    position_cycle_id: str
    symbol_id: str
    exchange: str
    position_opened_at: datetime
    quantity: float
    average_price: float | None
    routing_decision_id: str | None
    effective_scan_tier: ScanTier | None
    market_data_available: bool
    market_data_complete: bool
    last_valid_market_timestamp: datetime | None
    expected_market_session: date | None
    market_data_staleness_sessions: int | None
    missing_data_fields: tuple[str, ...]
    investigator_evidence_complete: bool
    opportunity_episode_id: str | None
    episode_match_status: str
    coverage_status: PositionCoverageStatus
    coverage_reasons: tuple[str, ...]
    as_of: datetime
    policy_version: str = POSITION_COVERAGE_POLICY_VERSION

    def as_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["effective_scan_tier"] = (
            self.effective_scan_tier.value if self.effective_scan_tier else None
        )
        row["coverage_status"] = self.coverage_status.value
        return row


@dataclass(frozen=True, slots=True)
class EpisodeCompatibilityResult:
    status: PositionEpisodeCompatibility
    candidate_id: str | None = None
    compatible_candidate_ids: tuple[str, ...] = ()
    open_episode_ids: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()


def make_position_cycle_id(
    *, exchange: str, symbol_id: str, position_opened_at: str | datetime
) -> str:
    opened = (
        position_opened_at.isoformat()
        if isinstance(position_opened_at, datetime)
        else str(position_opened_at)
    )
    value = f"{exchange.strip().upper()}|{symbol_id.strip().upper()}|{opened}"
    return "position-cycle-" + hashlib.sha256(value.encode()).hexdigest()[:24]


def make_recovery_proposal_id(
    *, position_cycle_id: str, symbol_id: str, exchange: str,
    recovery_mode: PositionRecoveryMode,
    policy_version: str = POSITION_RECOVERY_POLICY_VERSION,
) -> str:
    value = "|".join(
        (position_cycle_id, exchange.upper(), symbol_id.upper(), policy_version, recovery_mode.value)
    )
    return "position-recovery-" + hashlib.sha256(value.encode()).hexdigest()[:24]


def evaluate_position_episode_compatibility(
    *,
    position_cycle_id: str,
    position_opened_at: datetime,
    episodes: Iterable[Any],
    current_states: Iterable[Any],
    trigger_alignment_sessions: int = 5,
) -> EpisodeCompatibilityResult:
    """Require temporal/setup evidence; symbol equality is established by the caller."""
    episode_list = tuple(episodes)
    open_episodes = tuple(
        episode for episode in episode_list
        if str(getattr(getattr(episode, "episode_status", None), "value", "")).upper() == "OPEN"
    )
    if not open_episodes:
        if episode_list:
            return EpisodeCompatibilityResult(
                PositionEpisodeCompatibility.CLOSED_EPISODE,
                open_episode_ids=(),
                reasons=("only closed episodes exist; automatic reopen is prohibited",),
            )
        return EpisodeCompatibilityResult(PositionEpisodeCompatibility.NO_OPEN_EPISODE)
    state_by_id = {state.candidate_id: state for state in current_states}
    compatible: list[str] = []
    rejection_reasons: list[str] = []
    for episode in open_episodes:
        family = str(getattr(episode, "setup_family", ""))
        state = state_by_id.get(episode.candidate_id)
        if family == "position_state_recovery":
            identity = str(getattr(episode, "admission_identity", ""))
            if position_cycle_id in identity:
                compatible.append(episode.candidate_id)
            else:
                rejection_reasons.append(f"{episode.candidate_id}: recovery identity differs")
            continue
        if state is None or state.last_transition_at is None:
            rejection_reasons.append(f"{episode.candidate_id}: trigger timing unavailable")
            continue
        lifecycle = str(state.current_lifecycle_state or "")
        if lifecycle not in {"triggered", "pending_followthrough", "confirmed", "advancing", "weakening"}:
            rejection_reasons.append(f"{episode.candidate_id}: lifecycle is not position-compatible")
            continue
        delta = abs((state.last_transition_at.date() - position_opened_at.date()).days)
        if delta > trigger_alignment_sessions:
            rejection_reasons.append(f"{episode.candidate_id}: trigger timing outside compatibility window")
            continue
        if episode.episode_started_at > position_opened_at:
            rejection_reasons.append(f"{episode.candidate_id}: episode starts after position cycle")
            continue
        compatible.append(episode.candidate_id)
    open_ids = tuple(sorted(episode.candidate_id for episode in open_episodes))
    if len(compatible) == 1 and len(open_episodes) == 1:
        return EpisodeCompatibilityResult(
            PositionEpisodeCompatibility.COMPATIBLE,
            compatible[0], tuple(compatible), open_ids,
            ("setup lifecycle and trigger timing align with the position cycle",),
        )
    if len(open_episodes) > 1:
        return EpisodeCompatibilityResult(
            PositionEpisodeCompatibility.AMBIGUOUS_MULTIPLE_EPISODES,
            compatible_candidate_ids=tuple(sorted(compatible)),
            open_episode_ids=open_ids,
            reasons=tuple(rejection_reasons) or ("multiple open episodes have competing claims",),
        )
    status = (
        PositionEpisodeCompatibility.TEMPORAL_MISMATCH
        if any(
            "outside compatibility window" in reason or "starts after" in reason
            for reason in rejection_reasons
        )
        else PositionEpisodeCompatibility.INSUFFICIENT_EVIDENCE
    )
    return EpisodeCompatibilityResult(
        status, open_episode_ids=open_ids,
        reasons=tuple(rejection_reasons) or ("same symbol alone is insufficient",),
    )


def recovery_payload_hash(payload: Mapping[str, Any]) -> str:
    """Hash the durable recovery decision, excluding per-run provenance.

    Recovery proposal IDs are stable for a position cycle and policy.  The run
    that observed the proposal and its source artifact lineage are audit
    metadata, so including them would turn a semantically identical replay into
    an idempotency conflict.
    """
    semantic_payload = {
        key: value
        for key, value in payload.items()
        if key not in {"payload_hash", "created_run_id", "source_lineage"}
    }
    return hashlib.sha256(
        json.dumps(semantic_payload, sort_keys=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()
