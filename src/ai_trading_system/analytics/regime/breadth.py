"""UNIV_TOP1000 market breadth regime snapshots."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import yaml


@dataclass(frozen=True)
class MarketRegimeSnapshot:
    date: str
    regime: str
    raw_regime: str
    pct_above_50dma: float
    pct_above_200dma: float
    pct_near_52w_high: float  # within 10% of 252-day high
    pct_at_52w_high: float    # at the 252-day high (close >= high252) — leadership signal
    universe_count: int  # alias for eligible_200dma_count (kept for backwards compat)
    top1000_above_50dma: bool
    top1000_above_200dma: bool
    # ── Breadth-quality fields ────────────────────────────────────────────
    # eligible_*dma_count = number of symbols with at least N trading days of
    # history on this date. total_symbols_count is every symbol with a close
    # on this date (no history requirement). breadth_confidence is the ratio
    # eligible_200dma_count / total_symbols_count — when this falls (e.g.
    # early years where only 300/1500 symbols have 200 days history) the
    # pct_above_200dma signal becomes structurally noisy and shouldn't be
    # compared with modern-era values.
    eligible_50dma_count: int = 0
    eligible_200dma_count: int = 0
    total_symbols_count: int = 0
    breadth_confidence: float = 0.0
    # ── Derived metrics (Phase 4b) ───────────────────────────────────────
    # regime_score: continuous 0..100 blend of the three breadth metrics so
    # downstream code can read a numeric signal independent of the
    # categorical label. Weights: 200DMA=0.5, at-new-high=0.3, 50DMA=0.2.
    # regime_confidence: 0..1, how far inside the classified band we are
    # relative to the nearest threshold edge. 1.0 = solidly inside,
    # 0.0 = on the boundary (about to flip). Computed by classify_regime
    # itself since it knows the active rule set; default 0 if unset.
    regime_score: float = 0.0
    regime_confidence: float = 0.0
    confirmation_days: int = 3
    source: str = "UNIV_TOP1000"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── Rule-schema validation ─────────────────────────────────────────────────
#
# Every key inside a regime rule block must reference a real metric on the
# snapshot. Numeric metrics support the comparison suffixes _lt/_lte/_gt/_gte;
# boolean metrics accept only the bare key with a literal true/false value.
# Validation runs at load time (load_regime_rules) so rule typos fail fast
# at boot rather than silently classifying every day as the default regime.
_NUMERIC_METRICS: frozenset[str] = frozenset({
    "pct_above_50dma",
    "pct_above_200dma",
    "pct_near_52w_high",
    "pct_at_52w_high",
    "universe_count",
    "eligible_50dma_count",
    "eligible_200dma_count",
    "total_symbols_count",
    "breadth_confidence",
    "regime_score",
    "regime_confidence",
})
_BOOLEAN_METRICS: frozenset[str] = frozenset({
    "top1000_above_50dma",
    "top1000_above_200dma",
})
_COMPARISON_SUFFIXES: tuple[str, ...] = ("_lt", "_lte", "_gt", "_gte")


def _split_rule_key(key: str) -> tuple[str, str | None]:
    """Return ``(metric_name, suffix)`` for a rule key. Suffix is None for bare keys."""
    for suffix in _COMPARISON_SUFFIXES:
        if key.endswith(suffix):
            return key[: -len(suffix)], suffix
    return key, None


def _validate_rule_key(regime: str, key: str, value: Any) -> None:
    """Raise ValueError/TypeError if a rule key/value is malformed."""
    metric, suffix = _split_rule_key(key)
    if metric in _NUMERIC_METRICS:
        if suffix is None:
            raise ValueError(
                f"regime rule '{regime}.{key}': numeric metric '{metric}' "
                f"requires a comparison suffix (one of {_COMPARISON_SUFFIXES})"
            )
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise TypeError(
                f"regime rule '{regime}.{key}': expected numeric value, got "
                f"{type(value).__name__}={value!r}"
            )
        return
    if metric in _BOOLEAN_METRICS:
        if suffix is not None:
            raise TypeError(
                f"regime rule '{regime}.{key}': boolean metric '{metric}' "
                f"cannot be compared with '{suffix}'; use the bare key with "
                f"a true/false value"
            )
        if not isinstance(value, bool):
            raise TypeError(
                f"regime rule '{regime}.{key}': expected bool value, got "
                f"{type(value).__name__}={value!r}"
            )
        return
    raise ValueError(
        f"regime rule '{regime}.{key}': unknown metric '{metric}'. "
        f"Valid numeric metrics: {sorted(_NUMERIC_METRICS)}; "
        f"valid boolean metrics: {sorted(_BOOLEAN_METRICS)}"
    )


# ── Disagreement helpers ──────────────────────────────────────────────────
#
# raw_regime captures "what today's breadth alone implies"; regime (confirmed)
# requires the 3-day-of-the-last-3 hysteresis to flip. The two diverge during
# transitions. We treat a divergence where the raw signal worsened (e.g.
# raw=risk_off while confirmed=bull/strong_bull) as an early warning worth
# surfacing to UI/alerts/execute — the confirmed regime is lagging and
# fresh positions opened today carry that lag risk.
_REGIME_RANK: dict[str, int] = {
    "risk_off": 0,
    "neutral": 1,
    "cautious_bull": 2,
    "bull": 3,
    "strong_bull": 4,
}

# Confirmed-regime values that should NOT have been opening fresh positions
# when raw collapsed to risk_off. This is the "dangerous disagreement" set
# used by alert emission and the optional execute-stage override.
# cautious_bull is included: by design it allows entries (top breakouts only),
# so a raw=risk_off collapse should warn there too.
_DANGEROUS_DISAGREEMENT_CONFIRMED: frozenset[str] = frozenset(
    {"cautious_bull", "bull", "strong_bull"}
)


def regime_disagreement(
    confirmed: str | None, raw: str | None
) -> dict[str, Any]:
    """Return a structured disagreement payload for a (confirmed, raw) pair.

    Keys:
        present:   true when confirmed != raw
        dangerous: true when raw is risk_off and confirmed is bull/strong_bull
                   — i.e. the lagging confirmed signal is opening positions
                   the raw breadth says are unsafe
        direction: "raw_worse" | "raw_better" | "same"
        confirmed, raw: echoed for downstream convenience
    """
    confirmed_s = str(confirmed) if confirmed else ""
    raw_s = str(raw) if raw else ""
    if not confirmed_s or not raw_s or confirmed_s == raw_s:
        return {
            "present": False,
            "dangerous": False,
            "direction": "same",
            "confirmed": confirmed_s,
            "raw": raw_s,
        }
    raw_rank = _REGIME_RANK.get(raw_s)
    confirmed_rank = _REGIME_RANK.get(confirmed_s)
    direction = "same"
    if raw_rank is not None and confirmed_rank is not None:
        direction = "raw_worse" if raw_rank < confirmed_rank else "raw_better"
    return {
        "present": True,
        "dangerous": (
            raw_s == "risk_off" and confirmed_s in _DANGEROUS_DISAGREEMENT_CONFIRMED
        ),
        "direction": direction,
        "confirmed": confirmed_s,
        "raw": raw_s,
    }


def validate_regime_rules(rules: dict[str, Any]) -> None:
    """Walk every regime block and raise on unknown keys / type mismatches.

    Called from ``load_regime_rules`` so a bad config fails the pipeline at
    boot rather than silently mis-classifying days. Recurses into nested
    ``enter:`` and ``exit:`` sub-blocks (Phase 4 hysteresis).
    """
    blocks = rules.get("rules") if isinstance(rules, dict) else None
    if not isinstance(blocks, dict):
        return
    for regime, spec in blocks.items():
        if not isinstance(spec, dict):
            raise TypeError(
                f"regime rule '{regime}': expected mapping, got "
                f"{type(spec).__name__}"
            )
        for key, value in spec.items():
            if key in ("enter", "exit"):
                # Hysteresis sub-block. Recurse so nested keys are validated
                # under the same metric whitelist.
                if not isinstance(value, dict):
                    raise TypeError(
                        f"regime rule '{regime}.{key}': expected mapping, got "
                        f"{type(value).__name__}"
                    )
                for nested_key, nested_value in value.items():
                    _validate_rule_key(
                        f"{regime}.{key}", str(nested_key), nested_value
                    )
                continue
            _validate_rule_key(str(regime), str(key), value)


def resolve_previous_regime(
    registry: Any,
    *,
    exclude_run_id: str | None = None,
) -> str | None:
    """Look up the most recent rank-stage regime classification for hysteresis seeding.

    Reads the last completed rank-stage ``dashboard_payload`` artifact from
    ``registry`` and returns the persisted regime label. Returns None on
    any miss (no prior runs, file missing, JSON malformed, regime field
    absent) — caller should treat None as cold-start.

    Pass the current run's ``run_id`` as ``exclude_run_id`` to avoid
    re-reading the in-progress run's own artifact.
    """
    if registry is None:
        return None
    try:
        artifacts = registry.get_latest_artifact(
            stage_name="rank",
            artifact_type="dashboard_payload",
            limit=1,
            exclude_run_id=exclude_run_id,
        )
    except Exception:
        return None
    if not artifacts:
        return None
    uri = getattr(artifacts[0], "uri", None)
    if not uri:
        return None
    try:
        import json
        with open(uri, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return None
    market_regime = payload.get("market_regime") if isinstance(payload, dict) else None
    if not isinstance(market_regime, dict):
        return None
    regime = market_regime.get("regime")
    if not isinstance(regime, str) or not regime:
        return None
    return regime


def resolve_regime_rules_path(project_root: Path | str, value: str | Path | None = None) -> Path:
    root = Path(project_root)
    if value:
        path = Path(value)
        return path if path.is_absolute() else root / path
    return root / "config" / "active_regime_rules.yaml"


def load_regime_rules(project_root: Path | str, rules_path: str | Path | None = None) -> dict[str, Any]:
    path = resolve_regime_rules_path(project_root, rules_path)
    if not path.exists():
        return {}
    rules = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    validate_regime_rules(rules)
    return rules


def compute_market_regime_snapshot(
    db_path: Path | str,
    *,
    as_of: str | date,
    project_root: Path | str | None = None,
    rules_path: str | Path | None = None,
    index_code: str | None = None,
    exchange: str = "NSE",
    previous_regime: str | None = None,
) -> MarketRegimeSnapshot:
    """Compute the confirmed breadth regime as of ``as_of``.

    Rolling SMA and 52-week-high windows end at the current row, so the query
    uses current and past observations only.

    ``previous_regime`` is the optional hysteresis seed — the classified
    regime of the day immediately preceding the in-window history. When
    provided AND the rules define nested ``exit:`` sub-blocks, the
    classifier prefers staying in the seed regime as long as its exit
    predicates hold (see ``classify_regime``). Cold-start (None) means
    every day in the window classifies under enter-only rules.
    """
    rules_payload = load_regime_rules(project_root or Path("."), rules_path) if project_root is not None else {}
    source_cfg = dict(rules_payload.get("regime_source") or {})
    confirmation_days = int(source_cfg.get("confirmation_days") or 3)
    code = str(index_code or source_cfg.get("index_code") or "UNIV_TOP1000")
    raw_rules = dict(rules_payload.get("rules") or {})
    # Stash priority list (top-level YAML key) into the rules dict under a
    # sentinel so classify_regime can pick it up without an extra param.
    priority = rules_payload.get("priority")
    if isinstance(priority, (list, tuple)):
        raw_rules["__priority__"] = list(priority)

    snapshots = _load_recent_raw_snapshots(
        db_path,
        as_of=str(as_of),
        exchange=exchange,
        index_code=code,
        limit=max(confirmation_days, 1),
        rules=raw_rules,
        previous_regime=previous_regime,
    )
    if not snapshots and project_root is not None:
        research_db = Path(project_root) / "data" / "research" / "research_ohlcv.duckdb"
        if research_db.exists() and Path(db_path).resolve() != research_db.resolve():
            snapshots = _load_recent_raw_snapshots(
                research_db,
                as_of=str(as_of),
                exchange=exchange,
                index_code=code,
                limit=max(confirmation_days, 1),
                rules=raw_rules,
                previous_regime=previous_regime,
            )
    if not snapshots:
        raise RuntimeError(f"No regime breadth data available at or before {as_of}")
    confirmed = confirmed_regime([item.regime for item in snapshots], confirmation_days=confirmation_days)
    latest = snapshots[-1]
    return replace(latest, raw_regime=latest.regime, regime=confirmed, confirmation_days=confirmation_days)


def _enter_spec(regime_spec: dict[str, Any]) -> dict[str, Any]:
    """Return the predicates that must hold to ENTER a regime from below.

    Flat blocks (no nested enter/exit) act as both — backward compat.
    Nested ``enter:`` block, when present, wins.
    """
    nested = regime_spec.get("enter")
    if isinstance(nested, dict):
        return nested
    # Drop nested sub-blocks from the flat view so they aren't treated as
    # metric predicates.
    return {k: v for k, v in regime_spec.items() if k not in ("enter", "exit")}


def _exit_spec(regime_spec: dict[str, Any]) -> dict[str, Any]:
    """Return the (looser) predicates that allow STAYING in a regime.

    Nested ``exit:`` block wins; flat blocks act as both (hysteresis off,
    enter == exit) for backward compat.
    """
    nested = regime_spec.get("exit")
    if isinstance(nested, dict):
        return nested
    return {k: v for k, v in regime_spec.items() if k not in ("enter", "exit")}


def classify_regime(
    metrics: dict[str, float | bool],
    rules: dict[str, Any] | None = None,
    previous_regime: str | None = None,
) -> str:
    """Classify one raw day using configured rules, defaulting to the 5-tier ladder.

    Configured rules win when supplied — the priority list in the YAML
    (e.g. ``[strong_bull, bull, cautious_bull, neutral, risk_off]``)
    determines walk order. ``cautious_bull`` is opt-in: a legacy 4-tier
    config without that block still classifies correctly (bull takes
    precedence at ≥55% 200DMA regardless of new-high leadership).

    Hysteresis (Phase 4): when ``previous_regime`` is provided AND the
    current regime block has a nested ``exit:`` sub-block, the stay-put
    decision is made first — if the looser exit predicates still hold
    for the previous regime, return it unchanged. Otherwise walk the
    priority list applying each regime's ``enter:`` predicates
    (typically stricter). This avoids 54%↔56% pct_above_200dma flip-flop
    between adjacent bands.
    """
    if rules:
        # Step 1: walk priority list using enter predicates to find the
        # highest-aggression regime that today qualifies for. This is
        # where we'd land without hysteresis.
        priority = rules.get("__priority__") if isinstance(rules, dict) else None
        if not isinstance(priority, (list, tuple)):
            priority = ("strong_bull", "bull", "cautious_bull", "neutral", "risk_off")
        enter_target: str | None = None
        for name in priority:
            spec = rules.get(name)
            if isinstance(spec, dict) and _matches_rule(metrics, _enter_spec(spec)):
                enter_target = name
                break

        # Step 2: hysteresis stay-put. Only fires when we'd be moving DOWN
        # (target_rank < prev_rank) AND the previous regime defines a
        # nested exit block AND that exit predicate still holds. Moving
        # up or staying at a higher band is always allowed because enter
        # is the strict path — hysteresis is about lagging the descent,
        # not blocking the ascent.
        if (
            previous_regime
            and enter_target
            and isinstance(rules.get(previous_regime), dict)
            and isinstance(rules[previous_regime].get("exit"), dict)
        ):
            prev_rank = _REGIME_RANK.get(previous_regime, -1)
            target_rank = _REGIME_RANK.get(enter_target, -1)
            if (
                prev_rank >= 0
                and target_rank >= 0
                and target_rank < prev_rank
                and _matches_rule(metrics, _exit_spec(rules[previous_regime]))
            ):
                return previous_regime

        if enter_target is not None:
            return enter_target
    # Default 5-tier path (no rules YAML). Matches the post-Phase-4b
    # philosophy: 200DMA controls whether risk is allowed; at-new-high
    # leadership controls how aggressive to be within an allowed band.
    pct200 = float(metrics.get("pct_above_200dma") or 0.0)
    pct50 = float(metrics.get("pct_above_50dma") or 0.0)
    pct_at_high = float(metrics.get("pct_at_52w_high") or 0.0)
    top50 = bool(metrics.get("top1000_above_50dma"))
    top200 = bool(metrics.get("top1000_above_200dma"))
    # Risk-off: weak 200DMA AND weak leadership. Recovery periods with
    # improving 200DMA but lagging highs land in neutral, not risk_off.
    if pct200 < 0.30 and pct_at_high < 0.05:
        return "risk_off"
    if pct200 < 0.55:
        return "neutral"
    if (
        pct200 >= 0.75
        and pct50 >= 0.60
        and pct_at_high >= 0.15
        and top50
        and top200
    ):
        return "strong_bull"
    if pct200 >= 0.55 and pct_at_high >= 0.12 and top200:
        return "bull"
    if pct200 >= 0.55 and top200:
        return "cautious_bull"
    return "neutral"


# ── Derived metrics (Phase 4b) ─────────────────────────────────────────────


def compute_regime_score(metrics: dict[str, float | bool]) -> float:
    """0..100 continuous blend of the three breadth signals.

    Weighting: 200DMA participation 50%, at-new-high leadership 30%,
    50DMA short-term tape 20%. Inputs are 0..1 fractions; output is a
    0..100 score. Designed so risk_off territory lands roughly <30,
    neutral around 30–55, cautious_bull/bull around 55–75, strong_bull >75.
    """
    pct200 = float(metrics.get("pct_above_200dma") or 0.0)
    pct50 = float(metrics.get("pct_above_50dma") or 0.0)
    pct_at_high = float(metrics.get("pct_at_52w_high") or 0.0)
    # Clamp to [0, 1] before weighting so a future bug or stale snapshot
    # can't push the score out of the [0, 100] band.
    pct200 = max(0.0, min(1.0, pct200))
    pct50 = max(0.0, min(1.0, pct50))
    pct_at_high = max(0.0, min(1.0, pct_at_high))
    return round(100.0 * (0.50 * pct200 + 0.30 * pct_at_high + 0.20 * pct50), 2)


# Default 5-tier ladder thresholds, mirrors the shipped rules YAML. Used by
# compute_regime_confidence when no rule set is supplied.
_DEFAULT_BAND_EDGES_PCT200: tuple[tuple[str, float, float], ...] = (
    # (regime, lower_edge, upper_edge)
    ("risk_off", 0.0, 0.30),
    ("neutral", 0.30, 0.55),
    ("cautious_bull", 0.55, 0.75),
    ("bull", 0.55, 0.75),
    ("strong_bull", 0.75, 1.00),
)


def compute_regime_confidence(
    metrics: dict[str, float | bool],
    regime_label: str,
    rules: dict[str, Any] | None = None,
) -> float:
    """0..1 distance from the nearest 200DMA threshold edge of the active band.

    Pure function of the snapshot. 1.0 = solidly inside the regime's
    band; 0.0 = right on a boundary (about to flip). We use the 200DMA
    band because it's the dominant gate in every rule set we ship.

    For configured rules: scans the rule set for the lowest and highest
    pct_above_200dma thresholds that constrain this regime and measures
    relative position within that band. Falls back to the default 5-tier
    band edges when the rules don't constrain pct_above_200dma for the
    classified regime (e.g. legacy YAML).
    """
    pct200 = float(metrics.get("pct_above_200dma") or 0.0)
    lower, upper = _resolve_pct200_band(regime_label, rules)
    if lower is None or upper is None or upper <= lower:
        return 0.0
    width = upper - lower
    # Distance to the nearer edge, normalized by half-width so confidence is
    # 1.0 at the band center and 0.0 at either edge.
    inner = min(pct200 - lower, upper - pct200)
    return round(max(0.0, min(1.0, inner / (width / 2.0))), 3)


def _resolve_pct200_band(
    regime_label: str, rules: dict[str, Any] | None
) -> tuple[float | None, float | None]:
    """Return (lower, upper) pct_above_200dma edges for a regime.

    Looks at the regime's own rule block for `_gte` (lower) and `_lt` (upper)
    constraints. When the rules don't constrain pct_above_200dma directly
    for this regime, returns the default ladder edges.
    """
    if rules and isinstance(rules.get(regime_label), dict):
        spec = rules[regime_label]
        lower = spec.get("pct_above_200dma_gte")
        upper = spec.get("pct_above_200dma_lt")
        if isinstance(lower, (int, float)) or isinstance(upper, (int, float)):
            # If only one edge is specified, infer the other from neighbouring
            # bands. Simpler: fall back to the default band edges where one
            # side is missing.
            for default in _DEFAULT_BAND_EDGES_PCT200:
                if default[0] == regime_label:
                    return (
                        float(lower) if isinstance(lower, (int, float)) else default[1],
                        float(upper) if isinstance(upper, (int, float)) else default[2],
                    )
            return (
                float(lower) if isinstance(lower, (int, float)) else 0.0,
                float(upper) if isinstance(upper, (int, float)) else 1.0,
            )
    # No rule-based info — use the default ladder.
    for default in _DEFAULT_BAND_EDGES_PCT200:
        if default[0] == regime_label:
            return (default[1], default[2])
    return (None, None)


def confirmed_regime(raw_regimes: list[str], *, confirmation_days: int = 3) -> str:
    """Apply the 3-day-of-N confirmation filter across the 5-tier ladder.

    Higher-risk regimes need ≥2 raw days to confirm; lower-risk regimes
    cascade so a bull→cautious_bull→bull window still confirms as bull
    (positions stay sized as the broader band warrants). cautious_bull
    confirms when 2 of N are *at or above* cautious_bull (i.e. cautious_bull,
    bull, or strong_bull). risk_off requires 2 explicit risk_off days —
    it's the only sticky-down state because de-risking is conservative.
    """
    last = list(raw_regimes)[-max(int(confirmation_days), 1):]
    if not last:
        return "neutral"
    if last.count("strong_bull") >= 2:
        return "strong_bull"
    if last.count("bull") + last.count("strong_bull") >= 2:
        return "bull"
    # cautious_bull confirms when ≥2 days are cautious_bull-or-better (bull
    # / strong_bull don't have to confirm separately to keep cautious_bull
    # — they're a superset). This prevents bull→cautious_bull jitter.
    if (
        last.count("cautious_bull")
        + last.count("bull")
        + last.count("strong_bull")
    ) >= 2:
        return "cautious_bull"
    if last.count("risk_off") >= 2:
        return "risk_off"
    return "neutral"


def _matches_rule(metrics: dict[str, float | bool], spec: dict[str, Any]) -> bool:
    """Evaluate one regime block's predicates against a metrics dict.

    Keys are assumed to have already passed ``validate_regime_rules`` —
    unknown keys / type mismatches are caught at load time, not here.
    """
    for key, threshold in spec.items():
        metric_key, suffix = _split_rule_key(key)
        if suffix == "_lt":
            if not float(metrics.get(metric_key) or 0.0) < float(threshold):
                return False
        elif suffix == "_lte":
            if not float(metrics.get(metric_key) or 0.0) <= float(threshold):
                return False
        elif suffix == "_gt":
            if not float(metrics.get(metric_key) or 0.0) > float(threshold):
                return False
        elif suffix == "_gte":
            if not float(metrics.get(metric_key) or 0.0) >= float(threshold):
                return False
        else:
            # Bare key — boolean equality check.
            if bool(metrics.get(metric_key)) != bool(threshold):
                return False
    return True


def _load_recent_raw_snapshots(
    db_path: Path | str,
    *,
    as_of: str,
    exchange: str,
    index_code: str,
    limit: int,
    rules: dict[str, Any],
    previous_regime: str | None = None,
) -> list[MarketRegimeSnapshot]:
    db = Path(db_path)
    conn = duckdb.connect(str(db), read_only=True)
    try:
        catalog_columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = '_catalog'"
            ).fetchall()
        }
        benchmark_filter = "AND COALESCE(is_benchmark, FALSE) = FALSE" if "is_benchmark" in catalog_columns else ""
        rows = conn.execute(
            f"""
            WITH symbol_roll AS (
                SELECT
                    symbol_id,
                    CAST(timestamp AS DATE) AS d,
                    close,
                    AVG(close) OVER (
                        PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS sma50,
                    AVG(close) OVER (
                        PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                    ) AS sma200,
                    MAX(close) OVER (
                        PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                    ) AS high252,
                    COUNT(close) OVER (
                        PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS n50,
                    COUNT(close) OVER (
                        PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                    ) AS n200
                FROM _catalog
                WHERE exchange = ?
                  AND CAST(timestamp AS DATE) <= ?::DATE
                  AND close IS NOT NULL
                  AND close > 0
                  {benchmark_filter}
            ),
            breadth AS (
                SELECT
                    d,
                    COUNT(*) AS total_symbols_count,
                    COUNT(*) FILTER (WHERE n50 = 50) AS eligible_50dma_count,
                    COUNT(*) FILTER (WHERE n200 = 200) AS eligible_200dma_count,
                    COUNT(*) FILTER (WHERE n200 = 200) AS universe_count,
                    SUM(CASE WHEN n50 = 50 AND close > sma50 THEN 1 ELSE 0 END)::DOUBLE
                        / NULLIF(COUNT(*) FILTER (WHERE n50 = 50), 0) AS pct_above_50dma,
                    SUM(CASE WHEN n200 = 200 AND close > sma200 THEN 1 ELSE 0 END)::DOUBLE
                        / NULLIF(COUNT(*) FILTER (WHERE n200 = 200), 0) AS pct_above_200dma,
                    SUM(CASE WHEN n200 = 200 AND high252 > 0 AND close >= high252 * 0.90 THEN 1 ELSE 0 END)::DOUBLE
                        / NULLIF(COUNT(*) FILTER (WHERE n200 = 200), 0) AS pct_near_52w_high,
                    SUM(CASE WHEN n200 = 200 AND high252 > 0 AND close >= high252 THEN 1 ELSE 0 END)::DOUBLE
                        / NULLIF(COUNT(*) FILTER (WHERE n200 = 200), 0) AS pct_at_52w_high
                FROM symbol_roll
                GROUP BY d
            ),
            idx AS (
                SELECT
                    date AS d,
                    close,
                    AVG(close) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma50,
                    AVG(close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma200,
                    COUNT(close) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS n50,
                    COUNT(close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS n200
                FROM _index_catalog
                WHERE index_code = ?
                  AND date <= ?::DATE
                  AND close IS NOT NULL
                  AND close > 0
            )
            SELECT
                b.d,
                COALESCE(b.pct_above_50dma, 0.0) AS pct_above_50dma,
                COALESCE(b.pct_above_200dma, 0.0) AS pct_above_200dma,
                COALESCE(b.pct_near_52w_high, 0.0) AS pct_near_52w_high,
                COALESCE(b.universe_count, 0) AS universe_count,
                COALESCE(i.n50 = 50 AND i.close > i.sma50, FALSE) AS top1000_above_50dma,
                COALESCE(i.n200 = 200 AND i.close > i.sma200, FALSE) AS top1000_above_200dma,
                COALESCE(b.eligible_50dma_count, 0) AS eligible_50dma_count,
                COALESCE(b.eligible_200dma_count, 0) AS eligible_200dma_count,
                COALESCE(b.total_symbols_count, 0) AS total_symbols_count,
                COALESCE(b.pct_at_52w_high, 0.0) AS pct_at_52w_high
            FROM breadth b
            JOIN idx i USING (d)
            WHERE b.universe_count > 0
            ORDER BY b.d DESC
            LIMIT ?
            """,
            [exchange, as_of, index_code, as_of, int(limit)],
        ).fetchall()
    finally:
        conn.close()

    snapshots: list[MarketRegimeSnapshot] = []
    # Chain the previous-day regime through the loop so each day's
    # classification can read it as its hysteresis seed. The function's
    # `previous_regime` kwarg seeds the FIRST iteration; subsequent days
    # inherit yesterday's classification.
    rolling_prev = previous_regime
    for row in reversed(rows):
        eligible_50 = int(row[7] or 0)
        eligible_200 = int(row[8] or 0)
        total_syms = int(row[9] or 0)
        pct_at_high = float(row[10] or 0.0)
        breadth_conf = (eligible_200 / total_syms) if total_syms > 0 else 0.0
        metrics = {
            "pct_above_50dma": float(row[1] or 0.0),
            "pct_above_200dma": float(row[2] or 0.0),
            "pct_near_52w_high": float(row[3] or 0.0),
            "pct_at_52w_high": pct_at_high,
            "universe_count": int(row[4] or 0),
            "top1000_above_50dma": bool(row[5]),
            "top1000_above_200dma": bool(row[6]),
            "eligible_50dma_count": eligible_50,
            "eligible_200dma_count": eligible_200,
            "total_symbols_count": total_syms,
            "breadth_confidence": breadth_conf,
        }
        regime_label = classify_regime(metrics, rules, previous_regime=rolling_prev)
        snapshots.append(
            MarketRegimeSnapshot(
                date=str(row[0]),
                regime=regime_label,
                raw_regime=regime_label,
                pct_above_50dma=float(metrics["pct_above_50dma"]),
                pct_above_200dma=float(metrics["pct_above_200dma"]),
                pct_near_52w_high=float(metrics["pct_near_52w_high"]),
                pct_at_52w_high=pct_at_high,
                universe_count=int(metrics["universe_count"]),
                top1000_above_50dma=bool(metrics["top1000_above_50dma"]),
                top1000_above_200dma=bool(metrics["top1000_above_200dma"]),
                eligible_50dma_count=eligible_50,
                eligible_200dma_count=eligible_200,
                total_symbols_count=total_syms,
                breadth_confidence=breadth_conf,
                regime_score=compute_regime_score(metrics),
                regime_confidence=compute_regime_confidence(metrics, regime_label, rules),
                source=index_code,
            )
        )
        rolling_prev = regime_label
    return snapshots
