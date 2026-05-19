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
    pct_near_52w_high: float
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
    "universe_count",
    "eligible_50dma_count",
    "eligible_200dma_count",
    "total_symbols_count",
    "breadth_confidence",
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
    "bull": 2,
    "strong_bull": 3,
}

# Confirmed-regime values that should NOT have been opening fresh positions
# when raw collapsed to risk_off. This is the "dangerous disagreement" set
# used by alert emission and the optional execute-stage override.
_DANGEROUS_DISAGREEMENT_CONFIRMED: frozenset[str] = frozenset({"bull", "strong_bull"})


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
    boot rather than silently mis-classifying days.
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
            _validate_rule_key(str(regime), str(key), value)


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
) -> MarketRegimeSnapshot:
    """Compute the confirmed breadth regime as of ``as_of``.

    Rolling SMA and 52-week-high windows end at the current row, so the query
    uses current and past observations only.
    """
    rules_payload = load_regime_rules(project_root or Path("."), rules_path) if project_root is not None else {}
    source_cfg = dict(rules_payload.get("regime_source") or {})
    confirmation_days = int(source_cfg.get("confirmation_days") or 3)
    code = str(index_code or source_cfg.get("index_code") or "UNIV_TOP1000")
    raw_rules = dict(rules_payload.get("rules") or {})

    snapshots = _load_recent_raw_snapshots(
        db_path,
        as_of=str(as_of),
        exchange=exchange,
        index_code=code,
        limit=max(confirmation_days, 1),
        rules=raw_rules,
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
            )
    if not snapshots:
        raise RuntimeError(f"No regime breadth data available at or before {as_of}")
    confirmed = confirmed_regime([item.regime for item in snapshots], confirmation_days=confirmation_days)
    latest = snapshots[-1]
    return replace(latest, raw_regime=latest.regime, regime=confirmed, confirmation_days=confirmation_days)


def classify_regime(metrics: dict[str, float | bool], rules: dict[str, Any] | None = None) -> str:
    """Classify one raw day using configured rules, defaulting to the requested thresholds."""
    if rules:
        for name in ("strong_bull", "bull", "neutral", "risk_off"):
            spec = rules.get(name)
            if isinstance(spec, dict) and _matches_rule(metrics, spec):
                return name
    pct200 = float(metrics.get("pct_above_200dma") or 0.0)
    pct50 = float(metrics.get("pct_above_50dma") or 0.0)
    top50 = bool(metrics.get("top1000_above_50dma"))
    top200 = bool(metrics.get("top1000_above_200dma"))
    if pct200 < 0.40:
        return "risk_off"
    if pct200 < 0.55:
        return "neutral"
    if pct200 >= 0.70 and pct50 >= 0.65 and top50 and top200:
        return "strong_bull"
    if pct200 >= 0.55 and top200:
        return "bull"
    return "neutral"


def confirmed_regime(raw_regimes: list[str], *, confirmation_days: int = 3) -> str:
    last = list(raw_regimes)[-max(int(confirmation_days), 1):]
    if not last:
        return "neutral"
    if last.count("strong_bull") >= 2:
        return "strong_bull"
    if last.count("bull") + last.count("strong_bull") >= 2:
        return "bull"
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
                        / NULLIF(COUNT(*) FILTER (WHERE n200 = 200), 0) AS pct_near_52w_high
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
                COALESCE(b.total_symbols_count, 0) AS total_symbols_count
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
    for row in reversed(rows):
        eligible_50 = int(row[7] or 0)
        eligible_200 = int(row[8] or 0)
        total_syms = int(row[9] or 0)
        breadth_conf = (eligible_200 / total_syms) if total_syms > 0 else 0.0
        metrics = {
            "pct_above_50dma": float(row[1] or 0.0),
            "pct_above_200dma": float(row[2] or 0.0),
            "pct_near_52w_high": float(row[3] or 0.0),
            "universe_count": int(row[4] or 0),
            "top1000_above_50dma": bool(row[5]),
            "top1000_above_200dma": bool(row[6]),
            "eligible_50dma_count": eligible_50,
            "eligible_200dma_count": eligible_200,
            "total_symbols_count": total_syms,
            "breadth_confidence": breadth_conf,
        }
        snapshots.append(
            MarketRegimeSnapshot(
                date=str(row[0]),
                regime=classify_regime(metrics, rules),
                raw_regime=classify_regime(metrics, rules),
                pct_above_50dma=float(metrics["pct_above_50dma"]),
                pct_above_200dma=float(metrics["pct_above_200dma"]),
                pct_near_52w_high=float(metrics["pct_near_52w_high"]),
                universe_count=int(metrics["universe_count"]),
                top1000_above_50dma=bool(metrics["top1000_above_50dma"]),
                top1000_above_200dma=bool(metrics["top1000_above_200dma"]),
                eligible_50dma_count=eligible_50,
                eligible_200dma_count=eligible_200,
                total_symbols_count=total_syms,
                breadth_confidence=breadth_conf,
                source=index_code,
            )
        )
    return snapshots
