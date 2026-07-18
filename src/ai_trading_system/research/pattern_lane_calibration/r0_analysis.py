"""R0.1 measurement-repair analysis over an immutable R0 pilot bundle.

Reads a completed R0 replay directory (never writes into it), recomputes the
outcome layer with corrected definitions, and writes a derived analysis bundle
whose manifest is bound to the source pilot manifest hash.

Corrections over the pilot outcome layer (`pattern-r0-analysis-policy-v1`):

- benchmark-relative returns against the equal-weight liquid-1000 index;
- market-regime joined by historical as-of date from a supplied regime series;
- strict breakout confirmation (daily close above the breakout level, not an
  intraday high touch);
- invalidation independent of breakout failure (any close at or below the
  invalidation price within the window);
- failed breakout redefined as a strict-confirmed breakout whose window-end
  close is back at or below the breakout level;
- malformed suppression rows (missing ``as_of_date``/lane context) repaired
  from ``signal_date`` and the pilot structure context;
- episode-level deduplication keyed on symbol/family/pattern_start;
- forward outcomes computed for matched control symbols and signal-minus-
  control differences reported per pair.

The module fails loudly when the pilot bundle hashes do not match its
manifest, when benchmark or regime data is missing for any required date,
when control outcome windows are incomplete beyond the allowed count, when
episode keys cannot be derived, or when suppression rows cannot be repaired.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from ai_trading_system.domains.features.benchmark_index import load_benchmark_levels
from ai_trading_system.platform.db.paths import ensure_domain_layout, require_data_root_available
from ai_trading_system.research.pattern_lane_calibration.harness import _wilson_interval
from ai_trading_system.research.pattern_lane_calibration.policy import default_r0_policy

ANALYSIS_POLICY: dict[str, Any] = {
    "version": "pattern-r0-analysis-policy-v1",
    "horizons": [5, 10, 20],
    "episode_key": ["symbol_id", "pattern_family", "pattern_start"],
    "episode_observation": "first as_of_date row per episode key",
    "confirmation": "any daily close > breakout_level within the horizon window",
    "invalidation": "any daily close <= invalidation_price within the horizon window",
    "failed_breakout": "strict-confirmed and horizon-end close <= breakout_level",
    "suppression_repair": "as_of_date := signal_date; lane context joined from structure context",
    "control_pairing": "pilot matched_controls; signal-minus-control requires both windows complete",
    "regime_join": "confirmed regime matched on exact as_of_date",
}

PILOT_FILES_READ = (
    "r0_pattern_lane_signals.csv",
    "r0_pattern_structure_context.csv",
    "r0_pattern_matched_controls.csv",
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_commit(project_root: Path) -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=project_root, capture_output=True, text=True, check=True,
        ).stdout.strip()
    except Exception:
        return None


@dataclass(frozen=True)
class PilotBundle:
    directory: Path
    manifest: dict[str, Any]
    manifest_sha256: str


def load_and_verify_pilot(pilot_dir: Path) -> PilotBundle:
    manifest_path = pilot_dir / "r0_pattern_manifest.json"
    if not manifest_path.exists():
        raise RuntimeError(f"pilot manifest missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text())
    recorded = dict(manifest.get("dataset_hashes") or {})
    for name in PILOT_FILES_READ:
        expected = recorded.get(name)
        if not expected:
            raise RuntimeError(f"pilot manifest records no hash for {name}")
        actual = _sha256_file(pilot_dir / name)
        if actual != expected:
            raise RuntimeError(
                f"pilot bundle integrity failure: {name} sha256 {actual} != manifest {expected}"
            )
    return PilotBundle(
        directory=pilot_dir,
        manifest=manifest,
        manifest_sha256=_sha256_file(manifest_path),
    )


def load_benchmark_series(db_path: Path, *, benchmark_source: str, max_as_of: pd.Timestamp) -> pd.DataFrame:
    frame = load_benchmark_levels(db_path, source=benchmark_source)
    if frame["date"].max() < max_as_of:
        raise RuntimeError(
            f"benchmark series ends {frame['date'].max().date()} before final as-of {max_as_of.date()}"
        )
    return frame


def load_regime_series(regime_csv: Path, *, required_dates: pd.DatetimeIndex) -> pd.DataFrame:
    frame = pd.read_csv(regime_csv)
    if "date" not in frame.columns or "regime" not in frame.columns:
        raise RuntimeError(f"regime series {regime_csv} must contain date and regime columns")
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    missing = sorted(set(required_dates) - set(frame["date"]))
    if missing:
        raise RuntimeError(
            f"regime series missing {len(missing)} required as-of dates, first: "
            f"{missing[0].date()}"
        )
    return frame


def repair_signals(signals: pd.DataFrame, context: pd.DataFrame) -> pd.DataFrame:
    repaired = signals.copy()
    broken = repaired["as_of_date"].isna()
    repaired.loc[broken, "as_of_date"] = repaired.loc[broken, "signal_date"]
    if repaired["as_of_date"].isna().any():
        raise RuntimeError(
            f"{int(repaired['as_of_date'].isna().sum())} signal rows lack both "
            "as_of_date and signal_date; suppression repair impossible"
        )
    lane_context = context[
        ["symbol_id", "as_of_date", "scan_lane_as_of", "history_band", "structure_observation_id", "exchange"]
    ].drop_duplicates(subset=["symbol_id", "as_of_date"])
    fill = repaired.loc[broken, ["symbol_id", "as_of_date"]].merge(
        lane_context, on=["symbol_id", "as_of_date"], how="left"
    )
    for column in ("scan_lane_as_of", "history_band", "structure_observation_id", "exchange"):
        repaired.loc[broken, column] = fill[column].to_numpy()
    still_broken = repaired.loc[broken, "scan_lane_as_of"].isna()
    if still_broken.any():
        raise RuntimeError(
            f"{int(still_broken.sum())} repaired suppression rows have no structure-context "
            "lane for their symbol/as-of; cannot stamp lane context"
        )
    repaired["is_suppression_evidence"] = broken.to_numpy()
    return repaired


def assign_episodes(signals: pd.DataFrame) -> pd.DataFrame:
    keyed = signals.copy()
    key_columns = list(ANALYSIS_POLICY["episode_key"])
    incomplete = keyed[key_columns].isna().any(axis=1)
    if incomplete.any():
        raise RuntimeError(
            f"{int(incomplete.sum())} signal rows have null episode-key fields {key_columns}"
        )
    keyed["episode_id"] = (
        keyed["symbol_id"].astype(str)
        + "|" + keyed["pattern_family"].astype(str)
        + "|" + keyed["pattern_start"].astype(str)
    )
    keyed = keyed.sort_values(["episode_id", "as_of_date"], kind="mergesort")
    keyed["episode_first"] = ~keyed["episode_id"].duplicated()
    keyed["episode_observation_count"] = keyed.groupby("episode_id")["episode_id"].transform("size")
    return keyed


def load_market_groups(
    db_path: Path, *, exchange: str, symbols: list[str], from_date: str
) -> dict[str, pd.DataFrame]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        placeholders = ",".join("?" for _ in symbols)
        frame = conn.execute(
            f"""
            SELECT symbol_id, CAST(timestamp AS DATE) AS date, high, low, close
            FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) >= CAST(? AS DATE)
              AND symbol_id IN ({placeholders})
            ORDER BY symbol_id, date
            """,
            [exchange, from_date, *symbols],
        ).fetchdf()
    finally:
        conn.close()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    return {str(symbol): group.reset_index(drop=True) for symbol, group in frame.groupby("symbol_id", sort=False)}


def _window_outcomes(
    group: pd.DataFrame,
    *,
    as_of: pd.Timestamp,
    horizons: list[int],
    breakout_level: float,
    invalidation_price: float,
    bench_dates: np.ndarray,
    bench_levels: np.ndarray,
) -> list[dict[str, Any]]:
    dates = group["date"].to_numpy()
    position = int(np.searchsorted(dates, np.datetime64(as_of), side="right"))
    if position == 0:
        return []
    start_close = float(group["close"].iloc[position - 1])
    closes = group["close"].to_numpy()
    highs = group["high"].to_numpy()
    lows = group["low"].to_numpy()
    bench_position = int(np.searchsorted(bench_dates, np.datetime64(as_of), side="right"))
    rows: list[dict[str, Any]] = []
    for horizon in horizons:
        complete = position + horizon <= len(dates)
        window_close = closes[position:position + horizon]
        window_high = highs[position:position + horizon]
        window_low = lows[position:position + horizon]
        ret = float(window_close[-1] / start_close - 1.0) if complete and start_close > 0 else np.nan
        mfe = float(window_high.max() / start_close - 1.0) if complete and start_close > 0 else np.nan
        mae = float(window_low.min() / start_close - 1.0) if complete and start_close > 0 else np.nan
        bench_complete = bench_position > 0 and bench_position + horizon <= len(bench_levels)
        bench_ret = (
            float(bench_levels[bench_position + horizon - 1] / bench_levels[bench_position - 1] - 1.0)
            if bench_complete else np.nan
        )
        confirmed = bool(
            len(window_close) and np.isfinite(breakout_level) and (window_close > breakout_level).any()
        )
        confirm_hits = (
            np.flatnonzero(window_close > breakout_level)
            if len(window_close) and np.isfinite(breakout_level) else np.array([], dtype=int)
        )
        invalidated = bool(
            len(window_close) and np.isfinite(invalidation_price) and (window_close <= invalidation_price).any()
        )
        failed = bool(complete and confirmed and window_close[-1] <= breakout_level)
        rows.append({
            "horizon_sessions": int(horizon),
            "outcome_window_complete": bool(complete),
            "forward_return": ret,
            "benchmark_return": bench_ret,
            "benchmark_relative_return": ret - bench_ret if np.isfinite(ret) and np.isfinite(bench_ret) else np.nan,
            "maximum_favourable_excursion": mfe,
            "maximum_adverse_excursion": mae,
            "confirmed_breakout_strict": confirmed,
            "sessions_to_confirmation": int(confirm_hits[0] + 1) if len(confirm_hits) else np.nan,
            "invalidated_setup": invalidated,
            "failed_breakout": failed,
        })
    return rows


def compute_signal_outcomes(
    signals: pd.DataFrame,
    market_groups: dict[str, pd.DataFrame],
    benchmark: pd.DataFrame,
    *,
    horizons: list[int],
) -> pd.DataFrame:
    bench_dates = benchmark["date"].to_numpy()
    bench_levels = benchmark["close"].to_numpy()
    carry_columns = [
        "signal_id", "symbol_id", "pattern_family", "pattern_state", "evidence_origin",
        "scan_lane_as_of", "history_band", "as_of_date", "market_regime",
        "episode_id", "episode_first", "episode_observation_count",
    ]
    rows: list[dict[str, Any]] = []
    for signal in signals.itertuples(index=False):
        group = market_groups.get(str(signal.symbol_id))
        if group is None:
            raise RuntimeError(f"no market rows for signal symbol {signal.symbol_id}")
        base = {column: getattr(signal, column) for column in carry_columns}
        outcomes = _window_outcomes(
            group,
            as_of=pd.Timestamp(signal.as_of_date),
            horizons=horizons,
            breakout_level=float(signal.breakout_level) if pd.notna(signal.breakout_level) else np.nan,
            invalidation_price=float(signal.invalidation_price) if pd.notna(signal.invalidation_price) else np.nan,
            bench_dates=bench_dates,
            bench_levels=bench_levels,
        )
        for outcome in outcomes:
            rows.append({**base, **outcome})
    return pd.DataFrame(rows)


def compute_control_outcomes(
    controls: pd.DataFrame,
    market_groups: dict[str, pd.DataFrame],
    benchmark: pd.DataFrame,
    *,
    horizons: list[int],
) -> pd.DataFrame:
    bench_dates = benchmark["date"].to_numpy()
    bench_levels = benchmark["close"].to_numpy()
    rows: list[dict[str, Any]] = []
    for control in controls.itertuples(index=False):
        group = market_groups.get(str(control.control_symbol_id))
        if group is None:
            raise RuntimeError(f"no market rows for control symbol {control.control_symbol_id}")
        outcomes = _window_outcomes(
            group,
            as_of=pd.Timestamp(control.as_of_date),
            horizons=horizons,
            breakout_level=np.nan,
            invalidation_price=np.nan,
            bench_dates=bench_dates,
            bench_levels=bench_levels,
        )
        for outcome in outcomes:
            rows.append({
                "signal_id": control.signal_id,
                "control_symbol_id": control.control_symbol_id,
                "scan_lane": control.scan_lane,
                "pattern_family": control.pattern_family,
                "history_band": control.history_band,
                "as_of_date": control.as_of_date,
                "horizon_sessions": outcome["horizon_sessions"],
                "outcome_window_complete": outcome["outcome_window_complete"],
                "forward_return": outcome["forward_return"],
                "benchmark_return": outcome["benchmark_return"],
                "benchmark_relative_return": outcome["benchmark_relative_return"],
                "maximum_favourable_excursion": outcome["maximum_favourable_excursion"],
                "maximum_adverse_excursion": outcome["maximum_adverse_excursion"],
            })
    return pd.DataFrame(rows)


def signal_minus_control(
    signal_outcomes: pd.DataFrame,
    control_outcomes: pd.DataFrame,
    *,
    allow_incomplete_control_pairs: int,
) -> pd.DataFrame:
    join_keys = ["signal_id", "as_of_date", "horizon_sessions"]
    duplicated = control_outcomes.duplicated(subset=join_keys)
    if duplicated.any():
        raise RuntimeError(
            f"{int(duplicated.sum())} control outcome rows share a signal_id/as_of/horizon key; "
            "control pairing would be ambiguous"
        )
    merged = signal_outcomes.merge(
        control_outcomes,
        on=join_keys,
        how="inner",
        suffixes=("", "_control"),
    )
    if len(merged) > len(signal_outcomes):
        raise RuntimeError("control pairing produced more pairs than signal outcome rows")
    inconsistent = merged["outcome_window_complete"] & ~merged["outcome_window_complete_control"]
    if int(inconsistent.sum()) > allow_incomplete_control_pairs:
        raise RuntimeError(
            f"{int(inconsistent.sum())} control pairs lack complete outcome windows while the "
            f"signal window is complete (allowed: {allow_incomplete_control_pairs}); "
            "pass --allow-incomplete-control-pairs to override explicitly"
        )
    both = merged.loc[merged["outcome_window_complete"] & merged["outcome_window_complete_control"]].copy()
    both["signal_minus_control_return"] = both["forward_return"] - both["forward_return_control"]
    return both[[
        "signal_id", "symbol_id", "control_symbol_id", "scan_lane_as_of", "pattern_family",
        "history_band", "pattern_state", "evidence_origin", "market_regime", "as_of_date",
        "episode_id", "episode_first", "horizon_sessions",
        "forward_return", "forward_return_control", "signal_minus_control_return",
        "benchmark_relative_return",
    ]]


def _population_block(
    frame: pd.DataFrame, *, population: str, minimum_observations: int
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (lane, family, horizon), group in frame.groupby(
        ["scan_lane_as_of", "pattern_family", "horizon_sessions"], dropna=False
    ):
        total = int(len(group))
        rel = pd.to_numeric(group["benchmark_relative_return"], errors="coerce").dropna()
        row: dict[str, Any] = {
            "population": population,
            "scan_lane_as_of": lane,
            "pattern_family": family,
            "horizon_sessions": int(horizon),
            "sample_size": total,
            "benchmark_relative_sample": int(len(rel)),
            "unique_symbols": int(group["symbol_id"].nunique()),
            "median_forward_return": float(pd.to_numeric(group["forward_return"], errors="coerce").median()),
            "median_benchmark_relative_return": float(rel.median()) if len(rel) else np.nan,
            "benchmark_beat_rate": float((rel > 0).mean()) if len(rel) else np.nan,
            "minimum_sample_passed": total >= minimum_observations,
        }
        if "confirmed_breakout_strict" in group.columns:
            confirmed = int(group["confirmed_breakout_strict"].fillna(False).sum())
            invalidated = int(group["invalidated_setup"].fillna(False).sum())
            failed = int(group["failed_breakout"].fillna(False).sum())
            confirm_low, confirm_high = _wilson_interval(confirmed, total)
            row.update({
                "strict_confirmation_rate": confirmed / total,
                "strict_confirmation_ci_low": confirm_low,
                "strict_confirmation_ci_high": confirm_high,
                "invalidation_rate": invalidated / total,
                "failed_breakout_rate": failed / total,
            })
        rows.append(row)
    return pd.DataFrame(rows)


def build_population_summary(
    signal_outcomes: pd.DataFrame,
    control_outcomes: pd.DataFrame,
    pairs: pd.DataFrame,
    *,
    minimum_observations: int,
) -> pd.DataFrame:
    complete = signal_outcomes.loc[
        signal_outcomes["outcome_window_complete"] & ~signal_outcomes["is_suppression_evidence"]
    ]
    blocks = [
        _population_block(complete, population="raw_weekly_observations", minimum_observations=minimum_observations),
        _population_block(
            complete.loc[complete["evidence_origin"] == "fresh"],
            population="fresh_observations", minimum_observations=minimum_observations,
        ),
        _population_block(
            complete.loc[complete["evidence_origin"] == "carry_forward"],
            population="carry_forward_observations", minimum_observations=minimum_observations,
        ),
        _population_block(
            complete.loc[complete["episode_first"]],
            population="deduplicated_episodes", minimum_observations=minimum_observations,
        ),
    ]
    controls_complete = control_outcomes.loc[control_outcomes["outcome_window_complete"]].copy()
    controls_complete = controls_complete.rename(
        columns={"scan_lane": "scan_lane_as_of", "control_symbol_id": "symbol_id"}
    )
    blocks.append(
        _population_block(controls_complete, population="matched_controls", minimum_observations=minimum_observations)
    )
    pair_rows: list[dict[str, Any]] = []
    for scope_name, scope in (("signal_minus_control", pairs), ("signal_minus_control_episodes", pairs.loc[pairs["episode_first"]])):
        for (lane, family, horizon), group in scope.groupby(
            ["scan_lane_as_of", "pattern_family", "horizon_sessions"], dropna=False
        ):
            total = int(len(group))
            positive = int((group["signal_minus_control_return"] > 0).sum())
            positive_low, positive_high = _wilson_interval(positive, total)
            pair_rows.append({
                "population": scope_name,
                "scan_lane_as_of": lane,
                "pattern_family": family,
                "horizon_sessions": int(horizon),
                "sample_size": total,
                "unique_symbols": int(group["symbol_id"].nunique()),
                "median_forward_return": float(group["forward_return"].median()),
                "median_benchmark_relative_return": float(group["benchmark_relative_return"].median()),
                "median_signal_minus_control_return": float(group["signal_minus_control_return"].median()),
                "signal_beats_control_rate": positive / total,
                "signal_beats_control_ci_low": positive_low,
                "signal_beats_control_ci_high": positive_high,
                "minimum_sample_passed": total >= minimum_observations,
            })
    blocks.append(pd.DataFrame(pair_rows))
    return pd.concat(blocks, ignore_index=True)


def run_analysis(
    *,
    pilot_dir: Path,
    output_dir: Path,
    ohlcv_db: Path,
    regime_csv: Path,
    project_root: Path,
    allow_incomplete_control_pairs: int = 0,
) -> dict[str, Any]:
    if output_dir.resolve() == pilot_dir.resolve():
        raise RuntimeError("output directory must not be the immutable pilot directory")
    bundle = load_and_verify_pilot(pilot_dir)
    policy = default_r0_policy()
    horizons = list(ANALYSIS_POLICY["horizons"])

    signals = pd.read_csv(pilot_dir / "r0_pattern_lane_signals.csv", low_memory=False)
    context = pd.read_csv(
        pilot_dir / "r0_pattern_structure_context.csv",
        usecols=["symbol_id", "as_of_date", "scan_lane_as_of", "history_band", "structure_observation_id", "exchange"],
    )
    controls = pd.read_csv(pilot_dir / "r0_pattern_matched_controls.csv")

    signals = repair_signals(signals, context)
    signals = assign_episodes(signals)

    as_of_dates = pd.DatetimeIndex(sorted(pd.to_datetime(signals["as_of_date"]).dt.normalize().unique()))
    benchmark = load_benchmark_series(
        ohlcv_db, benchmark_source=policy.outcomes.benchmark_source, max_as_of=as_of_dates.max()
    )
    regime = load_regime_series(regime_csv, required_dates=as_of_dates)
    regime_map = dict(zip(regime["date"], regime["regime"]))
    signals["market_regime"] = pd.to_datetime(signals["as_of_date"]).dt.normalize().map(regime_map)
    if signals["market_regime"].isna().any():
        raise RuntimeError("regime join produced null regimes despite coverage check")

    symbols = sorted(
        set(signals["symbol_id"].astype(str)) | set(controls["control_symbol_id"].astype(str))
    )
    market_groups = load_market_groups(
        ohlcv_db,
        exchange="NSE",
        symbols=symbols,
        from_date=(as_of_dates.min() - pd.Timedelta(days=120)).date().isoformat(),
    )

    scannable = signals.loc[~signals["is_suppression_evidence"]]
    signal_outcomes = compute_signal_outcomes(scannable, market_groups, benchmark, horizons=horizons)
    signal_outcomes["is_suppression_evidence"] = False
    control_outcomes = compute_control_outcomes(controls, market_groups, benchmark, horizons=horizons)
    pairs = signal_minus_control(
        signal_outcomes, control_outcomes,
        allow_incomplete_control_pairs=allow_incomplete_control_pairs,
    )
    summary = build_population_summary(
        signal_outcomes, control_outcomes, pairs,
        minimum_observations=policy.outcomes.minimum_observations_per_lane_family,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "r0a_signals_repaired.csv": signals,
        "r0a_signal_outcomes.csv": signal_outcomes,
        "r0a_control_outcomes.csv": control_outcomes,
        "r0a_signal_minus_control.csv": pairs,
        "r0a_population_summary.csv": summary,
    }
    dataset_hashes: dict[str, str] = {}
    row_counts: dict[str, int] = {}
    for name, frame in outputs.items():
        path = output_dir / name
        frame.to_csv(path, index=False)
        dataset_hashes[name] = _sha256_file(path)
        row_counts[name] = int(len(frame))
    regime_copy = output_dir / "r0a_regime_series.csv"
    regime_copy.write_bytes(Path(regime_csv).read_bytes())
    dataset_hashes["r0a_regime_series.csv"] = _sha256_file(regime_copy)
    row_counts["r0a_regime_series.csv"] = int(len(regime))

    manifest = {
        "schema_version": "pattern-r0-analysis-manifest-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "analysis_policy": ANALYSIS_POLICY,
        "source_pilot": {
            "directory": str(pilot_dir),
            "manifest_sha256": bundle.manifest_sha256,
            "policy_hash": bundle.manifest.get("policy_hash"),
            "policy_version": bundle.manifest.get("policy_version"),
            "verified_inputs": {name: bundle.manifest["dataset_hashes"][name] for name in PILOT_FILES_READ},
        },
        "benchmark_source": policy.outcomes.benchmark_source,
        "benchmark_symbol": policy.outcomes.benchmark_symbol,
        "regime_source": str(regime_csv),
        "outcome_policy_version": policy.outcomes.version,
        "analysis_code_commit": _git_commit(project_root),
        "allow_incomplete_control_pairs": allow_incomplete_control_pairs,
        "dataset_hashes": dataset_hashes,
        "row_counts": row_counts,
        "operational_side_effects": False,
    }
    manifest_path = output_dir / "r0a_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="R0.1 measurement-repair analysis over an immutable R0 pilot bundle.")
    parser.add_argument("--pilot-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--regime-csv", required=True, help="Daily confirmed-regime series with date and regime columns.")
    parser.add_argument("--ohlcv-db", help="Override the operational OHLCV DuckDB path.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--allow-incomplete-control-pairs", type=int, default=0,
        help="Number of signal-complete pairs allowed to have incomplete control windows before failing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    project_root = Path(args.project_root).resolve()
    paths = ensure_domain_layout(project_root=project_root, data_domain="operational")
    require_data_root_available(paths)
    db_path = Path(args.ohlcv_db).resolve() if args.ohlcv_db else paths.ohlcv_db_path
    manifest = run_analysis(
        pilot_dir=Path(args.pilot_dir).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        ohlcv_db=db_path,
        regime_csv=Path(args.regime_csv).resolve(),
        project_root=project_root,
        allow_incomplete_control_pairs=args.allow_incomplete_control_pairs,
    )
    print(json.dumps({"status": "complete", "row_counts": manifest["row_counts"]}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
