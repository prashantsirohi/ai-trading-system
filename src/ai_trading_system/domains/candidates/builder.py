"""Build deterministic final candidates from rank and enrichment artifacts."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.candidates.contracts import (
    CANDIDATE_GROUP_PRIORITY,
    DEFAULT_MAX_CANDIDATES,
    DEFAULT_MIN_CANDIDATES,
    DEFAULT_TECHNICAL_POOL_SIZE,
    FINAL_CANDIDATE_COLUMNS,
)
from ai_trading_system.domains.fundamentals.enrich_rank import normalize_symbol


def build_final_candidates(
    *,
    ranked_signals: pd.DataFrame,
    breakout_scan: pd.DataFrame | None = None,
    pattern_scan: pd.DataFrame | None = None,
    sector_dashboard: pd.DataFrame | None = None,
    watchlist_candidates: pd.DataFrame | None = None,
    min_candidates: int = DEFAULT_MIN_CANDIDATES,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    technical_pool_size: int = DEFAULT_TECHNICAL_POOL_SIZE,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Combine technical, setup, sector, and fundamental evidence."""

    warnings: list[str] = []
    ranked = _symbol_frame(ranked_signals)
    if ranked.empty:
        result = pd.DataFrame(columns=FINAL_CANDIDATE_COLUMNS)
        return result, _summary("completed_empty", result, rows_ranked=0, warnings=["ranked_signals is empty"])

    ranked.loc[:, "composite_score"] = _num(ranked, "composite_score")
    ranked = ranked.sort_values("composite_score", ascending=False, na_position="last", kind="stable")
    ranked = ranked.head(int(technical_pool_size)).copy()

    breakout = _best_by_symbol(breakout_scan, ["breakout_score"])
    pattern = _best_by_symbol(pattern_scan, ["pattern_score", "setup_quality"])
    watchlist = _best_by_symbol(watchlist_candidates, ["final_watchlist_score", "fundamental_score"])
    sector_lookup = _sector_lookup(sector_dashboard)
    if sector_dashboard is None or sector_dashboard.empty:
        warnings.append("sector_dashboard missing or empty; sector leadership bonus disabled")
    if watchlist_candidates is None or watchlist_candidates.empty:
        warnings.append("watchlist_candidates missing or empty; fundamental bonus disabled")

    merged = ranked.merge(breakout, on="symbol", how="left", suffixes=("", "_breakout"))
    merged = merged.merge(pattern, on="symbol", how="left", suffixes=("", "_pattern"))
    merged = merged.merge(watchlist, on="symbol", how="left", suffixes=("", "_fundamental"))

    merged = _assign_first_available(merged, "name", ["name", "name_fundamental"], "")
    merged = _assign_first_available(
        merged,
        "industry_group",
        ["industry_group", "industry_group_fundamental", "sector_name", "sector", "Sector"],
        "",
    )
    merged.loc[:, "_sector_key"] = _first_available(
        merged,
        ["sector_name", "sector", "industry_group", "industry_group_fundamental", "Sector"],
        "",
    ).map(_norm_text)
    merged.loc[:, "_sector_state"] = merged["_sector_key"].map(lambda key: sector_lookup.get(key, ""))

    merged.loc[:, "breakout_score"] = _num(merged, "breakout_score", default=0.0)
    merged.loc[:, "pattern_score"] = _num(merged, "pattern_score", default=0.0)
    merged.loc[:, "fundamental_score"] = _num(merged, "fundamental_score", default=50.0)
    merged.loc[:, "fundamental_tier"] = merged.get("fundamental_tier", pd.Series("", index=merged.index)).fillna("").astype(str)
    merged.loc[:, "fundamental_trend_label"] = (
        merged.get("fundamental_trend_label", pd.Series("", index=merged.index)).fillna("").astype(str).str.upper()
    )
    merged.loc[:, "watchlist_bucket"] = merged.get("watchlist_bucket", pd.Series("", index=merged.index)).fillna("").astype(str)
    merged.loc[:, "quarterly_result_bucket"] = (
        merged.get("quarterly_result_bucket", pd.Series("", index=merged.index)).fillna("").astype(str)
    )
    merged.loc[:, "valuation_history_bucket"] = (
        merged.get("valuation_history_bucket", pd.Series("", index=merged.index)).fillna("").astype(str)
    )
    merged.loc[:, "valuation_reason"] = merged.get("valuation_reason", pd.Series("", index=merged.index)).fillna("").astype(str)
    merged.loc[:, "_hard_red_flag"] = _truthy(merged, "hard_red_flag") | merged["fundamental_tier"].str.upper().eq("REJECT")
    merged.loc[:, "_qualified_breakout"] = _truthy(merged, "qualified") | merged["breakout_score"].ge(75)
    merged.loc[:, "_strong_pattern"] = merged["pattern_score"].ge(75) | _num(merged, "setup_quality", default=0.0).ge(70)
    merged.loc[:, "_near_high"] = _near_high(merged)
    merged.loc[:, "_has_setup"] = merged["_qualified_breakout"] | merged["_strong_pattern"] | merged["_near_high"]
    merged.loc[:, "_leading_sector"] = merged["_sector_state"].eq("LEADING")
    merged.loc[:, "_improving_sector"] = merged["_sector_state"].isin(["LEADING", "IMPROVING"])
    merged.loc[:, "_stage2"] = _stage2(merged)
    merged.loc[:, "_catalyst_present"] = _num(merged, "catalyst_score", default=float("nan")).notna() | merged.get(
        "catalyst_type", pd.Series("", index=merged.index)
    ).fillna("").astype(str).str.strip().ne("")

    merged.loc[:, "final_candidate_score"] = _final_score(merged)
    merged.loc[:, "candidate_group"] = merged.apply(_candidate_group, axis=1)
    merged.loc[:, "candidate_reason"] = merged.apply(_candidate_reason, axis=1)
    merged.loc[:, "next_action"] = merged["candidate_group"].map(_next_action)

    result_watchlist = merged["watchlist_bucket"].isin(
        [
            "F4_ACTION_CANDIDATE",
            "F3_FUND_VALUE_TECH_READY",
            "F2_RESULT_VALUE_ACCUMULATION",
            "F1_FUNDAMENTAL_WATCH",
        ]
    )
    result_downturn = merged["watchlist_bucket"].eq("D1_RESULT_DOWNTURN")
    normal = merged.loc[~merged["_hard_red_flag"] & ~result_downturn & (merged["_has_setup"] | result_watchlist)].copy()
    avoid = merged.loc[merged["_hard_red_flag"] | result_downturn].copy()
    selected = pd.concat([normal, avoid], ignore_index=True)
    selected.loc[:, "_group_priority"] = selected["candidate_group"].map(CANDIDATE_GROUP_PRIORITY).fillna(99)
    selected = selected.sort_values(
        ["_group_priority", "final_candidate_score", "composite_score"],
        ascending=[True, False, False],
        na_position="last",
        kind="stable",
    )

    target_max = max(int(min_candidates), int(max_candidates))
    result = selected.head(target_max).reset_index(drop=True)
    for column in FINAL_CANDIDATE_COLUMNS:
        if column not in result.columns:
            result.loc[:, column] = pd.NA
    result.loc[:, "final_candidate_score"] = pd.to_numeric(result["final_candidate_score"], errors="coerce").round(2)
    result = result[FINAL_CANDIDATE_COLUMNS]
    summary = _summary("completed", result, rows_ranked=len(ranked), warnings=warnings)
    return result, summary


def build_final_candidates_from_files(
    *,
    ranked_signals_path: str | Path,
    breakout_scan_path: str | Path | None = None,
    pattern_scan_path: str | Path | None = None,
    sector_dashboard_path: str | Path | None = None,
    watchlist_candidates_path: str | Path | None = None,
    output_dir: str | Path,
    min_candidates: int = DEFAULT_MIN_CANDIDATES,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    technical_pool_size: int = DEFAULT_TECHNICAL_POOL_SIZE,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Read artifacts, write final candidates and summary JSON."""

    ranked_path = Path(ranked_signals_path)
    if not ranked_path.exists():
        raise FileNotFoundError(f"ranked_signals.csv not found: {ranked_path}")

    result, summary = build_final_candidates(
        ranked_signals=pd.read_csv(ranked_path),
        breakout_scan=_read_csv_optional(breakout_scan_path),
        pattern_scan=_read_csv_optional(pattern_scan_path),
        sector_dashboard=_read_csv_optional(sector_dashboard_path),
        watchlist_candidates=_read_csv_optional(watchlist_candidates_path),
        min_candidates=min_candidates,
        max_candidates=max_candidates,
        technical_pool_size=technical_pool_size,
    )
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    result.to_csv(out_dir / "final_candidates.csv", index=False)
    (out_dir / "candidate_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return result, summary


def _symbol_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["symbol"])
    output = frame.copy()
    if "symbol" not in output.columns:
        for candidate in ("symbol_id", "Symbol", "NSE Code", "ticker"):
            if candidate in output.columns:
                output.loc[:, "symbol"] = output[candidate]
                break
    if "symbol" not in output.columns:
        return pd.DataFrame(columns=["symbol"])
    output.loc[:, "symbol"] = output["symbol"].map(normalize_symbol)
    return output.loc[output["symbol"].ne("")].copy()


def _read_csv_optional(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    file_path = Path(path)
    if not file_path.exists() or file_path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(file_path)
    except EmptyDataError:
        return pd.DataFrame()


def _best_by_symbol(frame: pd.DataFrame | None, score_columns: list[str]) -> pd.DataFrame:
    output = _symbol_frame(frame)
    if output.empty:
        return output
    for column in score_columns:
        if column not in output.columns:
            output.loc[:, column] = pd.NA
        output.loc[:, column] = pd.to_numeric(output[column], errors="coerce")
    return (
        output.sort_values([*score_columns, "symbol"], ascending=[False] * len(score_columns) + [True], na_position="last", kind="stable")
        .drop_duplicates(subset=["symbol"], keep="first")
        .reset_index(drop=True)
    )


def _sector_lookup(frame: pd.DataFrame | None) -> dict[str, str]:
    if frame is None or frame.empty:
        return {}
    sector_col = _first_column(frame, ["sector", "Sector", "sector_name", "industry_group"])
    if not sector_col:
        return {}
    output: dict[str, str] = {}
    ranked = frame.copy()
    rank_col = _first_column(ranked, ["RS_rank", "rs_rank", "rank", "Rank"])
    quadrant_col = _first_column(ranked, ["quadrant", "Quadrant", "sector_quadrant"])
    momentum_col = _first_column(ranked, ["Momentum", "momentum", "rs_momentum"])
    for idx, row in ranked.iterrows():
        key = _norm_text(row.get(sector_col))
        if not key:
            continue
        quadrant = str(row.get(quadrant_col) if quadrant_col else "").strip().upper()
        rank_value = _maybe_float(row.get(rank_col)) if rank_col else None
        momentum = _maybe_float(row.get(momentum_col)) if momentum_col else None
        has_explicit_sector_signal = bool(quadrant) or rank_value is not None or momentum is not None
        if quadrant == "LEADING" or (rank_value is not None and rank_value <= 5):
            output[key] = "LEADING"
        elif quadrant == "IMPROVING" or (momentum is not None and momentum > 0):
            output[key] = "IMPROVING"
        elif not has_explicit_sector_signal and idx < 5:
            output[key] = "LEADING"
        else:
            output[key] = ""
    return output


def _first_column(frame: pd.DataFrame, columns: list[str]) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None


def _first_available(frame: pd.DataFrame, columns: list[str], default: Any = pd.NA) -> pd.Series:
    text_default = isinstance(default, str)
    result = pd.Series(pd.NA, index=frame.index, dtype="object" if text_default else None)
    for column in columns:
        if column in frame.columns:
            replacement = frame[column].astype("object") if text_default else frame[column]
            result = result.where(result.notna(), replacement)
    return result.astype("object").fillna(default) if text_default else result.fillna(default)


def _assign_first_available(frame: pd.DataFrame, target: str, columns: list[str], default: Any = pd.NA) -> pd.DataFrame:
    values = _first_available(frame, columns, default)
    output = frame.drop(columns=[target], errors="ignore")
    output.loc[:, target] = values
    return output


def _num(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default).astype(float)


def _truthy(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    values = frame[column]
    text = values.astype(str).str.strip().str.lower()
    numeric = pd.to_numeric(values, errors="coerce")
    return text.isin({"1", "true", "t", "yes", "y", "qualified", "confirmed"}) | numeric.gt(0).fillna(False)


def _near_high(frame: pd.DataFrame) -> pd.Series:
    near_pct = _num(frame, "near_52w_high_pct", default=float("nan"))
    proximity = _num(frame, "proximity_to_highs", default=float("nan"))
    prox_high = _num(frame, "prox_high", default=float("nan"))
    prox_high_score = _num(frame, "prox_high_score", default=float("nan"))
    return (
        near_pct.le(10).fillna(False)
        | proximity.ge(70).fillna(False)
        | proximity.le(10).fillna(False)
        | prox_high.le(10).fillna(False)
        | prox_high_score.ge(70).fillna(False)
    )


def _stage2(frame: pd.DataFrame) -> pd.Series:
    text = _first_available(frame, ["stage2_label", "stage_label", "weekly_stage_label"], "").fillna("").astype(str).str.lower()
    return text.str.contains("stage2|stage 2|s2", regex=True)


def _final_score(frame: pd.DataFrame) -> pd.Series:
    setup_score = pd.concat(
        [
            _num(frame, "breakout_score", 0.0),
            _num(frame, "pattern_score", 0.0),
            _near_high(frame).astype(float) * 75.0,
        ],
        axis=1,
    ).max(axis=1)
    tier = frame.get("fundamental_tier", pd.Series("", index=frame.index)).fillna("").astype(str).str.upper()
    trend = frame.get("fundamental_trend_label", pd.Series("", index=frame.index)).fillna("").astype(str).str.upper()
    score = (
        0.60 * _num(frame, "composite_score")
        + 0.15 * setup_score
        + 0.10 * _num(frame, "fundamental_score", 50.0)
    )
    score = score + tier.isin(["A", "B"]).astype(float) * 6.0
    score = score + trend.eq("IMPROVING").astype(float) * 5.0
    score = score + frame["_leading_sector"].astype(float) * 4.0
    score = score + (frame["_improving_sector"] & ~frame["_leading_sector"]).astype(float) * 2.0
    score = score + frame["_catalyst_present"].astype(float) * 3.0
    score = score - frame["_hard_red_flag"].astype(float) * 30.0
    return score.clip(0, 100)


def _candidate_group(row: pd.Series) -> str:
    if bool(row.get("_hard_red_flag")):
        return "AVOID_RED_FLAG"
    watchlist_bucket = str(row.get("watchlist_bucket") or "")
    if watchlist_bucket == "F4_ACTION_CANDIDATE":
        return "BLOWOUT_RESULT_BREAKOUT"
    if watchlist_bucket == "F3_FUND_VALUE_TECH_READY":
        return "FUND_VALUE_TECH_READY"
    if watchlist_bucket == "F2_RESULT_VALUE_ACCUMULATION":
        return "RESULT_VALUE_ACCUMULATION"
    if watchlist_bucket == "F1_FUNDAMENTAL_WATCH":
        return "FUNDAMENTAL_WATCH"
    if watchlist_bucket == "D1_RESULT_DOWNTURN":
        return "RESULT_DOWNTURN_AVOID"
    if bool(row.get("_catalyst_present")):
        return "RESULTS_OR_CATALYST_PENDING"
    if str(row.get("fundamental_trend_label") or "").upper() == "IMPROVING":
        return "FUNDAMENTAL_IMPROVER"
    if bool(row.get("_leading_sector")) and bool(row.get("_qualified_breakout")):
        return "LEADING_SECTOR_BREAKOUT"
    if bool(row.get("_improving_sector")) and bool(row.get("_stage2")):
        return "IMPROVING_SECTOR_STAGE2"
    return "HIGH_RS_PULLBACK"


def _candidate_reason(row: pd.Series) -> str:
    group = str(row.get("candidate_group") or "")
    parts: list[str] = []
    if group == "AVOID_RED_FLAG":
        return f"Rejected by fundamental red flag: {str(row.get('red_flags') or 'hard red flag')}"
    if group == "RESULT_DOWNTURN_AVOID":
        return f"Result downturn: {str(row.get('quarterly_result_bucket') or 'DETERIORATING')}"
    q_bucket = str(row.get("quarterly_result_bucket") or "").strip()
    v_bucket = str(row.get("valuation_history_bucket") or "").strip()
    v_reason = str(row.get("valuation_reason") or "").strip()
    if q_bucket:
        parts.append(q_bucket)
    if v_bucket:
        parts.append(v_bucket)
    if v_reason:
        parts.append(v_reason)
    if bool(row.get("_qualified_breakout")):
        parts.append("qualified breakout")
    elif bool(row.get("_strong_pattern")):
        parts.append("strong pattern")
    elif bool(row.get("_near_high")):
        parts.append("near 52-week high")
    if bool(row.get("_leading_sector")):
        parts.append("leading sector")
    elif bool(row.get("_improving_sector")):
        parts.append("improving sector")
    tier = str(row.get("fundamental_tier") or "").upper()
    if tier in {"A", "B"}:
        parts.append(f"fundamental tier {tier}")
    if str(row.get("fundamental_trend_label") or "").upper() == "IMPROVING":
        parts.append("improving fundamentals")
    if bool(row.get("_catalyst_present")):
        parts.append("catalyst pending")
    return " + ".join(parts) if parts else "top technical rank with valid setup"


def _next_action(group: str) -> str:
    return {
        "LEADING_SECTOR_BREAKOUT": "Review chart and add to execution watchlist",
        "IMPROVING_SECTOR_STAGE2": "Study sector confirmation and watch pullback",
        "HIGH_RS_PULLBACK": "Wait for low-risk entry near support",
        "FUNDAMENTAL_IMPROVER": "Prioritize chart review; fundamentals improving",
        "RESULTS_OR_CATALYST_PENDING": "Track catalyst evidence before sizing",
        "AVOID_RED_FLAG": "Avoid unless special situation/catalyst",
    }.get(group, "No action")


def _summary(status: str, result: pd.DataFrame, *, rows_ranked: int, warnings: list[str]) -> dict[str, Any]:
    return {
        "status": status,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows_ranked": int(rows_ranked),
        "rows_selected": int(len(result)),
        "candidate_group_counts": {
            str(group): int(count)
            for group, count in result.get("candidate_group", pd.Series(dtype=str)).value_counts(dropna=False).to_dict().items()
        },
        "warnings": list(warnings),
    }


def _norm_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def _maybe_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build deterministic final candidates from rank and fundamentals artifacts.")
    parser.add_argument("--ranked-signals", required=True, help="Path to ranked_signals.csv")
    parser.add_argument("--breakout-scan", default=None, help="Path to breakout_scan.csv")
    parser.add_argument("--pattern-scan", default=None, help="Path to pattern_scan.csv")
    parser.add_argument("--sector-dashboard", default=None, help="Path to sector_dashboard.csv")
    parser.add_argument("--watchlist-candidates", default=None, help="Path to fundamentals watchlist_candidates.csv")
    parser.add_argument("--output-dir", required=True, help="Output attempt directory")
    parser.add_argument("--min-candidates", type=int, default=DEFAULT_MIN_CANDIDATES)
    parser.add_argument("--max-candidates", type=int, default=DEFAULT_MAX_CANDIDATES)
    parser.add_argument("--technical-pool-size", type=int, default=DEFAULT_TECHNICAL_POOL_SIZE)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result, summary = build_final_candidates_from_files(
        ranked_signals_path=args.ranked_signals,
        breakout_scan_path=args.breakout_scan,
        pattern_scan_path=args.pattern_scan,
        sector_dashboard_path=args.sector_dashboard,
        watchlist_candidates_path=args.watchlist_candidates,
        output_dir=args.output_dir,
        min_candidates=args.min_candidates,
        max_candidates=args.max_candidates,
        technical_pool_size=args.technical_pool_size,
    )
    print(f"rows written: {len(result)}")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
