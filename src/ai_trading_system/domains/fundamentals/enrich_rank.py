"""Enrich technical rank artifacts with latest fundamental scores."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.fundamentals.contracts import WATCHLIST_BUCKET_PRIORITY, WATCHLIST_OUTPUT_COLUMNS


DEFAULT_SCORES_PATH = Path("data/fundamentals/fundamental_scores_latest.csv")
DEFAULT_TRENDS_PATH = Path("data/fundamentals/fundamental_trends_latest.csv")
DEFAULT_CATALYSTS_PATH = Path("data/fundamentals/catalyst_scores_latest.csv")
DEFAULT_OUTPUT_PATH = Path("data/fundamentals/watchlist_candidates_latest.csv")


@dataclass(frozen=True)
class EnrichmentMetrics:
    rank_rows: int
    filtered_rank_rows: int
    matched_rank_rows: int
    missing_fundamental_rows: int
    output_rows: int
    watchlist_bucket_counts: dict[str, int]


def normalize_symbol(value: Any) -> str:
    if pd.isna(value):
        return ""
    symbol = str(value).strip().upper()
    if symbol.startswith("NSE:"):
        symbol = symbol[4:]
    if symbol.endswith(".NS"):
        symbol = symbol[:-3]
    return symbol.strip()


def _read_csv_optional(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _read_ranked(rank_dir: Path) -> pd.DataFrame:
    path = rank_dir / "ranked_signals.csv"
    if not path.exists():
        raise FileNotFoundError(f"ranked_signals.csv not found under {rank_dir}")
    return pd.read_csv(path)


def _symbol_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["symbol"])
    output = frame.copy()
    if "symbol" not in output.columns:
        for candidate in ("symbol_id", "Symbol", "NSE Code"):
            if candidate in output.columns:
                output.loc[:, "symbol"] = output[candidate]
                break
    if "symbol" not in output.columns:
        return pd.DataFrame(columns=["symbol"])
    output.loc[:, "symbol"] = output["symbol"].map(normalize_symbol)
    return output.loc[output["symbol"].ne("")].copy()


def _best_by_symbol(frame: pd.DataFrame, score_columns: list[str]) -> pd.DataFrame:
    output = _symbol_frame(frame)
    if output.empty:
        return output
    for column in score_columns:
        if column not in output.columns:
            output.loc[:, column] = pd.NA
        output.loc[:, column] = pd.to_numeric(output[column], errors="coerce")
    output = output.sort_values([*score_columns, "symbol"], ascending=[False] * len(score_columns) + [True], na_position="last", kind="stable")
    return output.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)


def _num(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default).astype(float)


def _truthy_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    values = frame[column]
    text = values.astype("string").str.strip().str.lower()
    numeric = pd.to_numeric(values, errors="coerce")
    return text.isin({"1", "true", "t", "yes", "y", "qualified"}) | numeric.gt(0).fillna(False)


def _first_available(frame: pd.DataFrame, columns: list[str], default: Any = pd.NA) -> pd.Series:
    result = pd.Series(default, index=frame.index)
    for column in columns:
        if column in frame.columns:
            result = result.where(result.notna(), frame[column])
    return result


def _breakout_pattern_score(frame: pd.DataFrame) -> pd.Series:
    breakout = _num(frame, "breakout_score", default=float("nan"))
    pattern = _num(frame, "pattern_score", default=float("nan"))
    both = breakout.notna() & pattern.notna()
    score = pd.Series(50.0, index=frame.index, dtype=float)
    score.loc[breakout.notna() & pattern.isna()] = breakout.loc[breakout.notna() & pattern.isna()]
    score.loc[pattern.notna() & breakout.isna()] = pattern.loc[pattern.notna() & breakout.isna()]
    score.loc[both] = (breakout.loc[both] * 0.6) + (pattern.loc[both] * 0.4)
    bonus = frame.get("candidate_tier", pd.Series("", index=frame.index)).astype("string").str.upper().eq("A") & _truthy_series(frame, "qualified")
    score.loc[bonus] = score.loc[bonus] + 5.0
    return score.clip(0, 100)


def _bucket(frame: pd.DataFrame) -> pd.Series:
    composite = _num(frame, "composite_score")
    fundamental_tier = frame.get("fundamental_tier", pd.Series("", index=frame.index)).astype("string").str.upper()
    hard = _truthy_series(frame, "hard_red_flag")
    qualified = _truthy_series(frame, "qualified")
    pattern_score = _num(frame, "pattern_score")
    strong_pattern = pattern_score.ge(75)
    has_setup = qualified | strong_pattern

    bucket = pd.Series("IGNORE_FOR_NOW", index=frame.index)
    bucket.loc[hard | fundamental_tier.eq("REJECT")] = "AVOID_RED_FLAG"
    bucket.loc[composite.ge(70) & fundamental_tier.eq("C") & ~hard] = "TECHNICAL_ONLY_RISK"
    bucket.loc[composite.ge(65) & fundamental_tier.isin(["B", "C"]) & ~hard] = "STUDY_ONLY"
    bucket.loc[composite.ge(70) & fundamental_tier.isin(["A", "B"]) & ~hard & has_setup] = "ADD_TO_WATCHLIST"
    bucket.loc[hard | fundamental_tier.eq("REJECT")] = "AVOID_RED_FLAG"
    return bucket


def _reason(row: pd.Series) -> str:
    bucket = str(row.get("watchlist_bucket", ""))
    tier = str(row.get("fundamental_tier", "") or "missing")
    candidate_tier = str(row.get("candidate_tier", "") or "").upper()
    red_flags = str(row.get("red_flags", "") or "").strip()
    if bucket == "ADD_TO_WATCHLIST":
        setup = f"Tier {candidate_tier} breakout" if candidate_tier else "strong setup"
        reason = f"Strong technical rank + {setup} + Fundamental {tier} + no red flags"
    if bucket == "AVOID_RED_FLAG":
        reason = f"Strong RS but fundamental red flag: {red_flags or 'Reject tier'}"
    elif bucket == "STUDY_ONLY":
        reason = f"Good fundamentals but no qualified breakout yet" if not bool(row.get("qualified")) else f"Study setup + Fundamental {tier}"
    elif bucket == "TECHNICAL_ONLY_RISK":
        reason = "Technical rank is strong but fundamentals are mixed"
    elif bucket != "ADD_TO_WATCHLIST":
        reason = "No strong technical/fundamental alignment yet"
    trend_label = str(row.get("fundamental_trend_label") or "").upper()
    trend_reason = str(row.get("trend_reason") or "").strip()
    if trend_label == "IMPROVING":
        reason = f"{reason}; improving fundamentals"
    elif trend_label == "DETERIORATING":
        reason = f"{reason}; deteriorating fundamentals"
    elif trend_label == "VALUE_TRAP_RISK":
        reason = f"{reason}; value-trap risk{': ' + trend_reason if trend_reason else ''}"
    return reason


def _next_action(bucket: str) -> str:
    return {
        "ADD_TO_WATCHLIST": "Add to watchlist and review chart",
        "STUDY_ONLY": "Study fundamentals and wait for better setup",
        "TECHNICAL_ONLY_RISK": "Review manually; technical strong but fundamentals mixed",
        "AVOID_RED_FLAG": "Avoid unless special situation/catalyst",
        "IGNORE_FOR_NOW": "No action",
    }.get(bucket, "No action")


def enrich_rank_artifacts(
    *,
    rank_dir: str | Path,
    fundamental_scores: str | Path = DEFAULT_SCORES_PATH,
    fundamental_trends: str | Path | None = DEFAULT_TRENDS_PATH,
    catalysts: str | Path | None = None,
    output: str | Path = DEFAULT_OUTPUT_PATH,
    run_id: str | None = None,
    top_n: int = 100,
    min_technical_score: float = 50.0,
    return_metrics: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, EnrichmentMetrics]:
    rank_dir = Path(rank_dir)
    output = Path(output)
    ranked = _symbol_frame(_read_ranked(rank_dir))
    rank_rows = len(ranked)
    breakout = _best_by_symbol(_read_csv_optional(rank_dir / "breakout_scan.csv"), ["breakout_score"])
    pattern = _best_by_symbol(_read_csv_optional(rank_dir / "pattern_scan.csv"), ["pattern_score"])
    fundamentals = _symbol_frame(pd.read_csv(fundamental_scores))
    trends = _symbol_frame(_read_csv_optional(Path(fundamental_trends))) if fundamental_trends else pd.DataFrame(columns=["symbol"])
    catalyst_frame = _symbol_frame(_read_csv_optional(Path(catalysts))) if catalysts else pd.DataFrame(columns=["symbol"])

    ranked = ranked.loc[_num(ranked, "composite_score").ge(float(min_technical_score))].copy()
    merged = ranked.merge(fundamentals, on="symbol", how="left", suffixes=("", "_fundamental"))
    has_fundamental_match = merged.get("fundamental_score", pd.Series(pd.NA, index=merged.index)).notna()
    if not trends.empty:
        merged = merged.merge(trends, on="symbol", how="left", suffixes=("", "_trend"))
    if not breakout.empty:
        merged = merged.merge(breakout, on="symbol", how="left", suffixes=("", "_breakout"))
    if not pattern.empty:
        merged = merged.merge(pattern, on="symbol", how="left", suffixes=("", "_pattern"))
    has_catalyst_scores = not catalyst_frame.empty
    if has_catalyst_scores:
        merged = merged.merge(catalyst_frame, on="symbol", how="left", suffixes=("", "_catalyst"))

    merged.loc[:, "name"] = _first_available(merged, ["name", "name_fundamental"], "")
    merged.loc[:, "industry_group"] = _first_available(merged, ["industry_group", "industry_group_fundamental"], "")
    merged.loc[:, "industry"] = _first_available(merged, ["industry", "industry_fundamental"], "")
    merged.loc[:, "relative_strength"] = _first_available(merged, ["relative_strength", "rel_strength", "rel_strength_score"], pd.NA)
    merged.loc[:, "volume_intensity"] = _first_available(merged, ["volume_intensity", "vol_intensity", "vol_intensity_score"], pd.NA)
    merged.loc[:, "trend_persistence"] = _first_available(merged, ["trend_persistence", "trend_score", "trend_score_score"], pd.NA)
    merged.loc[:, "proximity_to_highs"] = _first_available(merged, ["proximity_to_highs", "prox_high", "prox_high_score"], pd.NA)
    merged.loc[:, "sector_strength"] = _first_available(merged, ["sector_strength", "sector_strength_score", "sector_rs_value"], pd.NA)
    merged.loc[:, "breakout_type"] = _first_available(merged, ["breakout_type", "setup_family", "taxonomy_family", "breakout_tag"], "")
    merged.loc[:, "pattern_state"] = _first_available(merged, ["pattern_state", "pattern_lifecycle_state"], "")

    for column in ("quality_score", "growth_score", "balance_sheet_score", "valuation_score", "ownership_score", "fundamental_score"):
        if column not in merged.columns:
            merged.loc[:, column] = 50.0
        merged.loc[:, column] = pd.to_numeric(merged[column], errors="coerce").fillna(50.0)
    if "fundamental_tier" not in merged.columns:
        merged.loc[:, "fundamental_tier"] = "C"
    merged.loc[:, "fundamental_tier"] = merged["fundamental_tier"].fillna("C")
    if "red_flags" not in merged.columns:
        merged.loc[:, "red_flags"] = ""
    merged = merged.assign(red_flags=merged["red_flags"].astype("object").fillna(""))
    if "hard_red_flag" not in merged.columns:
        merged.loc[:, "hard_red_flag"] = False
    if "fundamental_score_delta" not in merged.columns:
        merged.loc[:, "fundamental_score_delta"] = 0.0
    merged.loc[:, "fundamental_score_delta"] = pd.to_numeric(merged["fundamental_score_delta"], errors="coerce").fillna(0.0)
    if "fundamental_trend_label" not in merged.columns:
        merged.loc[:, "fundamental_trend_label"] = ""
    merged.loc[:, "fundamental_trend_label"] = merged["fundamental_trend_label"].fillna("")
    if "trend_reason" not in merged.columns:
        merged.loc[:, "trend_reason"] = ""
    merged.loc[:, "trend_reason"] = merged["trend_reason"].fillna("")
    merged.loc[:, "breakout_pattern_score"] = _breakout_pattern_score(merged)
    catalyst_score = _num(merged, "catalyst_score", default=float("nan"))
    has_row_catalyst = catalyst_score.notna()
    base_score = (
        0.70 * _num(merged, "composite_score")
        + 0.15 * _num(merged, "breakout_pattern_score", 50.0)
        + 0.15 * _num(merged, "fundamental_score", 50.0)
    )
    catalyst_adjusted_score = (
        0.60 * _num(merged, "composite_score")
        + 0.15 * _num(merged, "breakout_pattern_score", 50.0)
        + 0.15 * _num(merged, "fundamental_score", 50.0)
        + 0.10 * catalyst_score.fillna(0.0)
    )
    merged.loc[:, "final_watchlist_score"] = base_score.where(~has_row_catalyst, catalyst_adjusted_score)
    value_trap = merged["fundamental_trend_label"].astype(str).str.upper().eq("VALUE_TRAP_RISK")
    merged.loc[value_trap, "final_watchlist_score"] = merged.loc[value_trap, "final_watchlist_score"] - 7.0
    merged.loc[:, "final_watchlist_score"] = pd.to_numeric(merged["final_watchlist_score"], errors="coerce").clip(0, 100).round(2)
    merged.loc[:, "watchlist_bucket"] = _bucket(merged)
    add_value_trap = value_trap & merged["watchlist_bucket"].eq("ADD_TO_WATCHLIST")
    merged.loc[add_value_trap, "watchlist_bucket"] = "STUDY_ONLY"
    merged.loc[:, "watchlist_reason"] = merged.apply(_reason, axis=1)
    merged.loc[:, "next_action"] = merged["watchlist_bucket"].map(_next_action)

    for column in WATCHLIST_OUTPUT_COLUMNS:
        if column not in merged.columns:
            merged.loc[:, column] = pd.NA
    merged.loc[:, "_bucket_priority"] = merged["watchlist_bucket"].map(WATCHLIST_BUCKET_PRIORITY).fillna(99)
    result = (
        merged.sort_values(["_bucket_priority", "final_watchlist_score", "composite_score"], ascending=[True, False, False], na_position="last", kind="stable")
        .head(int(top_n))
        .reset_index(drop=True)
    )
    result = result[WATCHLIST_OUTPUT_COLUMNS]
    output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output, index=False)
    if run_id:
        run_output = Path("data/pipeline_runs") / run_id / "fundamentals" / "watchlist_candidates.csv"
        run_output.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(run_output, index=False)
    if return_metrics:
        metrics = EnrichmentMetrics(
            rank_rows=int(rank_rows),
            filtered_rank_rows=int(len(ranked)),
            matched_rank_rows=int(has_fundamental_match.sum()),
            missing_fundamental_rows=int((~has_fundamental_match).sum()),
            output_rows=int(len(result)),
            watchlist_bucket_counts={
                str(bucket): int(count)
                for bucket, count in result["watchlist_bucket"].value_counts(dropna=False).to_dict().items()
            }
        )
        return result, metrics
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Enrich rank artifacts with latest fundamental scores.")
    parser.add_argument("--rank-dir", required=True, help="Rank attempt directory")
    parser.add_argument("--fundamental-scores", default=str(DEFAULT_SCORES_PATH), help="Latest fundamental scores CSV")
    parser.add_argument("--fundamental-trends", default=str(DEFAULT_TRENDS_PATH), help="Latest fundamental trends CSV")
    parser.add_argument("--catalysts", default=None, help="Optional catalyst score CSV")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="Enriched watchlist output CSV")
    parser.add_argument("--run-id", default=None, help="Optional pipeline run id for per-run output")
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--min-technical-score", type=float, default=50.0)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = enrich_rank_artifacts(
        rank_dir=args.rank_dir,
        fundamental_scores=args.fundamental_scores,
        fundamental_trends=args.fundamental_trends,
        catalysts=args.catalysts,
        output=args.output,
        run_id=args.run_id,
        top_n=args.top_n,
        min_technical_score=args.min_technical_score,
    )
    print(f"rows written: {len(result)}")
    if not result.empty:
        print(result[["symbol", "final_watchlist_score", "watchlist_bucket", "next_action"]].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
