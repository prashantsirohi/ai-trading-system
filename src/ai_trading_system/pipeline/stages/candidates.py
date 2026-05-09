"""Deterministic final candidate selection stage."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.candidates.builder import build_final_candidates
from ai_trading_system.domains.candidates.contracts import (
    DEFAULT_MAX_CANDIDATES,
    DEFAULT_MIN_CANDIDATES,
    DEFAULT_TECHNICAL_POOL_SIZE,
)
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult


class CandidatesStage:
    """Combine rank scores, setup evidence, sector state, and fundamental
    enrichment into a single ``final_candidates.csv`` artifact."""

    name = "candidates"

    def run(self, context: StageContext) -> StageResult:
        ranked_artifact = context.require_artifact("rank", "ranked_signals")
        ranked_df = _read_csv(Path(ranked_artifact.uri))

        breakout_df = _read_optional(context.artifact_for("rank", "breakout_scan"))
        pattern_df = _read_optional(context.artifact_for("rank", "pattern_scan"))
        sector_df = _read_optional(context.artifact_for("rank", "sector_dashboard"))
        watchlist_df = _read_optional(context.artifact_for("fundamentals", "watchlist_candidates"))

        result, summary = build_final_candidates(
            ranked_signals=ranked_df,
            breakout_scan=breakout_df,
            pattern_scan=pattern_df,
            sector_dashboard=sector_df,
            watchlist_candidates=watchlist_df,
            min_candidates=int(context.params.get("candidates_min", DEFAULT_MIN_CANDIDATES) or DEFAULT_MIN_CANDIDATES),
            max_candidates=int(context.params.get("candidates_max", DEFAULT_MAX_CANDIDATES) or DEFAULT_MAX_CANDIDATES),
            technical_pool_size=int(
                context.params.get("candidates_technical_pool", DEFAULT_TECHNICAL_POOL_SIZE)
                or DEFAULT_TECHNICAL_POOL_SIZE
            ),
        )

        output_dir = context.output_dir()
        candidates_path = output_dir / "final_candidates.csv"
        result.to_csv(candidates_path, index=False)
        summary_path = context.write_json("candidate_summary.json", summary)

        candidates_artifact = StageArtifact.from_file(
            "final_candidates",
            candidates_path,
            row_count=len(result),
            metadata={
                "columns": list(result.columns),
                "fundamentals_used": watchlist_df is not None and not watchlist_df.empty,
            },
            attempt_number=context.attempt_number,
        )
        summary_artifact = StageArtifact.from_file(
            "candidate_summary",
            summary_path,
            row_count=int(summary.get("rows_selected", len(result))),
            metadata=summary,
            attempt_number=context.attempt_number,
        )
        return StageResult(artifacts=[candidates_artifact, summary_artifact], metadata=summary)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _read_optional(artifact: StageArtifact | None) -> pd.DataFrame | None:
    if artifact is None:
        return None
    return _read_csv(Path(artifact.uri))
