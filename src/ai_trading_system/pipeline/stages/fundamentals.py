"""Optional fundamentals enrichment stage."""

from __future__ import annotations

import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.domains.fundamentals.enrich_rank import (
    DEFAULT_CATALYSTS_PATH,
    DEFAULT_SCORES_PATH,
    DEFAULT_TRENDS_PATH,
    enrich_rank_artifacts,
)
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult


class FundamentalsStage:
    """Enrich rank artifacts with the latest manual Screener snapshot."""

    name = "fundamentals"

    def run(self, context: StageContext) -> StageResult:
        rank_artifact = context.require_artifact("rank", "ranked_signals")
        rank_dir = Path(rank_artifact.uri).parent
        if not (rank_dir / "ranked_signals.csv").exists():
            raise FileNotFoundError(f"Rank artifact missing: {rank_dir / 'ranked_signals.csv'}")

        output_dir = context.output_dir()
        scores_path = self._resolve_scores_path(context)
        trends_path = self._resolve_trends_path(context)
        catalysts_path = self._resolve_catalysts_path(context)
        max_stale_days = int(context.params.get("fundamental_max_stale_days", 135) or 135)
        warnings: list[str] = []

        if not scores_path.exists():
            warnings.append(f"Fundamental scores snapshot missing: {scores_path}")
            summary = self._summary(
                context=context,
                status="skipped_missing_snapshot",
                snapshot_date=None,
                stale_days=None,
                rows_scored=0,
                matched_rank_rows=0,
                missing_fundamental_rows=0,
                tier_counts={},
                watchlist_bucket_counts={},
                hard_red_flag_count=0,
                warnings=warnings,
            )
            summary_artifact = self._write_summary(context, summary)
            return StageResult(artifacts=[summary_artifact], metadata=summary)

        scores = pd.read_csv(scores_path)
        snapshot_date = self._snapshot_date(scores)
        stale_days = self._stale_days(snapshot_date, context.run_date)
        if stale_days is None:
            warnings.append("Fundamental snapshot date missing; staleness could not be computed")
        elif stale_days > max_stale_days:
            warnings.append(
                f"Fundamental snapshot is stale: {stale_days} days old "
                f"(max {max_stale_days})"
            )

        scores_output = output_dir / "fundamental_scores.csv"
        shutil.copyfile(scores_path, scores_output)
        watchlist_output = output_dir / "watchlist_candidates.csv"
        watchlist, metrics = enrich_rank_artifacts(
            rank_dir=rank_dir,
            fundamental_scores=scores_path,
            fundamental_trends=trends_path,
            catalysts=catalysts_path if catalysts_path.exists() else None,
            output=watchlist_output,
            top_n=int(context.params.get("fundamental_top_n", 100) or 100),
            min_technical_score=float(context.params.get("fundamental_min_technical_score", 50.0) or 50.0),
            return_metrics=True,
        )

        tier_counts = (
            scores.get("fundamental_tier", pd.Series(dtype=str))
            .fillna("unknown")
            .astype(str)
            .value_counts()
            .to_dict()
        )
        hard_red_flag_count = int(
            scores.get("hard_red_flag", pd.Series(False, index=scores.index))
            .astype(str)
            .str.lower()
            .isin({"1", "true", "t", "yes", "y"})
            .sum()
        )
        summary = self._summary(
            context=context,
            status="completed",
            snapshot_date=snapshot_date,
            stale_days=stale_days,
            rows_scored=len(scores),
            matched_rank_rows=metrics.matched_rank_rows,
            missing_fundamental_rows=metrics.missing_fundamental_rows,
            tier_counts={str(k): int(v) for k, v in tier_counts.items()},
            watchlist_bucket_counts=metrics.watchlist_bucket_counts,
            hard_red_flag_count=hard_red_flag_count,
            warnings=warnings,
        )
        summary_artifact = self._write_summary(context, summary)
        artifacts = [
            StageArtifact.from_file(
                "watchlist_candidates",
                watchlist_output,
                row_count=len(watchlist),
                metadata={"columns": list(watchlist.columns), "source": "fundamentals"},
                attempt_number=context.attempt_number,
            ),
            StageArtifact.from_file(
                "fundamental_scores",
                scores_output,
                row_count=len(scores),
                metadata={"columns": list(scores.columns), "source_path": str(scores_path)},
                attempt_number=context.attempt_number,
            ),
            summary_artifact,
        ]
        return StageResult(artifacts=artifacts, metadata=summary)

    def _resolve_scores_path(self, context: StageContext) -> Path:
        configured = Path(str(context.params.get("fundamental_scores_path") or DEFAULT_SCORES_PATH))
        if configured.is_absolute():
            return configured
        return context.project_root / configured

    def _resolve_trends_path(self, context: StageContext) -> Path:
        configured = Path(str(context.params.get("fundamental_trends_path") or DEFAULT_TRENDS_PATH))
        if configured.is_absolute():
            return configured
        return context.project_root / configured

    def _resolve_catalysts_path(self, context: StageContext) -> Path:
        configured = Path(str(context.params.get("catalyst_scores_path") or DEFAULT_CATALYSTS_PATH))
        if configured.is_absolute():
            return configured
        return context.project_root / configured

    def _snapshot_date(self, scores: pd.DataFrame) -> str | None:
        for column in ("screener_snapshot_date", "snapshot_date"):
            if column not in scores.columns:
                continue
            value = scores[column].dropna()
            if not value.empty:
                return str(value.iloc[0])[:10]
        return None

    def _stale_days(self, snapshot_date: str | None, run_date: str) -> int | None:
        if not snapshot_date:
            return None
        try:
            snapshot = date.fromisoformat(str(snapshot_date)[:10])
            logical_date = date.fromisoformat(str(run_date)[:10])
        except ValueError:
            return None
        return max(0, (logical_date - snapshot).days)

    def _summary(
        self,
        *,
        context: StageContext,
        status: str,
        snapshot_date: str | None,
        stale_days: int | None,
        rows_scored: int,
        matched_rank_rows: int,
        missing_fundamental_rows: int,
        tier_counts: dict[str, int],
        watchlist_bucket_counts: dict[str, int],
        hard_red_flag_count: int,
        warnings: list[str],
    ) -> dict[str, Any]:
        return {
            "status": status,
            "run_id": context.run_id,
            "snapshot_date": snapshot_date,
            "stale_days": stale_days,
            "rows_scored": int(rows_scored),
            "matched_rank_rows": int(matched_rank_rows),
            "missing_fundamental_rows": int(missing_fundamental_rows),
            "tier_counts": dict(tier_counts),
            "watchlist_bucket_counts": dict(watchlist_bucket_counts),
            "hard_red_flag_count": int(hard_red_flag_count),
            "warnings": list(warnings),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def _write_summary(self, context: StageContext, summary: dict[str, Any]) -> StageArtifact:
        summary_path = context.write_json("fundamental_summary.json", summary)
        return StageArtifact.from_file(
            "fundamental_summary",
            summary_path,
            row_count=summary.get("rows_scored"),
            metadata=summary,
            attempt_number=context.attempt_number,
        )
