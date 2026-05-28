"""Optional fundamentals enrichment stage."""

from __future__ import annotations

import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.domains.fundamentals.enrich_rank import (
    DEFAULT_CATALYSTS_PATH,
    DEFAULT_INDUSTRY_SCORES_PATH,
    DEFAULT_INDUSTRY_TRENDS_PATH,
    DEFAULT_SCORES_PATH,
    DEFAULT_TRENDS_PATH,
    enrich_rank_artifacts,
)
from ai_trading_system.domains.fundamentals.enrich_sector_dashboard import enrich_sector_dashboard
from ai_trading_system.domains.fundamentals.screener_readmodels import refresh_fundamental_readmodels
from ai_trading_system.domains.fundamentals.screener_store import default_screener_db_path
from ai_trading_system.platform.db.paths import get_domain_paths
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
        industry_scores_path = self._resolve_industry_scores_path(context)
        industry_trends_path = self._resolve_industry_trends_path(context)
        max_stale_days = int(context.params.get("fundamental_max_stale_days", 135) or 135)
        warnings: list[str] = []

        if not scores_path.exists():
            db_path = self._resolve_screener_db_path(context)
            if db_path.exists():
                try:
                    refresh_fundamental_readmodels(
                        db_path=db_path,
                        latest_output=scores_path,
                        trends_output=trends_path,
                        snapshot_date=context.run_date,
                    )
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Failed to refresh fundamental scores from Screener SQLite: {exc}")
            if not scores_path.exists():
                warnings.append(f"Fundamental scores snapshot missing: {scores_path}")
        if not scores_path.exists():
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
                industry_status="unknown",
                industry_snapshot_date=None,
                industry_rows_scored=0,
                industry_label_counts={},
                matched_industry_rows=0,
                missing_industry_rows=0,
                industry_trend_status="unknown",
                industry_trend_label_counts={},
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
        industry_available = industry_scores_path.exists()
        industry_trends_available = industry_trends_path.exists()
        watchlist, metrics = enrich_rank_artifacts(
            rank_dir=rank_dir,
            fundamental_scores=scores_path,
            fundamental_trends=trends_path,
            industry_scores=industry_scores_path if industry_available else None,
            industry_trends=industry_trends_path if industry_trends_available else None,
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
        industry_artifacts: list[StageArtifact] = []
        industry_status = "missing"
        industry_snapshot_date: str | None = None
        industry_rows_scored = 0
        if industry_available:
            try:
                industry_scores = pd.read_csv(industry_scores_path)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Failed to read industry scores: {exc}")
                industry_status = "error"
                industry_scores = pd.DataFrame()
            else:
                industry_status = "available"
                industry_rows_scored = len(industry_scores)
                industry_snapshot_date = self._snapshot_date(industry_scores) or self._snapshot_date_column(
                    industry_scores, "screener_industry_snapshot_date"
                )
                industry_output = output_dir / "industry_fundamental_scores.csv"
                shutil.copyfile(industry_scores_path, industry_output)
                industry_artifacts.append(
                    StageArtifact.from_file(
                        "industry_fundamental_scores",
                        industry_output,
                        row_count=industry_rows_scored,
                        metadata={
                            "columns": list(industry_scores.columns),
                            "source_path": str(industry_scores_path),
                            "snapshot_date": industry_snapshot_date,
                        },
                        attempt_number=context.attempt_number,
                    )
                )
                if (rank_dir / "sector_dashboard.csv").exists():
                    try:
                        enriched = enrich_sector_dashboard(
                            rank_dir=rank_dir,
                            industry_scores=industry_scores_path,
                        )
                    except Exception as exc:  # noqa: BLE001
                        warnings.append(f"Sector dashboard enrichment failed: {exc}")
                    else:
                        enriched_path = rank_dir / "sector_dashboard_enriched.csv"
                        if enriched_path.exists():
                            industry_artifacts.append(
                                StageArtifact.from_file(
                                    "sector_dashboard_enriched",
                                    enriched_path,
                                    row_count=len(enriched),
                                    metadata={"columns": list(enriched.columns)},
                                    attempt_number=context.attempt_number,
                                )
                            )
        else:
            warnings.append(f"Industry fundamental scores missing: {industry_scores_path}")

        industry_trend_status = "missing"
        if industry_trends_available:
            try:
                industry_trends_frame = pd.read_csv(industry_trends_path)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Failed to read industry trends: {exc}")
                industry_trend_status = "error"
            else:
                industry_trend_status = "available"
                industry_trends_output = output_dir / "industry_fundamental_trends.csv"
                shutil.copyfile(industry_trends_path, industry_trends_output)
                industry_artifacts.append(
                    StageArtifact.from_file(
                        "industry_fundamental_trends",
                        industry_trends_output,
                        row_count=len(industry_trends_frame),
                        metadata={
                            "columns": list(industry_trends_frame.columns),
                            "source_path": str(industry_trends_path),
                        },
                        attempt_number=context.attempt_number,
                    )
                )

        analytical_artifacts: list[StageArtifact] = []
        analytical_summary: dict[str, Any] = {"status": "disabled"}
        if bool(context.params.get("enable_fundamental_insights", True)):
            db_path = self._resolve_screener_db_path(context)
            if db_path.exists():
                try:
                    from ai_trading_system.domains.fundamentals.insight_readmodels import (
                        refresh_fundamental_insight_readmodels,
                    )
                    from ai_trading_system.platform.db.paths import get_domain_paths

                    paths = get_domain_paths(project_root=context.project_root, data_domain="operational")
                    analytical_summary = refresh_fundamental_insight_readmodels(
                        screener_db_path=db_path,
                        fundamentals_db_path=context.params.get("fundamentals_duckdb_path")
                        or (paths.root_dir / "fundamentals.duckdb"),
                        ohlcv_db_path=paths.ohlcv_db_path,
                        master_db_path=paths.master_db_path,
                        from_date=context.params.get("fundamental_insights_from_date"),
                        to_date=context.params.get("fundamental_insights_to_date") or context.run_date,
                        output_dir=output_dir,
                        project_root=context.project_root,
                    )
                    for artifact_type, path_text in dict(analytical_summary.get("artifacts") or {}).items():
                        artifact_path = Path(path_text)
                        if artifact_path.exists():
                            try:
                                if artifact_path.suffix.lower() == ".json":
                                    row_count = None
                                else:
                                    row_count = len(pd.read_csv(artifact_path))
                            except Exception:
                                row_count = None
                            analytical_artifacts.append(
                                StageArtifact.from_file(
                                    artifact_type,
                                    artifact_path,
                                    row_count=row_count,
                                    metadata={"source": "fundamental_insights"},
                                    attempt_number=context.attempt_number,
                                )
                            )
                except Exception as exc:  # noqa: BLE001
                    analytical_summary = {"status": "error", "error": str(exc)}
                    warnings.append(f"Fundamental insight refresh failed: {exc}")
            else:
                analytical_summary = {"status": "skipped_missing_screener_db", "screener_db_path": str(db_path)}

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
            industry_status=industry_status,
            industry_snapshot_date=industry_snapshot_date,
            industry_rows_scored=int(industry_rows_scored),
            industry_label_counts=dict(metrics.industry_label_counts),
            matched_industry_rows=int(metrics.matched_industry_rows),
            missing_industry_rows=int(metrics.missing_industry_rows),
            industry_trend_status=industry_trend_status,
            industry_trend_label_counts=dict(metrics.industry_trend_label_counts),
            analytical_summary=analytical_summary,
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
            *industry_artifacts,
            *analytical_artifacts,
            summary_artifact,
        ]
        return StageResult(artifacts=artifacts, metadata=summary)

    def _resolve_scores_path(self, context: StageContext) -> Path:
        configured = Path(str(context.params.get("fundamental_scores_path") or DEFAULT_SCORES_PATH))
        if configured.is_absolute():
            return configured
        if configured.parts[:1] == ("fundamentals",) or configured.parts[:2] == ("data", "fundamentals"):
            return get_domain_paths(project_root=context.project_root, data_domain="operational").fundamentals_dir / configured.name
        return context.project_root / configured

    def _resolve_trends_path(self, context: StageContext) -> Path:
        configured = Path(str(context.params.get("fundamental_trends_path") or DEFAULT_TRENDS_PATH))
        if configured.is_absolute():
            return configured
        if configured.parts[:1] == ("fundamentals",) or configured.parts[:2] == ("data", "fundamentals"):
            return get_domain_paths(project_root=context.project_root, data_domain="operational").fundamentals_dir / configured.name
        return context.project_root / configured

    def _resolve_catalysts_path(self, context: StageContext) -> Path:
        configured = Path(str(context.params.get("catalyst_scores_path") or DEFAULT_CATALYSTS_PATH))
        if configured.is_absolute():
            return configured
        if configured.parts[:1] == ("fundamentals",) or configured.parts[:2] == ("data", "fundamentals"):
            return get_domain_paths(project_root=context.project_root, data_domain="operational").fundamentals_dir / configured.name
        return context.project_root / configured

    def _resolve_industry_scores_path(self, context: StageContext) -> Path:
        configured = Path(
            str(context.params.get("industry_fundamental_scores_path") or DEFAULT_INDUSTRY_SCORES_PATH)
        )
        if configured.is_absolute():
            return configured
        if configured.parts[:1] == ("fundamentals",) or configured.parts[:2] == ("data", "fundamentals"):
            return get_domain_paths(project_root=context.project_root, data_domain="operational").fundamentals_dir / configured.name
        return context.project_root / configured

    def _resolve_industry_trends_path(self, context: StageContext) -> Path:
        configured = Path(
            str(context.params.get("industry_fundamental_trends_path") or DEFAULT_INDUSTRY_TRENDS_PATH)
        )
        if configured.is_absolute():
            return configured
        if configured.parts[:1] == ("fundamentals",) or configured.parts[:2] == ("data", "fundamentals"):
            return get_domain_paths(project_root=context.project_root, data_domain="operational").fundamentals_dir / configured.name
        return context.project_root / configured

    def _resolve_screener_db_path(self, context: StageContext) -> Path:
        configured = context.params.get("screener_financials_db_path") or context.params.get("fundamental_screener_db_path")
        path = Path(str(configured)) if configured else default_screener_db_path(context.project_root)
        if path.is_absolute():
            return path
        return context.project_root / path

    def _snapshot_date_column(self, frame: pd.DataFrame, column: str) -> str | None:
        if column not in frame.columns:
            return None
        value = frame[column].dropna()
        if value.empty:
            return None
        return str(value.iloc[0])[:10]

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
        industry_status: str = "unknown",
        industry_snapshot_date: str | None = None,
        industry_rows_scored: int = 0,
        industry_label_counts: dict[str, int] | None = None,
        matched_industry_rows: int = 0,
        missing_industry_rows: int = 0,
        industry_trend_status: str = "unknown",
        industry_trend_label_counts: dict[str, int] | None = None,
        analytical_summary: dict[str, Any] | None = None,
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
            "industry_status": industry_status,
            "industry_snapshot_date": industry_snapshot_date,
            "industry_rows_scored": int(industry_rows_scored),
            "industry_label_counts": dict(industry_label_counts or {}),
            "matched_industry_rows": int(matched_industry_rows),
            "missing_industry_rows": int(missing_industry_rows),
            "industry_trend_status": industry_trend_status,
            "industry_trend_label_counts": dict(industry_trend_label_counts or {}),
            "fundamental_insights": dict(analytical_summary or {}),
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
