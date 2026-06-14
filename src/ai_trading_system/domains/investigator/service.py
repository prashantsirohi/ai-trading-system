"""Investigator service orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.investigator.buyer_fingerprint import score_buyer_fingerprint
from ai_trading_system.domains.investigator.fundamentals import load_fundamental_snapshot, score_fundamentals
from ai_trading_system.domains.investigator.intake import load_daily_gainers
from ai_trading_system.domains.investigator.lifecycle import apply_lifecycle
from ai_trading_system.domains.investigator.move_classifier import classify_move
from ai_trading_system.domains.investigator.price_structure import score_price_structure
from ai_trading_system.domains.investigator.repeat_tracker import build_repeat_tracker
from ai_trading_system.domains.investigator.scoring import final_gate, finalize_scores
from ai_trading_system.domains.investigator.sector_context import attach_sector_context
from ai_trading_system.domains.investigator.volume_anatomy import score_volume_anatomy
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult


class InvestigatorService:
    """Build post-rank investigation artifacts."""

    def run(self, context: StageContext) -> StageResult:
        ranked_artifact = context.require_artifact("rank", "ranked_signals")
        ranked = _read_csv(Path(ranked_artifact.uri))
        breakout = _read_optional(context.artifact_for("rank", "breakout_scan"))
        stock_scan = _read_optional(context.artifact_for("rank", "stock_scan"))
        sector_dashboard = _read_optional(context.artifact_for("rank", "sector_dashboard"))
        gainers = load_daily_gainers(
            ohlcv_db_path=context.db_path,
            ranked_signals=ranked,
            as_of=context.params.get("investigator_as_of") or None,
            min_return_pct=float(context.params.get("investigator_min_return_pct", 5.0)),
            min_volume_ratio=float(context.params.get("investigator_min_volume_ratio", 2.0)),
            min_market_cap_cr=float(context.params.get("investigator_min_market_cap_cr", 500.0)),
        )
        candidates = _merge_optional(gainers, breakout, stock_scan)
        candidates = score_price_structure(candidates)
        candidates = score_volume_anatomy(candidates)
        fundamentals = load_fundamental_snapshot(
            project_root=context.project_root,
            symbols=candidates.get("symbol_id", pd.Series(dtype=str)).astype(str).tolist(),
            fundamentals_db_path=Path(context.params["fundamentals_duckdb_path"]) if context.params.get("fundamentals_duckdb_path") else None,
        )
        candidates = score_fundamentals(candidates, fundamentals)
        candidates = attach_sector_context(candidates, sector_dashboard)
        candidates = classify_move(candidates)
        candidates = score_buyer_fingerprint(candidates)
        scores = finalize_scores(candidates)
        history = self._load_history(context)
        repeat = build_repeat_tracker(current_scores=scores, historical_daily_log=history)
        active, archived = apply_lifecycle(scores, repeat)
        traps = scores.loc[scores.get("verdict", pd.Series(dtype=str)).eq("NOISE_TRAP") | scores.get("hard_trap_flag", pd.Series(False, index=scores.index)).fillna(False)].copy()
        gate = final_gate(scores)
        summary = self._summary(
            context=context,
            gainers=gainers,
            scores=scores,
            repeat=repeat,
            active=active,
            traps=traps,
            archived=archived,
            gate=gate,
        )
        artifacts = self._write_artifacts(
            context=context,
            daily_gainer_log=gainers,
            investigator_scores=scores,
            repeat_tracker=repeat,
            active_watchlist=active,
            trap_log=traps,
            archived_investigator=archived,
            final_3q_gate=gate,
            investigator_summary=summary,
        )
        self._persist_tables(context, artifacts)
        return StageResult(artifacts=artifacts, metadata=summary)

    def _load_history(self, context: StageContext) -> pd.DataFrame:
        if context.registry is None:
            return pd.DataFrame()
        try:
            with context.registry._reader() as conn:  # noqa: SLF001
                return conn.execute(
                    """
                    SELECT symbol_id, trade_date, close, volume_ratio_20, composite_score, rank_position, final_score, sector
                    FROM investigator_scores
                    WHERE trade_date >= CAST(? AS DATE) - INTERVAL 60 DAY
                    """,
                    [context.run_date],
                ).fetchdf()
        except Exception:
            return pd.DataFrame()

    def _write_artifacts(self, context: StageContext, **frames_and_summary: Any) -> list[StageArtifact]:
        output_dir = context.output_dir()
        artifacts: list[StageArtifact] = []
        for artifact_type, value in frames_and_summary.items():
            if artifact_type == "investigator_summary":
                path = context.write_json("investigator_summary.json", value)
                artifacts.append(StageArtifact.from_file(artifact_type, path, row_count=1, metadata=value, attempt_number=context.attempt_number))
                continue
            assert isinstance(value, pd.DataFrame)
            filename = f"{artifact_type}.csv"
            path = output_dir / filename
            value.to_csv(path, index=False)
            artifacts.append(
                StageArtifact.from_file(
                    artifact_type,
                    path,
                    row_count=len(value),
                    metadata={"columns": list(value.columns)},
                    attempt_number=context.attempt_number,
                )
            )
        return artifacts

    def _persist_tables(self, context: StageContext, artifacts: list[StageArtifact]) -> None:
        if context.registry is None:
            return
        mapping = {
            "daily_gainer_log": "investigator_daily_log",
            "investigator_scores": "investigator_scores",
            "repeat_tracker": "investigator_repeat_tracker",
            "active_watchlist": "investigator_lifecycle",
            "final_3q_gate": "investigator_final_gate",
            "archived_investigator": "investigator_archive",
        }
        by_type = {artifact.artifact_type: artifact for artifact in artifacts}
        with context.registry._writer() as conn:  # noqa: SLF001
            for artifact_type, table in mapping.items():
                artifact = by_type.get(artifact_type)
                if artifact is None:
                    continue
                frame = _read_csv(Path(artifact.uri))
                if frame.empty:
                    continue
                frame = frame.copy()
                frame.loc[:, "run_id"] = context.run_id
                frame.loc[:, "attempt_number"] = context.attempt_number
                frame.loc[:, "artifact_uri"] = artifact.uri
                conn.execute("CREATE TEMP TABLE investigator_stage_frame AS SELECT * FROM frame")
                conn.execute("DELETE FROM " + table + " WHERE run_id = ? AND attempt_number = ?", [context.run_id, context.attempt_number])
                columns = [row[1] for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]
                selected = [col for col in columns if col in frame.columns]
                if selected:
                    conn.execute(
                        f"INSERT INTO {table} ({', '.join(selected)}) SELECT {', '.join(selected)} FROM investigator_stage_frame"
                    )
                conn.execute("DROP TABLE investigator_stage_frame")

    def _summary(
        self,
        *,
        context: StageContext,
        gainers: pd.DataFrame,
        scores: pd.DataFrame,
        repeat: pd.DataFrame,
        active: pd.DataFrame,
        traps: pd.DataFrame,
        archived: pd.DataFrame,
        gate: pd.DataFrame,
    ) -> dict[str, Any]:
        verdict_counts = scores.get("verdict", pd.Series(dtype=str)).value_counts().to_dict() if not scores.empty else {}
        status_counts = active.get("status", pd.Series(dtype=str)).value_counts().to_dict() if not active.empty else {}
        return {
            "status": "completed",
            "run_id": context.run_id,
            "run_date": context.run_date,
            "daily_gainer_count": int(len(gainers)),
            "scored_count": int(len(scores)),
            "active_count": int(len(active)),
            "trap_count": int(len(traps)),
            "archived_count": int(len(archived)),
            "final_gate_pending_count": int(len(gate)),
            "high_conviction_count": int(verdict_counts.get("HIGH_CONVICTION", 0)),
            "medium_conviction_count": int(verdict_counts.get("MEDIUM_CONVICTION", 0)),
            "repeat_accumulation_count": int(repeat.get("high_priority_repeat", pd.Series(dtype=bool)).sum()) if not repeat.empty else 0,
            "verdict_counts": {str(k): int(v) for k, v in verdict_counts.items()},
            "status_counts": {str(k): int(v) for k, v in status_counts.items()},
        }


def _merge_optional(gainers: pd.DataFrame, breakout: pd.DataFrame | None, stock_scan: pd.DataFrame | None) -> pd.DataFrame:
    out = gainers.copy()
    for frame in (breakout, stock_scan):
        if frame is None or frame.empty or "symbol_id" not in frame.columns:
            continue
        cols = ["symbol_id"] + [col for col in frame.columns if col != "symbol_id" and col not in out.columns]
        out = out.merge(frame[cols], on="symbol_id", how="left")
    return out


def _read_optional(artifact: StageArtifact | None) -> pd.DataFrame:
    if artifact is None:
        return pd.DataFrame()
    return _read_csv(Path(artifact.uri))


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()
