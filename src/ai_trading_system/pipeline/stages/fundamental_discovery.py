"""Shadow-only full-universe fundamental thesis discovery stage."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.fundamentals.analytical_store import (
    connect_fundamentals_duckdb,
    default_fundamentals_duckdb_path,
)
from ai_trading_system.domains.fundamentals.contracts import (
    ACTIVE_STATEMENT_BASIS_POLICY,
    FUNDAMENTAL_DISCOVERY_TAXONOMY_VERSION,
    FUNDAMENTAL_THESIS_RULE_VERSION,
    FundamentalDiscoveryMode,
)
from ai_trading_system.domains.fundamentals.discovery import (
    classify_fundamental_universe,
    ensure_discovery_schema,
    fundamental_source_hash,
    persist_discovery,
    project_cached_classification,
    quarantined_snapshot,
    snapshots_to_evaluations_frame,
    snapshots_to_frame,
)
from ai_trading_system.domains.fundamentals.screener_readmodels import build_raw_factor_frame
from ai_trading_system.domains.fundamentals.screener_store import (
    ScreenerFinancialsStore,
    default_screener_db_path,
)
from ai_trading_system.domains.opportunities.policy_snapshot import (
    PolicyVersionContentMismatchError,
    append_policy_snapshot_event,
    compute_policy_snapshot,
    register_or_verify_policy_snapshots,
)
from ai_trading_system.pipeline.contracts import PipelineStageError, StageArtifact, StageContext, StageResult


class FundamentalDiscoveryStageError(PipelineStageError):
    """Failure isolated to the optional fundamental-discovery shadow stage."""


class FundamentalDiscoveryStage:
    name = "fundamental_discovery"

    def run(self, context: StageContext) -> StageResult:
        mode = FundamentalDiscoveryMode(
            str(context.params.get("fundamental_discovery_mode", "off")).lower()
        )
        if mode is FundamentalDiscoveryMode.OFF:
            return StageResult(metadata={"status": "skipped", "mode": mode.value})

        policy_snapshot = compute_policy_snapshot(context.params)
        if context.registry is not None:
            try:
                register_or_verify_policy_snapshots(
                    context.registry, policy_snapshot, run_id=context.run_id
                )
            except PolicyVersionContentMismatchError as exc:
                raise FundamentalDiscoveryStageError(str(exc)) from exc
            append_policy_snapshot_event(
                context.registry,
                policy_snapshot,
                run_id=context.run_id,
                stage_name=self.name,
            )

        output_dir = context.output_dir()
        universe, warnings = self._build_input_frame(context)
        fundamentals_db_path = Path(
            str(
                context.params.get("fundamentals_duckdb_path")
                or default_fundamentals_duckdb_path(context.project_root)
            )
        )
        conn = connect_fundamentals_duckdb(fundamentals_db_path)
        try:
            ensure_discovery_schema(conn)
            cached_rows = conn.execute(
                """
                SELECT * FROM fundamental_thesis_classification
                WHERE taxonomy_version = ? AND rule_version = ?
                """,
                [
                    FUNDAMENTAL_DISCOVERY_TAXONOMY_VERSION,
                    FUNDAMENTAL_THESIS_RULE_VERSION,
                ],
            ).df()
            cached = {
                (str(row["symbol_id"]), str(row["exchange"]), str(row["source_data_hash"])): row.to_dict()
                for _, row in cached_rows.iterrows()
            }
            snapshots = []
            reused_count = 0
            recomputed_keys: set[tuple[str, str, str]] = set()
            for _, row in universe.iterrows():
                source_hash = fundamental_source_hash(row)
                key = (
                    str(row.get("symbol_id") or row.get("symbol") or "").upper().strip(),
                    str(row.get("exchange") or "NSE").upper().strip(),
                    source_hash,
                )
                if key in cached:
                    try:
                        snapshots.append(project_cached_classification(row, cached[key], as_of=context.run_date))
                        reused_count += 1
                    except Exception as exc:  # noqa: BLE001 - per-symbol quarantine boundary
                        warnings.append(f"{key[1]}:{key[0]} cached projection quarantined: {type(exc).__name__}")
                        snapshots.append(quarantined_snapshot(row, as_of=context.run_date, error=exc))
                else:
                    recomputed_keys.add(key)
                    try:
                        classified, _ = classify_fundamental_universe(
                            pd.DataFrame([row]), as_of=context.run_date
                        )
                        snapshots.extend(classified)
                    except Exception as exc:  # noqa: BLE001 - per-symbol quarantine boundary
                        warnings.append(f"{key[1]}:{key[0]} classification quarantined: {type(exc).__name__}")
                        snapshots.append(quarantined_snapshot(row, as_of=context.run_date, error=exc))
            evaluations = snapshots_to_evaluations_frame(snapshots)
            projected = snapshots_to_frame(snapshots)
            exclusions = projected.loc[
                ~projected.get("admission_eligible", pd.Series(False, index=projected.index)).fillna(False)
            ].copy()
            conn.execute("BEGIN TRANSACTION")
            created_classifications, created_projections = persist_discovery(conn, snapshots)
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            conn.close()

        changes = self._changes_frame(projected, fundamentals_db_path)
        if not changes.empty:
            changes = changes.loc[
                changes.apply(
                    lambda row: (
                        str(row["symbol_id"]), str(row["exchange"]), str(row["source_data_hash"])
                    ) in recomputed_keys,
                    axis=1,
                )
            ].reset_index(drop=True)
        paths = {
            "fundamental_thesis_universe": output_dir / "fundamental_thesis_universe.csv",
            "fundamental_thesis_evaluations": output_dir / "fundamental_thesis_evaluations.csv",
            "fundamental_thesis_exclusions": output_dir / "fundamental_thesis_exclusions.csv",
            "fundamental_thesis_changes": output_dir / "fundamental_thesis_changes.csv",
        }
        for artifact_type, frame in (
            ("fundamental_thesis_universe", projected),
            ("fundamental_thesis_evaluations", evaluations),
            ("fundamental_thesis_exclusions", exclusions),
            ("fundamental_thesis_changes", changes),
        ):
            frame.to_csv(paths[artifact_type], index=False)

        family_counts = (
            projected.get("primary_thesis", pd.Series(dtype=str))
            .replace("", "UNCLASSIFIED_FUNDAMENTAL")
            .value_counts()
            .astype(int)
            .to_dict()
        )
        status_counts = (
            projected.get("classification_status", pd.Series(dtype=str))
            .value_counts()
            .astype(int)
            .to_dict()
        )
        blocker_counts: dict[str, int] = {}
        for snapshot in snapshots:
            for blocker in snapshot.admission_blockers:
                blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1
        available_dates = [
            item.source_available_at.isoformat()
            for item in snapshots
            if item.source_available_at is not None
        ]
        summary = {
            "status": "completed",
            "mode": mode.value,
            "run_id": context.run_id,
            "as_of": context.run_date,
            "universe_rows": int(len(projected)),
            "evaluation_rows": int(len(evaluations)),
            "admission_eligible_rows": int(projected.get("admission_eligible", pd.Series(dtype=bool)).fillna(False).sum()),
            "excluded_rows": int(len(exclusions)),
            "classification_rows_created": int(created_classifications),
            "projection_rows_created": int(created_projections),
            "classification_rows_reused": int(reused_count),
            "family_counts": {str(k): int(v) for k, v in family_counts.items()},
            "family_overlap_rows": int(sum(bool(item.secondary_theses) for item in snapshots)),
            "status_counts": {str(k): int(v) for k, v in status_counts.items()},
            "exclusion_reason_counts": dict(sorted(blocker_counts.items())),
            "freshness": {
                "oldest_available_at": min(available_dates) if available_dates else None,
                "latest_available_at": max(available_dates) if available_dates else None,
                "missing_available_at_rows": int(sum(item.source_available_at is None for item in snapshots)),
            },
            "source_version_count": int(projected.get("source_data_hash", pd.Series(dtype=str)).nunique()),
            "dq_status": "degraded" if warnings or blocker_counts.get("FUTURE_DATED_INPUT") else "passed",
            "warnings": warnings,
            **policy_snapshot.metadata(),
        }
        summary_path = context.write_json("fundamental_thesis_summary.json", summary)
        artifacts = [
            StageArtifact.from_file(
                artifact_type,
                path,
                row_count=len(frame),
                metadata={"mode": mode.value, **policy_snapshot.metadata()},
                attempt_number=context.attempt_number,
            )
            for artifact_type, path, frame in (
                ("fundamental_thesis_universe", paths["fundamental_thesis_universe"], projected),
                ("fundamental_thesis_evaluations", paths["fundamental_thesis_evaluations"], evaluations),
                ("fundamental_thesis_exclusions", paths["fundamental_thesis_exclusions"], exclusions),
                ("fundamental_thesis_changes", paths["fundamental_thesis_changes"], changes),
            )
        ]
        artifacts.append(
            StageArtifact.from_file(
                "fundamental_thesis_summary",
                summary_path,
                metadata=summary,
                attempt_number=context.attempt_number,
            )
        )
        return StageResult(artifacts=artifacts, metadata=summary)

    def _build_input_frame(self, context: StageContext) -> tuple[pd.DataFrame, list[str]]:
        warnings: list[str] = []
        rank_artifact = context.require_artifact("rank", "ranked_universe")
        rank = pd.read_csv(rank_artifact.uri, low_memory=False)
        base = rank.copy()
        if "symbol" not in base.columns:
            base.loc[:, "symbol"] = base["symbol_id"]

        screener_path = Path(
            str(
                context.params.get("screener_db_path")
                or context.params.get("screener_financials_db_path")
                or context.params.get("fundamental_screener_db_path")
                or default_screener_db_path(context.project_root)
            )
        )
        if screener_path.exists():
            try:
                raw = build_raw_factor_frame(
                    ScreenerFinancialsStore(screener_path, initialize=False),
                    statement_basis_policy=str(
                        context.params.get("fundamental_statement_basis")
                        or ACTIVE_STATEMENT_BASIS_POLICY
                    ),
                    as_of_date=context.run_date,
                )
                base = _merge_symbol(base, raw)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"raw fundamental input unavailable: {exc}")
        else:
            warnings.append(f"local Screener store missing: {screener_path}")

        for stage_name, artifact_type in (
            ("fundamentals", "fundamental_scores"),
            ("fundamentals", "quarterly_result_scores"),
            ("fundamentals", "stock_valuation_bands_latest"),
        ):
            artifact = context.artifact_for(stage_name, artifact_type)
            if artifact is None or not Path(artifact.uri).exists():
                warnings.append(f"optional discovery input missing: {stage_name}/{artifact_type}")
                continue
            try:
                frame = pd.read_csv(artifact.uri, low_memory=False)
            except (pd.errors.EmptyDataError, OSError) as exc:
                warnings.append(f"optional discovery input unreadable: {artifact_type}: {exc}")
                continue
            base = _merge_symbol(base, _latest_symbol(_filter_as_of(frame, context.run_date)))

        weekly = context.artifact_for("weekly_stage", "weekly_stock_stage_universe")
        if weekly and Path(weekly.uri).exists():
            stage = pd.read_csv(weekly.uri, low_memory=False)
            stage = _filter_as_of(stage, context.run_date)
            keep = [column for column in ("symbol_id", "exchange", "effective_stage", "provisional_stage", "stage_confidence_score") if column in stage]
            if keep:
                base = base.merge(
                    stage[keep].drop_duplicates([column for column in ("symbol_id", "exchange") if column in keep]),
                    on=[column for column in ("symbol_id", "exchange") if column in keep],
                    how="left",
                    suffixes=("", "_weekly"),
                )
        else:
            warnings.append("weekly structural context missing")

        for stage_name, artifact_type in (
            ("pattern_lane_scan", "pattern_lane_scan"),
            ("investigator", "investigator_scores"),
        ):
            artifact = context.artifact_for(stage_name, artifact_type)
            if artifact is None or not Path(artifact.uri).exists():
                continue
            try:
                evidence = _latest_symbol(
                    _filter_as_of(pd.read_csv(artifact.uri, low_memory=False), context.run_date)
                )
            except (pd.errors.EmptyDataError, OSError):
                continue
            wanted = [
                column for column in (
                    "symbol", "pattern_score", "breakout_score", "final_score", "investigator_score"
                ) if column in evidence.columns
            ]
            if "symbol" in wanted:
                base = _merge_symbol(base, evidence[wanted])

        base.loc[:, "structural_stage"] = (
            base.get("effective_stage", pd.Series("unknown", index=base.index))
            .fillna(base.get("provisional_stage", "unknown"))
            .fillna("unknown")
        )
        valuation = base.get("valuation_history_bucket", pd.Series("UNKNOWN", index=base.index))
        base.loc[:, "daily_context_complete"] = (
            base["structural_stage"].astype(str).str.lower().ne("unknown")
            & valuation.fillna("UNKNOWN").astype(str).str.upper().ne("UNKNOWN")
        )
        return base, warnings

    @staticmethod
    def _changes_frame(projected: pd.DataFrame, fundamentals_db_path: Path) -> pd.DataFrame:
        if projected.empty:
            return projected.copy()
        conn = connect_fundamentals_duckdb(fundamentals_db_path, read_only=True)
        try:
            history = conn.execute(
                """
                SELECT symbol_id, exchange, as_of, primary_thesis, source_data_hash,
                       row_number() OVER (PARTITION BY symbol_id, exchange ORDER BY as_of DESC, created_at DESC) AS rn
                FROM fundamental_thesis_classification
                """
            ).df()
        finally:
            conn.close()
        previous = history.loc[history["rn"].eq(2), ["symbol_id", "exchange", "primary_thesis", "source_data_hash"]]
        previous = previous.rename(columns={"primary_thesis": "previous_primary_thesis", "source_data_hash": "previous_source_data_hash"})
        out = projected.merge(previous, on=["symbol_id", "exchange"], how="left")
        changed = out["previous_source_data_hash"].isna() | out["source_data_hash"].ne(out["previous_source_data_hash"]) | out["primary_thesis"].fillna("").ne(out["previous_primary_thesis"].fillna(""))
        return out.loc[changed].reset_index(drop=True)


def _latest_symbol(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    if "symbol" not in out.columns and "symbol_id" in out.columns:
        out.loc[:, "symbol"] = out["symbol_id"]
    if "symbol" not in out.columns:
        return pd.DataFrame(columns=["symbol"])
    out.loc[:, "symbol"] = out["symbol"].astype(str).str.upper().str.strip()
    date_columns = [column for column in ("available_at", "report_date", "snapshot_date", "date") if column in out]
    if date_columns:
        out = out.sort_values(date_columns, kind="stable")
    return out.drop_duplicates("symbol", keep="last")


def _merge_symbol(base: pd.DataFrame, other: pd.DataFrame) -> pd.DataFrame:
    if other is None or other.empty or "symbol" not in other.columns:
        return base
    right = _latest_symbol(other)
    overlapping = [column for column in right.columns if column != "symbol" and column in base.columns]
    right = right.drop(columns=overlapping, errors="ignore")
    return base.merge(right, on="symbol", how="left")


def _filter_as_of(frame: pd.DataFrame, as_of: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    cutoff = pd.Timestamp(as_of).date()
    for column in (
        "available_at", "snapshot_date", "report_date", "date", "trade_date",
        "week_end_date", "as_of_date", "observed_at",
    ):
        if column not in frame.columns:
            continue
        observed = pd.to_datetime(frame[column], errors="coerce", utc=True).dt.date
        return frame.loc[observed.notna() & observed.le(cutoff)].copy()
    return frame


__all__ = ["FundamentalDiscoveryStage", "FundamentalDiscoveryStageError"]
