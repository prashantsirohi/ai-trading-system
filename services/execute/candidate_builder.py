"""Normalized execution candidate and request builders."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from core.trust_confidence import TrustConfidenceEnvelope, attach_audit_fields
from run.stages.base import StageContext


def prioritize_execution_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    """Sort candidates by transparent priority columns when present."""
    if frame is None or frame.empty:
        return frame if frame is not None else pd.DataFrame()
    output = frame.copy()
    sort_cols = [col for col in ["composite_score", "rank_confidence", "signal_decay_score"] if col in output.columns]
    if not sort_cols:
        return output
    return output.sort_values(sort_cols, ascending=False).reset_index(drop=True)


def attach_execution_weight(frame: pd.DataFrame) -> pd.DataFrame:
    """Propagate rank confidence into an execution weighting scaffold."""
    output = frame.copy()
    if output.empty:
        output["execution_weight"] = pd.Series(dtype=float)
        return output
    if "rank_confidence" not in output.columns and "feature_confidence" in output.columns:
        output["rank_confidence"] = pd.to_numeric(output["feature_confidence"], errors="coerce")
    if "rank_confidence" in output.columns:
        output["execution_weight"] = pd.to_numeric(output["rank_confidence"], errors="coerce").fillna(1.0)
    else:
        output["execution_weight"] = 1.0
    return output


@dataclass(frozen=True)
class ExecutionRequest:
    """Normalized execution settings derived from pipeline params."""

    data_domain: str
    strategy_mode: str
    execution_enabled: bool
    preview_only: bool
    target_position_count: int
    ml_horizon: int
    ml_confirm_threshold: float
    capital: float
    buy_quantity: int | None
    breakout_linkage_mode: str
    regime: str
    regime_multiplier: float
    paper_slippage_bps: float
    order_type: str
    product_type: str
    validity: str
    entry_policy_name: str
    exit_atr_multiple: float
    exit_max_holding_days: int
    use_portfolio_constraints: bool
    max_positions: int
    max_sector_exposure: float
    max_single_stock_weight: float
    use_atr_position_sizing: bool

    @classmethod
    def from_context(cls, context: StageContext) -> "ExecutionRequest":
        quantity_raw = context.params.get("execution_fixed_quantity")
        return cls(
            data_domain=str(context.params.get("data_domain", "operational")),
            strategy_mode=str(context.params.get("strategy_mode", "technical")),
            execution_enabled=bool(context.params.get("execution_enabled", True)),
            preview_only=bool(context.params.get("execution_preview", False)),
            target_position_count=int(context.params.get("execution_top_n", context.params.get("top_n") or 5)),
            ml_horizon=int(context.params.get("execution_ml_horizon", 5)),
            ml_confirm_threshold=float(context.params.get("execution_ml_confirm_threshold", 0.55)),
            capital=float(context.params.get("execution_capital", 1_000_000)),
            buy_quantity=(
                int(quantity_raw)
                if quantity_raw not in (None, "")
                else None
            ),
            breakout_linkage_mode=str(context.params.get("execution_breakout_linkage", "off")).strip().lower(),
            regime=str(context.params.get("execution_regime", "TREND")),
            regime_multiplier=float(context.params.get("execution_regime_multiplier", 1.0)),
            paper_slippage_bps=float(context.params.get("paper_slippage_bps", 5.0)),
            order_type=str(context.params.get("execution_order_type", "MARKET")),
            product_type=str(context.params.get("execution_product_type", "INTRADAY")),
            validity=str(context.params.get("execution_validity", "DAY")),
            entry_policy_name=str(context.params.get("execution_entry_policy", "breakout")),
            exit_atr_multiple=float(context.params.get("execution_exit_atr_multiple", 2.0)),
            exit_max_holding_days=int(context.params.get("execution_exit_max_holding_days", 20)),
            use_portfolio_constraints=bool(context.params.get("execution_use_portfolio_constraints", False)),
            max_positions=int(context.params.get("execution_max_positions", 10)),
            max_sector_exposure=float(context.params.get("execution_max_sector_exposure", 0.30)),
            max_single_stock_weight=float(context.params.get("execution_max_single_stock_weight", 0.10)),
            use_atr_position_sizing=bool(context.params.get("execution_use_atr_position_sizing", False)),
        )


@dataclass
class ExecutionCandidateBundle:
    """Normalized execution inputs prepared from rank-stage artifacts."""

    ranked_df: pd.DataFrame
    ml_overlay_df: pd.DataFrame
    dashboard_payload: dict[str, Any]
    data_trust_status: str
    ranked_rows_before_linkage: int
    ranked_rows_after_linkage: int
    breakout_linkage_mode: str
    breakout_candidates_count: int
    breakout_qualified_count: int
    breakout_tier_a_count: int
    trust_confidence: dict[str, Any]


class ExecutionCandidateBuilder:
    """Build execution-ready candidate datasets while preserving current safeguards."""

    def build(self, context: StageContext, *, request: ExecutionRequest) -> ExecutionCandidateBundle:
        rank_artifact = context.artifact_for("rank", "ranked_signals")
        ranked_df = self._read_csv_artifact(context, "rank", "ranked_signals")
        ranked_rows_before_linkage = int(len(ranked_df))
        dashboard_payload = self._read_json_artifact(context, "rank", "dashboard_payload")
        data_trust_status = str((dashboard_payload.get("summary", {}) or {}).get("data_trust_status", "unknown"))
        self._assert_trust_gate(context, data_trust_status)

        ranked_df, breakout_metadata = self._apply_breakout_linkage(
            context,
            ranked_df=ranked_df,
            breakout_linkage_mode=request.breakout_linkage_mode,
        )
        ranked_df = attach_execution_weight(prioritize_execution_candidates(ranked_df))
        if not ranked_df.empty:
            ranked_rows = ranked_df.to_dict(orient="records")
            ranked_rows = [
                attach_audit_fields(
                    row,
                    run_id=context.run_id,
                    stage="execute",
                    artifact_path=rank_artifact.uri if rank_artifact is not None else None,
                )
                for row in ranked_rows
            ]
            ranked_df = pd.DataFrame(ranked_rows)
        ml_overlay_df = self._read_csv_artifact(context, "rank", "ml_overlay")
        top_rank_confidence = None
        top_execution_weight = None
        if not ranked_df.empty:
            if "rank_confidence" in ranked_df.columns:
                try:
                    top_rank_confidence = float(pd.to_numeric(ranked_df["rank_confidence"], errors="coerce").dropna().iloc[0])
                except Exception:
                    top_rank_confidence = None
            if "execution_weight" in ranked_df.columns:
                try:
                    top_execution_weight = float(pd.to_numeric(ranked_df["execution_weight"], errors="coerce").dropna().iloc[0])
                except Exception:
                    top_execution_weight = None
        provider_confidence = ((dashboard_payload.get("summary", {}) or {}).get("trust_confidence") or {}).get("provider_confidence")
        trust_confidence = TrustConfidenceEnvelope(
            trust_status=data_trust_status,
            provider_confidence=provider_confidence,
            rank_confidence=top_rank_confidence,
            execution_weight=top_execution_weight,
        ).to_dict()

        return ExecutionCandidateBundle(
            ranked_df=ranked_df,
            ml_overlay_df=ml_overlay_df,
            dashboard_payload=dashboard_payload,
            data_trust_status=data_trust_status,
            ranked_rows_before_linkage=ranked_rows_before_linkage,
            ranked_rows_after_linkage=int(len(ranked_df)),
            breakout_linkage_mode=request.breakout_linkage_mode,
            breakout_candidates_count=breakout_metadata["breakout_candidates_count"],
            breakout_qualified_count=breakout_metadata["breakout_qualified_count"],
            breakout_tier_a_count=breakout_metadata["breakout_tier_a_count"],
            trust_confidence=trust_confidence,
        )

    def _assert_trust_gate(self, context: StageContext, data_trust_status: str) -> None:
        block_degraded = bool(context.params.get("block_degraded_execution", False))
        blocked_states = {"blocked", "degraded"} if block_degraded else {"blocked"}
        if data_trust_status in blocked_states and not bool(context.params.get("allow_untrusted_execution", False)):
            raise RuntimeError(
                f"Execution blocked because rank data trust status is '{data_trust_status}'."
            )

    def _apply_breakout_linkage(
        self,
        context: StageContext,
        *,
        ranked_df: pd.DataFrame,
        breakout_linkage_mode: str,
    ) -> tuple[pd.DataFrame, dict[str, int]]:
        if breakout_linkage_mode != "soft_gate":
            return ranked_df, {
                "breakout_candidates_count": 0,
                "breakout_qualified_count": 0,
                "breakout_tier_a_count": 0,
            }

        breakout_df = self._read_csv_artifact(context, "rank", "breakout_scan")
        breakout_candidates = int(len(breakout_df))
        breakout_qualified = 0
        breakout_tier_a = 0
        filtered_ranked = ranked_df.copy()

        if not breakout_df.empty and "symbol_id" in breakout_df.columns:
            if "candidate_tier" in breakout_df.columns:
                tier_a_mask = breakout_df["candidate_tier"].astype(str) == "A"
                breakout_tier_a = int(tier_a_mask.sum())
                if "breakout_state" in breakout_df.columns:
                    eligible_mask = tier_a_mask & (breakout_df["breakout_state"].astype(str) == "qualified")
                else:
                    eligible_mask = tier_a_mask
                qualified_symbols = (
                    breakout_df[eligible_mask]["symbol_id"].astype(str).dropna().unique().tolist()
                )
            elif "breakout_state" in breakout_df.columns:
                qualified_symbols = (
                    breakout_df[breakout_df["breakout_state"].astype(str) == "qualified"]["symbol_id"]
                    .astype(str)
                    .dropna()
                    .unique()
                    .tolist()
                )
            else:
                qualified_symbols = breakout_df["symbol_id"].astype(str).dropna().unique().tolist()
            breakout_qualified = len(qualified_symbols)
            if qualified_symbols:
                filtered_ranked = filtered_ranked[
                    filtered_ranked["symbol_id"].astype(str).isin(set(qualified_symbols))
                ].copy()
            else:
                filtered_ranked = filtered_ranked.iloc[0:0].copy()

        return filtered_ranked, {
            "breakout_candidates_count": breakout_candidates,
            "breakout_qualified_count": breakout_qualified,
            "breakout_tier_a_count": breakout_tier_a,
        }

    def _read_csv_artifact(self, context: StageContext, stage_name: str, artifact_type: str) -> pd.DataFrame:
        artifact = context.artifact_for(stage_name, artifact_type)
        if artifact is None or not Path(artifact.uri).exists():
            return pd.DataFrame()
        return pd.read_csv(artifact.uri)

    def _read_json_artifact(self, context: StageContext, stage_name: str, artifact_type: str) -> dict[str, Any]:
        artifact = context.artifact_for(stage_name, artifact_type)
        if artifact is None or not Path(artifact.uri).exists():
            return {}
        return json.loads(Path(artifact.uri).read_text(encoding="utf-8"))
