"""Operational ML scoring helpers for rank-stage overlays."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from ai_trading_system.analytics.lightgbm_engine import LightGBMAlphaEngine
from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.analytics.shadow_monitor import build_shadow_overlay, prepare_current_universe_dataset
from ai_trading_system.platform.db.paths import ensure_domain_layout


DEFAULT_SHADOW_ENVIRONMENTS: dict[int, str] = {
    5: "operational_shadow_5d",
    20: "operational_shadow_20d",
}


@dataclass(frozen=True)
class DeployedModelRef:
    horizon: int
    environment: str
    deployment_id: str
    model_id: str
    model_name: str
    model_version: str
    artifact_uri: str
    engine_name: str


class OperationalMLOverlayService:
    """Build non-blocking ML overlay data for operational ranking."""

    def __init__(
        self,
        *,
        project_root: str | Path,
        registry: RegistryStore,
        data_domain: str = "operational",
        environment_map: Optional[dict[int, str]] = None,
    ):
        self.project_root = Path(project_root)
        self.registry = registry
        self.data_domain = data_domain
        self.environment_map = environment_map or DEFAULT_SHADOW_ENVIRONMENTS
        self.paths = ensure_domain_layout(project_root=self.project_root, data_domain=data_domain)

    def build_shadow_overlay(
        self,
        *,
        prediction_date: Optional[str] = None,
        exchange: str = "NSE",
        lookback_days: int = 420,
        technical_weight: float = 0.75,
        ml_weight: float = 0.25,
    ) -> Dict[str, Any]:
        model_refs = self._resolve_deployed_models()
        if len(model_refs) < 2 or 5 not in model_refs or 20 not in model_refs:
            available = sorted(model_refs.keys())
            return {
                "status": "skipped_no_active_deployment",
                "reason": f"Missing active LightGBM deployments for horizons 5 and 20. Available horizons: {available}",
                "overlay_df": pd.DataFrame(),
                "prediction_logs": {},
                "metadata": {"available_horizons": available},
            }

        scorer = LightGBMAlphaEngine(
            ohlcv_db_path=str(self.paths.ohlcv_db_path),
            feature_store_dir=str(self.paths.feature_store_dir),
            model_dir=str(self.paths.model_dir),
            data_domain=self.data_domain,
        )

        current_df, prediction_ts = prepare_current_universe_dataset(
            project_root=self.project_root,
            prediction_date=prediction_date,
            exchange=exchange,
            lookback_days=lookback_days,
        )
        if current_df.empty:
            return {
                "status": "skipped_empty_universe",
                "reason": f"No operational universe available for {prediction_date or 'latest'}",
                "overlay_df": pd.DataFrame(),
                "prediction_logs": {},
                "metadata": {},
            }

        model_5d = scorer.load_model_from_uri(model_refs[5].artifact_uri)
        model_20d = scorer.load_model_from_uri(model_refs[20].artifact_uri)
        overlay_df = build_shadow_overlay(
            current_df,
            scorer=scorer,
            model_5d=model_5d,
            model_20d=model_20d,
            technical_weight=technical_weight,
            ml_weight=ml_weight,
        )
        prediction_logs = {
            horizon: self._prediction_log_rows(
                overlay_df,
                model_ref=model_refs[horizon],
                probability_col=f"ml_{horizon}d_prob",
                rank_col=f"ml_{horizon}d_rank",
            )
            for horizon in (5, 20)
        }
        return {
            "status": "shadow_ready",
            "prediction_date": prediction_ts.date().isoformat(),
            "overlay_df": overlay_df,
            "prediction_logs": prediction_logs,
            "metadata": {
                "exchange": exchange,
                "lookback_days": int(lookback_days),
                "technical_weight": float(technical_weight),
                "ml_weight": float(ml_weight),
                "models": {
                    str(horizon): {
                        "model_id": ref.model_id,
                        "model_name": ref.model_name,
                        "model_version": ref.model_version,
                        "environment": ref.environment,
                        "deployment_id": ref.deployment_id,
                    }
                    for horizon, ref in model_refs.items()
                },
            },
        }

    def _resolve_deployed_models(self) -> Dict[int, DeployedModelRef]:
        refs: Dict[int, DeployedModelRef] = {}
        for horizon, environment in self.environment_map.items():
            deployment = self.registry.get_active_deployment(environment)
            if deployment is None:
                continue
            record = self.registry.get_model_record(deployment["model_id"])
            metadata = record.get("metadata", {})
            engine_name = metadata.get("engine", "unknown")
            if engine_name != "lightgbm":
                continue
            refs[horizon] = DeployedModelRef(
                horizon=int(horizon),
                environment=environment,
                deployment_id=deployment["deployment_id"],
                model_id=record["model_id"],
                model_name=record["model_name"],
                model_version=record["model_version"],
                artifact_uri=record["artifact_uri"],
                engine_name=engine_name,
            )
        return refs

    def _prediction_log_rows(
        self,
        overlay_df: pd.DataFrame,
        *,
        model_ref: DeployedModelRef,
        probability_col: str,
        rank_col: str,
    ) -> list[dict]:
        rows: list[dict] = []
        for row in overlay_df.to_dict(orient="records"):
            probability = row.get(probability_col)
            rows.append(
                {
                    "symbol_id": row["symbol_id"],
                    "exchange": row.get("exchange", "NSE"),
                    "model_id": model_ref.model_id,
                    "model_name": model_ref.model_name,
                    "model_version": model_ref.model_version,
                    "score": probability,
                    "probability": probability,
                    "prediction": int((probability or 0.0) >= 0.5),
                    "rank": row.get(rank_col),
                    "metadata": {
                        "technical_rank": row.get("technical_rank"),
                        "technical_score": row.get("technical_score"),
                        "top_decile": row.get(f"ml_{model_ref.horizon}d_top_decile"),
                        "blend_score": row.get(f"blend_{model_ref.horizon}d_score"),
                        "blend_rank": row.get(f"blend_{model_ref.horizon}d_rank"),
                    },
                }
            )
        return rows
