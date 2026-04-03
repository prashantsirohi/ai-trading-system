"""Ranking stage with explicit artifact outputs."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Optional

import pandas as pd

from utils.data_domains import ensure_domain_layout
from run.stages.base import StageArtifact, StageContext, StageResult


class RankStage:
    """Computes downstream ranking artifacts without publishing them."""

    name = "rank"

    def __init__(self, operation: Optional[Callable[[StageContext], Dict[str, pd.DataFrame]]] = None):
        self.operation = operation

    def run(self, context: StageContext) -> StageResult:
        outputs = self._run_smoke(context) if context.params.get("smoke") else self._run_default(context)
        stage_metadata = outputs.pop("__stage_metadata__", {})
        dashboard_payload = outputs.pop("__dashboard_payload__", None)
        artifacts = []
        metadata = {"completed_at": datetime.now(timezone.utc).isoformat()}
        output_dir = context.output_dir()

        for artifact_type, df in outputs.items():
            if df is None:
                continue
            path = output_dir / f"{artifact_type}.csv"
            df.to_csv(path, index=False)
            artifacts.append(
                StageArtifact.from_file(
                    artifact_type,
                    path,
                    row_count=len(df),
                    metadata={"columns": list(df.columns)},
                    attempt_number=context.attempt_number,
                )
            )
            metadata[f"{artifact_type}_rows"] = len(df)

        if dashboard_payload is not None:
            dashboard_path = context.write_json("dashboard_payload.json", dashboard_payload)
            artifacts.append(
                StageArtifact.from_file(
                    "dashboard_payload",
                    dashboard_path,
                    row_count=dashboard_payload.get("summary", {}).get("ranked_count"),
                    metadata={"sections": list(dashboard_payload.keys())},
                    attempt_number=context.attempt_number,
                )
            )

        ranked_signals = outputs.get("ranked_signals", pd.DataFrame())
        metadata["ranked_rows"] = len(ranked_signals)
        metadata["top_symbol"] = (
            str(ranked_signals.iloc[0]["symbol_id"]) if not ranked_signals.empty and "symbol_id" in ranked_signals.columns else None
        )
        metadata.update(stage_metadata)
        summary_path = context.write_json("rank_summary.json", metadata)
        artifacts.append(
            StageArtifact.from_file(
                "rank_summary",
                summary_path,
                row_count=metadata["ranked_rows"],
                metadata=metadata,
                attempt_number=context.attempt_number,
            )
        )
        return StageResult(artifacts=artifacts, metadata=metadata)

    def _run_default(self, context: StageContext) -> Dict[str, pd.DataFrame]:
        if self.operation is not None:
            return self.operation(context)

        from analytics.ranker import StockRanker
        from channel.breakout_scan import scan_breakouts
        from channel import sector_dashboard, stock_scan

        paths = ensure_domain_layout(
            project_root=context.project_root,
            data_domain=context.params.get("data_domain", "operational"),
        )
        ranker = StockRanker(
            ohlcv_db_path=str(context.db_path),
            feature_store_dir=str(paths.feature_store_dir),
            data_domain=context.params.get("data_domain", "operational"),
        )
        ranked = ranker.rank_all(
            date=context.run_date,
            min_score=float(context.params.get("min_score", 0.0)),
            top_n=context.params.get("top_n"),
        )

        outputs: Dict[str, pd.DataFrame] = {"ranked_signals": ranked}
        warnings: list[str] = []

        try:
            outputs["breakout_scan"] = scan_breakouts(
                ohlcv_db_path=str(context.db_path),
                feature_store_dir=str(paths.feature_store_dir),
                master_db_path=str(paths.master_db_path),
                date=context.run_date,
            )
        except Exception as exc:
            outputs["breakout_scan"] = pd.DataFrame()
            warnings.append(f"breakout_scan unavailable: {exc}")

        try:
            sector_rs = stock_scan.load_sector_rs()
            stock_vs_sector = stock_scan.load_stock_vs_sector()
            sector_mapping = stock_scan.load_sector_mapping()
            outputs["stock_scan"] = stock_scan.scan_stocks(sector_rs, stock_vs_sector, sector_mapping).reset_index()
        except Exception as exc:
            outputs["stock_scan"] = pd.DataFrame()
            warnings.append(f"stock_scan unavailable: {exc}")

        try:
            sector_rs = sector_dashboard.load_sector_rs()
            stock_vs_sector = sector_dashboard.load_stock_vs_sector()
            sector_mapping = sector_dashboard.load_sector_mapping()
            sector_momentum = sector_dashboard.compute_sector_momentum(sector_rs, days=20)
            outputs["sector_dashboard"] = sector_dashboard.build_dashboard(sector_rs, sector_momentum).reset_index()
        except Exception as exc:
            outputs["sector_dashboard"] = pd.DataFrame()
            warnings.append(f"sector_dashboard unavailable: {exc}")

        outputs["__stage_metadata__"] = {
            "degraded_outputs": warnings,
            "degraded_output_count": len(warnings),
        }
        outputs["__dashboard_payload__"] = self._build_dashboard_payload(
            context=context,
            ranked_df=ranked,
            breakout_df=outputs.get("breakout_scan", pd.DataFrame()),
            stock_scan_df=outputs.get("stock_scan", pd.DataFrame()),
            sector_dashboard_df=outputs.get("sector_dashboard", pd.DataFrame()),
            warnings=warnings,
        )

        return outputs

    def _run_smoke(self, context: StageContext) -> Dict[str, pd.DataFrame]:
        ranked = pd.DataFrame(
            [
                {
                    "symbol_id": "SMOKE",
                    "exchange": "NSE",
                    "close": 104.0,
                    "composite_score": 88.5,
                    "rel_strength_score": 90.0,
                }
            ]
        )
        stock_scan = pd.DataFrame(
            [
                {
                    "Symbol": "SMOKE",
                    "category": "BUY",
                    "why": "Smoke test strength",
                    "score": 88.5,
                }
            ]
        )
        sector_dashboard = pd.DataFrame(
            [
                {
                    "Sector": "Smoke Sector",
                    "RS": 0.9,
                    "Momentum": 0.2,
                    "Quadrant": "Leading",
                }
            ]
        )
        return {
            "ranked_signals": ranked,
            "breakout_scan": pd.DataFrame(
                [
                    {
                        "symbol_id": "SMOKE",
                        "sector": "Smoke Sector",
                        "breakout_tag": "range_breakout_volume_supertrend",
                        "setup_quality": 92.0,
                    }
                ]
            ),
            "stock_scan": stock_scan,
            "sector_dashboard": sector_dashboard,
            "__dashboard_payload__": self._build_dashboard_payload(
                context=context,
                ranked_df=ranked,
                breakout_df=pd.DataFrame(
                    [
                        {
                            "symbol_id": "SMOKE",
                            "sector": "Smoke Sector",
                            "breakout_tag": "range_breakout_volume_supertrend",
                            "setup_quality": 92.0,
                        }
                    ]
                ),
                stock_scan_df=stock_scan,
                sector_dashboard_df=sector_dashboard,
                warnings=[],
            ),
        }

    def _build_dashboard_payload(
        self,
        context: StageContext,
        ranked_df: pd.DataFrame,
        breakout_df: pd.DataFrame,
        stock_scan_df: pd.DataFrame,
        sector_dashboard_df: pd.DataFrame,
        warnings: list[str],
    ) -> Dict[str, object]:
        """Assemble a unified operator payload from the rank-stage artifacts."""
        def _records(df: pd.DataFrame, limit: int = 10) -> list[dict]:
            if df is None or df.empty:
                return []
            return df.head(limit).to_dict(orient="records")

        top_sector = None
        if not sector_dashboard_df.empty:
            sector_col = "Sector" if "Sector" in sector_dashboard_df.columns else sector_dashboard_df.columns[0]
            top_sector = sector_dashboard_df.iloc[0].get(sector_col)

        return {
            "summary": {
                "run_id": context.run_id,
                "run_date": context.run_date,
                "ranked_count": int(len(ranked_df)),
                "breakout_count": int(len(breakout_df)),
                "stock_scan_count": int(len(stock_scan_df)),
                "sector_count": int(len(sector_dashboard_df)),
                "top_symbol": ranked_df.iloc[0]["symbol_id"] if not ranked_df.empty and "symbol_id" in ranked_df.columns else None,
                "top_sector": top_sector,
            },
            "ranked_signals": _records(ranked_df, limit=10),
            "breakout_scan": _records(breakout_df, limit=10),
            "stock_scan": _records(stock_scan_df, limit=10),
            "sector_dashboard": _records(sector_dashboard_df, limit=10),
            "warnings": warnings,
        }
