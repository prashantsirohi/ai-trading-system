"""Prepared dataset builder for research and operational ML workflows."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Any, Dict, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd

from analytics.alpha.feature_schema import DEFAULT_FEATURE_SCHEMA, FeatureSchema
from analytics.alpha.labeling import TargetSpec
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.platform.logging.logger import logger


@dataclass(frozen=True)
class PreparedDataset:
    dataset_path: Path
    metadata_path: Path
    dataset_ref: str
    row_count: int
    symbol_count: int


class AlphaDatasetBuilder:
    """Build reproducible OHLCV+feature training datasets for ML workflows."""

    def __init__(
        self,
        project_root: str | Path,
        data_domain: str = "research",
        feature_schema: FeatureSchema | None = None,
        registry=None,
    ):
        self.project_root = Path(project_root)
        self.paths = ensure_domain_layout(project_root=self.project_root, data_domain=data_domain)
        self.data_domain = data_domain
        self.feature_schema = feature_schema or DEFAULT_FEATURE_SCHEMA
        self.registry = registry

    def prepare(
        self,
        *,
        engine,
        dataset_name: str,
        from_date: str,
        to_date: str,
        horizon: int,
        validation_fraction: float = 0.2,
        exchange: str = "NSE",
        target_spec: Optional[TargetSpec] = None,
        register_dataset: bool = False,
        extra_metadata: Optional[Dict[str, Any]] = None,
    ) -> PreparedDataset:
        target_spec = target_spec or TargetSpec(horizon=horizon)
        df = engine.prepare_training_data(
            from_date=from_date,
            to_date=to_date,
            exchange=exchange,
            horizons=[target_spec.horizon],
        ).copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values(["timestamp", "symbol_id"]).reset_index(drop=True)
        df = self._enrich_features(df, horizon=target_spec.horizon)

        feature_cols = self._resolve_feature_columns(engine, df, target_spec=target_spec)
        unique_dates = sorted(df["timestamp"].dt.normalize().unique())
        if not unique_dates:
            raise ValueError("Prepared dataset is empty; no timestamps available.")

        split_index = max(1, int(len(unique_dates) * (1 - validation_fraction)))
        validation_start = pd.Timestamp(unique_dates[min(split_index, len(unique_dates) - 1)])

        dataset_path = self.paths.dataset_dir / f"{dataset_name}.parquet"
        metadata_path = self.paths.dataset_dir / f"{dataset_name}.metadata.json"
        df.to_parquet(dataset_path, index=False)

        metadata: Dict[str, Any] = {
            "dataset_ref": f"{self.data_domain}:training:{dataset_name}",
            "dataset_uri": str(dataset_path),
            "engine_name": getattr(engine, "engine_name", "unknown"),
            "data_domain": self.data_domain,
            "exchange": exchange,
            "from_date": from_date,
            "to_date": to_date,
            "horizon": target_spec.horizon,
            "row_count": int(len(df)),
            "symbol_count": int(df["symbol_id"].nunique()),
            "feature_count": int(len(feature_cols)),
            "feature_columns": feature_cols,
            "feature_schema_version": self.feature_schema.version,
            "feature_schema_hash": self.feature_schema.schema_hash(feature_cols),
            "target_column": target_spec.target_column,
            "target_spec": target_spec.to_metadata(),
            "target_version": target_spec.version,
            "validation_fraction": validation_fraction,
            "train_end": str((validation_start - pd.Timedelta(days=1)).date()),
            "validation_start": str(validation_start.date()),
            "label_positive_rate": (
                float(df[target_spec.target_column].mean())
                if target_spec.target_column in df.columns
                else None
            ),
        }
        if extra_metadata:
            metadata.update(extra_metadata)

        if register_dataset:
            metadata["dataset_id"] = self._register_dataset(metadata)

        metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

        logger.info(
            "Prepared training dataset ref=%s rows=%s symbols=%s path=%s",
            metadata["dataset_ref"],
            metadata["row_count"],
            metadata["symbol_count"],
            dataset_path,
        )
        return PreparedDataset(
            dataset_path=dataset_path,
            metadata_path=metadata_path,
            dataset_ref=metadata["dataset_ref"],
            row_count=metadata["row_count"],
            symbol_count=metadata["symbol_count"],
        )

    def _register_dataset(self, metadata: Dict[str, Any]) -> str:
        if self.registry is None:
            from analytics.registry import RegistryStore

            self.registry = RegistryStore(self.project_root)
        return self.registry.register_dataset(
            dataset_ref=metadata["dataset_ref"],
            dataset_uri=metadata["dataset_uri"],
            data_domain=metadata["data_domain"],
            engine_name=metadata["engine_name"],
            feature_schema_version=metadata["feature_schema_version"],
            feature_schema_hash=metadata["feature_schema_hash"],
            label_version=metadata["target_version"],
            target_column=metadata["target_column"],
            from_date=metadata["from_date"],
            to_date=metadata["to_date"],
            horizon=metadata["horizon"],
            row_count=metadata["row_count"],
            symbol_count=metadata["symbol_count"],
            metadata=metadata,
        )

    def _resolve_feature_columns(self, engine, df: pd.DataFrame, *, target_spec: TargetSpec) -> list[str]:
        preferred = None
        if hasattr(engine, "_feature_cols"):
            preferred = list(engine._feature_cols(df))
        return self.feature_schema.feature_columns(
            df,
            preferred=preferred,
            target_column=target_spec.target_column,
        )

    def _enrich_features(self, df: pd.DataFrame, *, horizon: int) -> pd.DataFrame:
        enriched = df.copy()
        enriched = self._add_price_structure_features(enriched)
        enriched = self._add_delivery_features(enriched)
        enriched = self._add_sector_features(enriched)
        enriched = self._add_regime_features(enriched)
        enriched = self._add_cross_sectional_features(enriched)
        target_col = f"target_{horizon}d"
        if target_col in enriched.columns:
            enriched = enriched.dropna(subset=[target_col])
        return enriched

    def _add_price_structure_features(self, df: pd.DataFrame) -> pd.DataFrame:
        ordered = df.sort_values(["symbol_id", "timestamp"]).copy()
        by_symbol = ordered.groupby("symbol_id", group_keys=False)

        ordered["ret_5d_back"] = by_symbol["close"].pct_change(5)
        ordered["ret_20d_back"] = by_symbol["close"].pct_change(20)
        ordered["ret_60d_back"] = by_symbol["close"].pct_change(60)
        ordered["volume_avg_20"] = by_symbol["volume"].transform(
            lambda series: series.shift(1).rolling(20, min_periods=5).mean()
        )
        ordered["volume_ratio_20"] = ordered["volume"] / ordered["volume_avg_20"].replace(0, np.nan)
        ordered["volatility_20"] = by_symbol["close"].transform(
            lambda series: series.pct_change().rolling(20, min_periods=10).std()
        )
        ordered["volatility_60"] = by_symbol["close"].transform(
            lambda series: series.pct_change().rolling(60, min_periods=20).std()
        )

        ordered["sma_20"] = by_symbol["close"].transform(
            lambda series: series.rolling(20, min_periods=5).mean()
        )
        ordered["sma_50"] = by_symbol["close"].transform(
            lambda series: series.rolling(50, min_periods=10).mean()
        )
        ordered["sma_200"] = by_symbol["close"].transform(
            lambda series: series.rolling(200, min_periods=30).mean()
        )
        ordered["dist_sma_20"] = (
            (ordered["close"] - ordered["sma_20"]) / ordered["sma_20"].replace(0, np.nan)
        )
        ordered["dist_sma_50"] = (
            (ordered["close"] - ordered["sma_50"]) / ordered["sma_50"].replace(0, np.nan)
        )
        ordered["dist_sma_200"] = (
            (ordered["close"] - ordered["sma_200"]) / ordered["sma_200"].replace(0, np.nan)
        )

        ordered["high_252"] = by_symbol["high"].transform(
            lambda series: series.rolling(252, min_periods=20).max()
        )
        ordered["dist_52w_high"] = (
            1 - (ordered["close"] / ordered["high_252"].replace(0, np.nan))
        )
        ordered["prior_range_high_20"] = by_symbol["high"].transform(
            lambda series: series.shift(1).rolling(20, min_periods=5).max()
        )
        ordered["prior_range_low_20"] = by_symbol["low"].transform(
            lambda series: series.shift(1).rolling(20, min_periods=5).min()
        )
        ordered["range_width_pct_20"] = (
            (ordered["prior_range_high_20"] - ordered["prior_range_low_20"])
            / ordered["prior_range_low_20"].replace(0, np.nan)
        )
        ordered["breakout_pct_20"] = (
            (ordered["close"] - ordered["prior_range_high_20"])
            / ordered["prior_range_high_20"].replace(0, np.nan)
        )
        ordered["is_range_breakout_20"] = (
            ordered["close"] > ordered["prior_range_high_20"]
        ).astype(int)
        st_signal = (
            ordered["st_signal"]
            if "st_signal" in ordered.columns
            else pd.Series(0, index=ordered.index)
        )
        ordered["supertrend_bullish"] = (st_signal.fillna(0) > 0).astype(int)
        ordered["trend_alignment_score"] = (
            (ordered["dist_sma_20"] > 0).astype(int)
            + (ordered["dist_sma_50"] > 0).astype(int)
            + (ordered["dist_sma_200"] > 0).astype(int)
        )
        return ordered

    def _add_delivery_features(self, df: pd.DataFrame) -> pd.DataFrame:
        delivery_db = self.paths.ohlcv_db_path
        if not delivery_db.exists():
            df["delivery_pct"] = 20.0
            return df

        conn = duckdb.connect(str(delivery_db), read_only=True)
        try:
            delivery = conn.execute(
                """
                SELECT symbol_id, CAST(timestamp AS DATE) AS trade_date, delivery_pct
                FROM _delivery
                """
            ).fetchdf()
        except Exception:
            conn.close()
            df["delivery_pct"] = 20.0
            return df
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if delivery.empty:
            df["delivery_pct"] = 20.0
            return df

        delivery["trade_date"] = pd.to_datetime(delivery["trade_date"])
        delivery = delivery.sort_values(["symbol_id", "trade_date"])
        enriched = df.copy()
        enriched["trade_date"] = enriched["timestamp"].dt.normalize()
        enriched = enriched.merge(
            delivery[["symbol_id", "trade_date", "delivery_pct"]],
            on=["symbol_id", "trade_date"],
            how="left",
        )
        enriched["delivery_pct"] = enriched["delivery_pct"].fillna(20.0)
        return enriched.drop(columns=["trade_date"])

    def _load_sector_map(self) -> dict[str, str]:
        if not self.paths.master_db_path.exists():
            return {}
        conn = sqlite3.connect(self.paths.master_db_path.as_posix())
        try:
            rows = conn.execute(
                "SELECT Symbol, Sector FROM stock_details WHERE Symbol IS NOT NULL"
            ).fetchall()
        finally:
            conn.close()
        return {symbol: sector for symbol, sector in rows if sector}

    def _add_sector_features(self, df: pd.DataFrame) -> pd.DataFrame:
        all_symbols_dir = self.paths.feature_store_dir / "all_symbols"
        sector_rs_path = all_symbols_dir / "sector_rs.parquet"
        stock_vs_sector_path = all_symbols_dir / "stock_vs_sector.parquet"
        if not sector_rs_path.exists() or not stock_vs_sector_path.exists():
            df["sector_rs_value"] = 0.5
            df["stock_vs_sector_value"] = 0.0
            return df

        sector_map = self._load_sector_map()
        sector_rs = pd.read_parquet(sector_rs_path)
        stock_vs_sector = pd.read_parquet(stock_vs_sector_path)
        sector_rs.index = pd.to_datetime(sector_rs.index).normalize()
        stock_vs_sector.index = pd.to_datetime(stock_vs_sector.index).normalize()
        sector_rs = sector_rs[~sector_rs.index.duplicated(keep="last")]
        stock_vs_sector = stock_vs_sector[~stock_vs_sector.index.duplicated(keep="last")]

        enriched = df.copy()
        enriched["trade_date"] = enriched["timestamp"].dt.normalize()
        enriched["sector_name"] = enriched["symbol_id"].map(sector_map).fillna("Other")

        sector_long = (
            sector_rs.stack(dropna=False)
            .rename_axis(index=["trade_date", "sector_name"])
            .reset_index(name="sector_rs_value")
        )
        stock_long = (
            stock_vs_sector.stack(dropna=False)
            .rename_axis(index=["trade_date", "symbol_id"])
            .reset_index(name="stock_vs_sector_value")
        )
        sector_long["trade_date"] = pd.to_datetime(sector_long["trade_date"])
        stock_long["trade_date"] = pd.to_datetime(stock_long["trade_date"])

        enriched = enriched.merge(sector_long, on=["trade_date", "sector_name"], how="left")
        enriched = enriched.merge(stock_long, on=["trade_date", "symbol_id"], how="left")
        enriched["sector_rs_value"] = enriched["sector_rs_value"].fillna(0.5)
        enriched["stock_vs_sector_value"] = enriched["stock_vs_sector_value"].fillna(0.0)
        return enriched.drop(columns=["trade_date"])

    def _add_regime_features(self, df: pd.DataFrame) -> pd.DataFrame:
        enriched = df.copy()
        enriched["trade_date"] = enriched["timestamp"].dt.normalize()
        ordered = enriched.sort_values(["symbol_id", "trade_date"]).copy()

        ordered["sma50_for_regime"] = ordered.groupby("symbol_id")["close"].transform(
            lambda series: series.rolling(50, min_periods=10).mean()
        )
        ordered["sma200_for_regime"] = ordered.groupby("symbol_id")["close"].transform(
            lambda series: series.rolling(200, min_periods=30).mean()
        )
        ordered["close_20_back"] = ordered.groupby("symbol_id")["close"].shift(20)
        ordered["close_50_back"] = ordered.groupby("symbol_id")["close"].shift(50)

        by_trade_date = ordered.groupby("trade_date")
        regime = by_trade_date.agg(
            pct_above_50=("close", lambda series: 0.0),
        )
        regime["pct_above_50"] = ((ordered["close"] > ordered["sma50_for_regime"]).astype(float)).groupby(
            ordered["trade_date"]
        ).mean() * 100
        regime["pct_above_200"] = ((ordered["close"] > ordered["sma200_for_regime"]).astype(float)).groupby(
            ordered["trade_date"]
        ).mean() * 100
        regime["pct_up_20"] = ((ordered["close"] > ordered["close_20_back"]).astype(float)).groupby(
            ordered["trade_date"]
        ).mean() * 100
        regime["pct_up_50"] = ((ordered["close"] > ordered["close_50_back"]).astype(float)).groupby(
            ordered["trade_date"]
        ).mean() * 100
        regime["breadth_score"] = regime[
            ["pct_above_50", "pct_above_200", "pct_up_20", "pct_up_50"]
        ].mean(axis=1)

        enriched = enriched.merge(regime.reset_index(), on="trade_date", how="left")
        return enriched.drop(columns=["trade_date"])

    def _add_cross_sectional_features(self, df: pd.DataFrame) -> pd.DataFrame:
        enriched = df.copy()
        group = enriched.groupby("timestamp")

        if "ret_20d_back" in enriched.columns:
            enriched["rel_strength_pct"] = group["ret_20d_back"].rank(pct=True) * 100
        if "volume_ratio_20" in enriched.columns:
            enriched["vol_intensity_pct"] = group["volume_ratio_20"].rank(pct=True) * 100
        if "adx_value" in enriched.columns:
            enriched["trend_score_pct"] = group["adx_value"].rank(pct=True) * 100
        if "dist_52w_high" in enriched.columns:
            enriched["prox_high_pct"] = (1 - group["dist_52w_high"].rank(pct=True)) * 100
        if "delivery_pct" in enriched.columns:
            enriched["delivery_pct_pct"] = group["delivery_pct"].rank(pct=True) * 100
        if "sector_rs_value" in enriched.columns:
            enriched["sector_rs_pct"] = group["sector_rs_value"].rank(pct=True) * 100
        if "stock_vs_sector_value" in enriched.columns:
            enriched["stock_vs_sector_pct"] = group["stock_vs_sector_value"].rank(pct=True) * 100
        if "breakout_pct_20" in enriched.columns:
            enriched["breakout_strength_pct"] = group["breakout_pct_20"].rank(pct=True) * 100
        return enriched

    @staticmethod
    def load_prepared_dataset(dataset_uri: str | Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        dataset_path = Path(dataset_uri)
        metadata_path = dataset_path.with_suffix(".metadata.json")
        if not dataset_path.exists():
            raise FileNotFoundError(f"Prepared dataset not found: {dataset_path}")
        if not metadata_path.exists():
            raise FileNotFoundError(f"Prepared dataset metadata not found: {metadata_path}")

        df = pd.read_parquet(dataset_path)
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df, metadata
