"""Canonical feature schema helpers for research and operational ML flows."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
from typing import Iterable, Sequence

import pandas as pd


@dataclass(frozen=True)
class FeatureSchema:
    """Defines the canonical ML feature contract for dataset assembly."""

    version: str = "v1"
    required_base_columns: tuple[str, ...] = (
        "symbol_id",
        "exchange",
        "timestamp",
        "close",
        "open",
        "high",
        "low",
        "volume",
    )
    excluded_columns: tuple[str, ...] = (
        "parquet_file",
        "ingestion_version",
        "ingestion_ts",
        "date",
        "trade_date",
    )
    target_prefixes: tuple[str, ...] = (
        "target_",
        "return_",
        "forward_return_",
        "signal_",
        "meta_label_",
    )
    allowed_dtypes: tuple[str, ...] = (
        "float64",
        "float32",
        "float16",
        "int64",
        "int32",
        "int16",
        "uint64",
        "uint32",
        "uint16",
        "bool",
    )
    required_features: tuple[str, ...] = field(default_factory=tuple)

    def validate_base_columns(self, df: pd.DataFrame) -> None:
        missing = [column for column in self.required_base_columns if column not in df.columns]
        if missing:
            raise ValueError(f"Dataset is missing required base columns: {missing}")

    def feature_columns(
        self,
        df: pd.DataFrame,
        *,
        preferred: Sequence[str] | None = None,
        target_column: str | None = None,
    ) -> list[str]:
        """Return the ordered feature columns for a frame."""
        self.validate_base_columns(df)
        if preferred is not None:
            columns = [column for column in preferred if column in df.columns]
        else:
            columns = []
            for column in df.columns:
                if column in self.required_base_columns or column in self.excluded_columns:
                    continue
                if target_column and column == target_column:
                    continue
                lowered = column.lower()
                if any(lowered.startswith(prefix) for prefix in self.target_prefixes):
                    continue
                dtype_name = str(df[column].dtype)
                if dtype_name in self.allowed_dtypes:
                    columns.append(column)

        if self.required_features:
            missing_required = [column for column in self.required_features if column not in columns]
            if missing_required:
                raise ValueError(
                    f"Dataset is missing required feature columns from schema {self.version}: "
                    f"{missing_required}"
                )
        return columns

    def schema_hash(self, feature_columns: Iterable[str]) -> str:
        ordered = ",".join(sorted(feature_columns))
        return hashlib.sha256(ordered.encode("utf-8")).hexdigest()


DEFAULT_FEATURE_SCHEMA = FeatureSchema()
