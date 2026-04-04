"""Target and label contracts for ML training datasets."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal


TargetKind = Literal["top_quantile", "positive_return", "meta_label"]


@dataclass(frozen=True)
class TargetSpec:
    """Defines the target used for model training and evaluation."""

    horizon: int
    kind: TargetKind = "top_quantile"
    version: str = "v1"
    quantile: float = 0.6
    threshold: float = 0.0
    meta_label_name: str | None = None

    @property
    def target_column(self) -> str:
        if self.kind == "meta_label" and self.meta_label_name:
            return self.meta_label_name
        return f"target_{self.horizon}d"

    def to_metadata(self) -> dict:
        payload = asdict(self)
        payload["target_column"] = self.target_column
        return payload
