from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ai_trading_system.domains.research_screener.store import content_hash


@dataclass(frozen=True)
class MaterialityInputs:
    ttm_revenue_cr: float | None = None
    net_block_cr: float | None = None
    old_capacity: float | None = None


@dataclass(frozen=True)
class StageDecision:
    stage: str
    score: float
    confidence: float
    materiality_passed: bool
    materiality_metrics: dict[str, float | None]
    reason_codes: tuple[str, ...]
    evidence_ids: tuple[str, ...]


class CapexStagePolicy:
    def __init__(self, materiality: dict[str, Any], stage: dict[str, Any]):
        self.materiality = materiality
        self.stage = stage
        self.policy_version = stage["policy_version"]
        self.policy_hash = content_hash({"materiality": materiality, "stage": stage})

    @classmethod
    def from_root(cls, project_root: Path) -> "CapexStagePolicy":
        root = project_root / "configs/research_screener/jcurve"
        return cls(
            json.loads((root / "materiality_policy.json").read_text(encoding="utf-8")),
            json.loads((root / "stage_policy.json").read_text(encoding="utf-8")),
        )

    def evaluate(
        self, claims: list[dict[str, Any]], *, inputs: MaterialityInputs
    ) -> StageDecision:
        accepted = [
            claim for claim in claims
            if claim.get("status") in set(self.stage["accepted_claim_statuses"])
            and claim.get("evidence_state") == "PRESENT"
        ]
        by_type: dict[str, list[dict[str, Any]]] = {}
        for claim in accepted:
            by_type.setdefault(str(claim["claim_type"]), []).append(claim)

        capex_cr = self._numeric(
            by_type.get("CAPEX_AMOUNT", []) + by_type.get("CAPEX_ANNOUNCED", []),
            "INR_CRORE",
        )
        incremental_capacity = self._numeric(by_type.get("CAPACITY_INCREMENT", []), None)
        incremental_revenue = self._numeric(by_type.get("EXPECTED_INCREMENTAL_REVENUE", []), "INR_CRORE")
        metrics = {
            "capex_to_ttm_revenue": self._ratio(capex_cr, inputs.ttm_revenue_cr),
            "incremental_capacity_to_old_capacity": self._ratio(incremental_capacity, inputs.old_capacity),
            "capex_to_net_block": self._ratio(capex_cr, inputs.net_block_cr),
            "incremental_revenue_to_ttm_revenue": self._ratio(incremental_revenue, inputs.ttm_revenue_cr),
        }
        thresholds = self.materiality["thresholds"]
        passed_metrics = [
            name for name, value in metrics.items()
            if value is not None and value >= float(thresholds[name])
        ]
        trigger = any(self._affirmed(by_type.get(kind, [])) for kind in self.stage["required_trigger_claims"])
        demand = any(self._affirmed(by_type.get(kind, [])) for kind in self.stage["demand_path_claims"])
        materiality_passed = bool(passed_metrics)
        reasons: list[str] = []
        if not trigger:
            reasons.append("VERIFIED_TRIGGER_MISSING")
        if not demand:
            reasons.append("VERIFIED_DEMAND_PATH_MISSING")
        if not materiality_passed:
            reasons.append("MATERIALITY_NOT_PROVEN")
        if any(claim.get("status") == "HUMAN_REVIEW_REQUIRED" for claim in claims):
            reasons.append("HUMAN_REVIEW_PENDING")
        qualifies = trigger and demand and materiality_passed and "HUMAN_REVIEW_PENDING" not in reasons
        if qualifies:
            reasons.extend(f"MATERIALITY_{name.upper()}" for name in passed_metrics)
            reasons.append("J1_VERIFIED_MATERIAL_TRIGGER")
        evidence_ids = tuple(sorted(str(claim["claim_id"]) for claim in accepted))
        confidence = min((float(claim.get("confidence", 0)) for claim in accepted), default=0.0)
        score = min(100.0, 30.0 * trigger + 30.0 * demand + 30.0 * materiality_passed + 10.0 * confidence)
        return StageDecision(
            stage=self.stage["first_qualified_stage"] if qualifies else self.stage["candidate_stage"],
            score=score,
            confidence=confidence,
            materiality_passed=materiality_passed,
            materiality_metrics=metrics,
            reason_codes=tuple(sorted(set(reasons))),
            evidence_ids=evidence_ids,
        )

    @staticmethod
    def project_key(claims: list[dict[str, Any]], source_artifact_id: str) -> tuple[str, str | None, str | None]:
        names = {str(row.get("project_name") or "").strip() for row in claims if row.get("project_name")}
        locations = {str(row.get("project_location") or "").strip() for row in claims if row.get("project_location")}
        name = sorted(names)[0] if len(names) == 1 else None
        location = sorted(locations)[0] if len(locations) == 1 else None
        if name:
            normalized = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
            location_key = re.sub(r"[^a-z0-9]+", "-", (location or "").lower()).strip("-")
            return f"{normalized}:{location_key or 'unknown'}", name, location
        return f"unresolved:{content_hash(source_artifact_id)[:16]}", None, location

    @staticmethod
    def _numeric(rows: list[dict[str, Any]], required_unit: str | None) -> float | None:
        values = [
            float(row["numeric_value"]) for row in rows
            if row.get("numeric_value") is not None
            and (required_unit is None or row.get("unit") == required_unit)
        ]
        return max(values) if values else None

    @staticmethod
    def _ratio(numerator: float | None, denominator: float | None) -> float | None:
        if numerator is None or denominator is None or denominator <= 0:
            return None
        return numerator / denominator

    @staticmethod
    def _affirmed(rows: list[dict[str, Any]]) -> bool:
        return any(row.get("boolean_value") is not False for row in rows)
