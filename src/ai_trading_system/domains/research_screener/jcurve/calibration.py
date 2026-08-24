from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.domains.research_screener.store import content_hash

from .store import JCurveStore


CALIBRATION_POLICY_VERSION = "jcurve-calibration-policy-v1"


class JCurveCalibrationService:
    """Compute inspectable quality/cost metrics against a reviewed 25-company set."""

    def __init__(self, *, store_path: Path, output_root: Path):
        self.store = JCurveStore(store_path)
        self.output_root = Path(output_root)

    def run(
        self, *, evaluation_run_id: str, labels_path: Path,
        allow_nonstandard_cohort: bool = False,
    ) -> dict[str, Any]:
        if not self.store.completed_run(evaluation_run_id):
            raise ValueError("evaluation run must be completed")
        result_path = self.output_root / evaluation_run_id / "result.json"
        if not result_path.is_file():
            raise ValueError("evaluation result pack is missing")
        result = json.loads(result_path.read_text(encoding="utf-8"))
        labels = json.loads(labels_path.read_text(encoding="utf-8"))
        self._validate_labels(labels)
        company_count = len({row["company_id"] for row in labels})
        if company_count != 25 and not allow_nonstandard_cohort:
            raise ValueError(f"calibration requires exactly 25 companies; observed {company_count}")
        expected = {(row["announcement_id"], row["claim_type"]): row for row in labels if row["expected_present"]}
        actual_rows = [row for row in result.get("claims", []) if row.get("evidence_state") == "PRESENT" and row.get("status") != "AGENT_REJECTED"]
        actual = {(row["announcement_id"], row["claim_type"]): row for row in actual_rows}
        matched = sorted(set(expected) & set(actual))
        true_positive = len(matched)
        precision = true_positive / len(actual) if actual else 1.0 if not expected else 0.0
        recall = true_positive / len(expected) if expected else 1.0
        page_matches = sum(actual[key].get("page") == expected[key].get("page") for key in matched)
        numeric_candidates = [key for key in matched if expected[key].get("numeric_value") is not None]
        numeric_matches = sum(
            self._numeric_equal(actual[key].get("numeric_value"), expected[key].get("numeric_value"))
            and actual[key].get("unit") == expected[key].get("unit")
            for key in numeric_candidates
        )
        calls = result.get("calls", [])
        reviews = result.get("reviews", [])
        metrics = {
            "company_count": company_count,
            "expected_claims": len(expected), "accepted_claims": len(actual),
            "precision": precision, "recall": recall,
            "unsupported_fact_rate": (len(set(actual) - set(expected)) / len(actual)) if actual else 0.0,
            "page_accuracy": page_matches / len(matched) if matched else None,
            "numeric_unit_accuracy": numeric_matches / len(numeric_candidates) if numeric_candidates else None,
            "schema_compliance_rate": sum(bool(row.get("schema_valid")) for row in calls) / len(calls) if calls else None,
            "extractor_verifier_disagreement_rate": sum(row.get("decision") != "ACCEPT" for row in reviews) / len(reviews) if reviews else None,
            "input_tokens": sum(int(row.get("input_tokens") or 0) for row in calls),
            "output_tokens": sum(int(row.get("output_tokens") or 0) for row in calls),
            "reported_cost_usd": sum(float(row.get("cost_usd") or 0) for row in calls),
            "request_elapsed_ms": sum(
                int(attempt.get("elapsed_ms") or 0)
                for row in calls for attempt in row.get("retry_history", [])
            ),
        }
        snapshot_hash = content_hash({
            "evaluation": result.get("snapshot_hash"), "labels": labels,
            "policy": CALIBRATION_POLICY_VERSION, "metrics": metrics,
        })
        run_id = f"jcurve-calibration-{snapshot_hash[:20]}"
        output_dir = self.output_root / run_id
        if self.store.completed_run(run_id) and output_dir.is_dir():
            return {"run_id": run_id, "status": "COMPLETED", "reused": True, "metrics": metrics}
        output_dir.mkdir(parents=True, exist_ok=False)
        report = {
            "run_id": run_id, "evaluation_run_id": evaluation_run_id,
            "policy_version": CALIBRATION_POLICY_VERSION,
            "labels_hash": content_hash(labels), "metrics": metrics,
            "false_positive_keys": sorted([list(key) for key in set(actual) - set(expected)]),
            "false_negative_keys": sorted([list(key) for key in set(expected) - set(actual)]),
        }
        (output_dir / "calibration_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        started = datetime.now(UTC)
        self.store.persist_evaluation({
            "run_id": run_id, "run_type": "CALIBRATION", "parent_run_id": evaluation_run_id,
            "as_of_date": date.fromisoformat(str(result["as_of_date"])),
            "policy_version": CALIBRATION_POLICY_VERSION,
            "policy_hash": content_hash(CALIBRATION_POLICY_VERSION), "snapshot_hash": snapshot_hash,
            "announcement_count": len({row["announcement_id"] for row in labels}),
            "claim_count": len(actual), "episode_count": 0, "degraded_reason": None,
            "started_at": started, "calls": [], "claims": [], "reviews": [],
            "episodes": [], "episode_evidence": [], "observations": [],
        })
        return {"run_id": run_id, "status": "COMPLETED", "reused": False, "output_dir": str(output_dir), "metrics": metrics}

    @staticmethod
    def _validate_labels(labels: Any) -> None:
        if not isinstance(labels, list) or not labels:
            raise ValueError("calibration labels must be a non-empty JSON array")
        required = {"company_id", "announcement_id", "claim_type", "expected_present", "numeric_value", "unit", "page"}
        for row in labels:
            if set(row) != required or not isinstance(row["expected_present"], bool):
                raise ValueError("calibration label rows have an invalid shape")

    @staticmethod
    def _numeric_equal(left: Any, right: Any) -> bool:
        try:
            expected = float(right)
            return abs(float(left) - expected) <= max(1e-9, abs(expected) * 1e-6)
        except (TypeError, ValueError):
            return False
