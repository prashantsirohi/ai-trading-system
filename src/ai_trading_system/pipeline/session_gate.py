"""R1a shadow session gate — does a normal shadow run count toward the clock?

From Day 2 the three-flag production run (registry + scan-routing +
pattern-lane shadow) replaces the full A/B/C parity test. A session counts when
all eight conditions hold. This module reads a completed run's registered
pattern-lane artifacts plus the pipeline registry and emits a pass/fail verdict
plus reviewable artifacts. It performs no writes to any operational store.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from ai_trading_system.platform.telemetry.performance import PerformanceConfig

SESSION_GATE_POLICY_VERSION = "r1a-session-gate-v1"

EXPECTED_ARTIFACTS: tuple[str, ...] = (
    "pattern_lane_scan",
    "pattern_lane_summary",
    "pattern_lane_runtime",
    "pattern_lane_source_diagnostics",
    "pattern_lane_parity_report",
    "pattern_lane_manifest",
    "pattern_lane_shadow_report",
)


class GateStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass(frozen=True)
class GateCheck:
    check_id: str
    status: GateStatus
    required: bool
    observed: Any
    expected: str

    def as_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["status"] = self.status.value
        return row


@dataclass(frozen=True)
class SessionGateResult:
    run_id: str
    policy_version: str
    day_counts: bool
    status: GateStatus
    checks: tuple[GateCheck, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "policy_version": self.policy_version,
            "day_counts": self.day_counts,
            "status": self.status.value,
            "checks": [c.as_row() for c in self.checks],
        }


def _load_json(artifacts: dict[str, Any], artifact_type: str) -> dict[str, Any]:
    art = artifacts.get(artifact_type)
    if art is None:
        return {}
    try:
        return json.loads(Path(art.uri).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _is_non_blocking_shadow(stage_run: dict[str, Any] | None) -> bool:
    """A shadow stage that failed non-blockingly (orchestrator degraded path)."""
    if not stage_run:
        return False
    if stage_run.get("status") != "failed":
        return False
    checkpoint = stage_run.get("checkpoint") or {}
    return bool(isinstance(checkpoint, dict) and checkpoint.get("non_blocking"))


def evaluate_session_gate(
    registry: Any, run_id: str, *, perf_config: PerformanceConfig | None = None,
) -> SessionGateResult:
    """Evaluate the eight day-count conditions for ``run_id``."""
    perf_config = perf_config or PerformanceConfig()
    _, runtime_warn, runtime_fail = perf_config.runtime_threshold("pattern_lane_scan")

    status_map = registry.latest_stage_status_map(run_id)
    artifact_map = registry.get_artifact_map(run_id)
    lane_artifacts = artifact_map.get("pattern_lane_scan", {})
    summary = _load_json(lane_artifacts, "pattern_lane_summary")
    runtime = _load_json(lane_artifacts, "pattern_lane_runtime")
    parity = _load_json(lane_artifacts, "pattern_lane_parity_report")
    manifest = _load_json(lane_artifacts, "pattern_lane_manifest")

    checks: list[GateCheck] = []

    def add(check_id: str, ok: bool, observed: Any, expected: str, *, required: bool = True,
            warn: bool = False) -> None:
        if ok:
            status = GateStatus.WARN if warn else GateStatus.PASS
        else:
            status = GateStatus.FAIL if required else GateStatus.WARN
        checks.append(GateCheck(check_id, status, required, observed, expected))

    # 1. lane stage completed
    lane_status = (status_map.get("pattern_lane_scan") or {}).get("status")
    add("lane_stage_completed", lane_status == "completed", lane_status, "status == completed")

    # 2. all seven artifacts registered (promoted from a completed attempt)
    present = sorted(set(lane_artifacts))
    missing = [a for a in EXPECTED_ARTIFACTS if a not in lane_artifacts]
    add("seven_artifacts_registered", not missing, {"present": present, "missing": missing},
        f"all of {list(EXPECTED_ARTIFACTS)}")

    # 3. runtime passes
    wall = runtime.get("total_wall_seconds")
    runtime_ok = isinstance(wall, (int, float)) and wall <= runtime_fail
    runtime_warn_only = isinstance(wall, (int, float)) and runtime_warn < wall <= runtime_fail
    add("runtime_passes", runtime_ok, {"total_wall_seconds": wall, "fail_over": runtime_fail},
        f"total_wall_seconds <= {runtime_fail}", warn=runtime_warn_only)

    # 4. policy diagnostics pass
    policy_mismatch = summary.get("policy_mismatch_count")
    add("policy_diagnostics_pass", policy_mismatch == 0, policy_mismatch, "policy_mismatch_count == 0")

    # 5. source diagnostics present (fallback monitored, not blocking)
    fallback = summary.get("fallback_rate")
    add("source_diagnostics_present", "fallback_rate" in summary, {"fallback_rate": fallback},
        "fallback_rate recorded (monitored)", required=False,
        warn=isinstance(fallback, (int, float)) and fallback > 0.5)

    # 6. no stale evidence admitted as fresh
    stale = summary.get("stale_admitted_as_fresh_count")
    add("no_stale_as_fresh", stale == 0, stale, "stale_admitted_as_fresh_count == 0")

    # 7. no malformed signal rows
    malformed = summary.get("malformed_signal_rows")
    add("no_malformed_signal_rows", malformed == 0, malformed, "malformed_signal_rows == 0")

    # 8. registry + routing shadows complete (opportunities may degrade non-blockingly)
    weekly = (status_map.get("weekly_stage") or {}).get("status")
    router = (status_map.get("scan_router") or {}).get("status")
    opp_run = status_map.get("opportunities")
    opp_status = (opp_run or {}).get("status")
    opp_ok = opp_status == "completed" or _is_non_blocking_shadow(opp_run)
    shadows_ok = weekly == "completed" and router == "completed" and opp_ok
    add("registry_routing_shadows_complete", shadows_ok,
        {"weekly_stage": weekly, "scan_router": router, "opportunities": opp_status},
        "weekly_stage & scan_router completed; opportunities completed or non-blocking",
        warn=(weekly == "completed" and router == "completed" and opp_status != "completed" and opp_ok))

    # 9. no operational consumer changed
    side_effects = [
        summary.get("operational_side_effects"),
        parity.get("operational_side_effects"),
        manifest.get("operational_side_effects"),
    ]
    add("no_operational_consumer_changed", all(x is False for x in side_effects), side_effects,
        "operational_side_effects == false in summary/parity/manifest")

    required_fail = any(c.status is GateStatus.FAIL and c.required for c in checks)
    any_warn = any(c.status is GateStatus.WARN for c in checks)
    day_counts = not required_fail
    status = GateStatus.FAIL if required_fail else (GateStatus.WARN if any_warn else GateStatus.PASS)
    return SessionGateResult(
        run_id=run_id, policy_version=SESSION_GATE_POLICY_VERSION,
        day_counts=day_counts, status=status, checks=tuple(checks),
    )


def write_session_gate_artifacts(result: SessionGateResult, output_root: Path) -> tuple[Path, ...]:
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "session_gate_verdict.json"
    json_path.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    csv_path = output_root / "session_gate_checks.csv"
    import csv as _csv
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = _csv.writer(handle)
        writer.writerow(["check_id", "status", "required", "observed", "expected"])
        for c in result.checks:
            writer.writerow([c.check_id, c.status.value, c.required, json.dumps(c.observed, default=str), c.expected])
    md_path = output_root / "session_gate.md"
    lines = [
        f"# R1a session gate — {result.run_id}",
        "",
        f"- **Day counts:** {'YES' if result.day_counts else 'NO'}  ",
        f"- **Status:** {result.status.value}  ",
        f"- **Policy:** {result.policy_version}",
        "",
        "| Check | Status | Required | Observed |",
        "|---|---|---|---|",
    ]
    for c in result.checks:
        lines.append(f"| {c.check_id} | {c.status.value} | {c.required} | `{json.dumps(c.observed, default=str)}` |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, csv_path, md_path
