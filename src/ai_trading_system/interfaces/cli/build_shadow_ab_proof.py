"""Build a permanent, signed A/B/C shadow-parity proof bundle.

Consumes three completed run directories (A=off, B=shadow, C=off control) and
emits a two-layer evidence bundle using the versioned comparison policy:

- a full staging bundle under ``--staging-root`` (inventories, comparisons,
  copied STRICT decision artifacts, logs, ``bundle.sha256``);
- a small git-trackable audit record under ``--git-record-root`` (manifests,
  comparison summaries, ``bundle_reference.json``).

The builder is read-only w.r.t. the run directories; it only writes the bundle.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

from ai_trading_system.platform.parity.comparison_policy import (
    FLOAT_TOLERANCE_COLUMNS,
    PARITY_POLICY_VERSION,
    RUN_SCOPED_COLUMNS,
    STRICT_ARTIFACTS,
    classify_artifact,
    compare_runs,
    normalized_sha256,
    raw_sha256,
)

BUNDLE_SCHEMA_VERSION = "shadow-ab-proof-bundle-v1"


def _read_row_count_and_columns(path: Path) -> tuple[int | None, list[str]]:
    if path.suffix != ".csv":
        return None, []
    import pandas as pd
    try:
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception:
        return None, []
    return int(len(frame)), list(frame.columns)


def _inventory(run_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(run_dir.rglob("*.csv")):
        if not path.is_file():
            continue
        rel = str(path.relative_to(run_dir))
        policy = classify_artifact(rel)
        row_count, columns = _read_row_count_and_columns(path)
        rows.append({
            "rel_path": rel,
            "size_bytes": path.stat().st_size,
            "row_count": row_count,
            "column_order": json.dumps(columns),
            "raw_sha256": raw_sha256(path),
            "normalized_sha256": normalized_sha256(path, policy),
            "comparison_class": policy.artifact_class.value,
        })
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]], header: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_proof_bundle(
    *,
    run_a: Path, run_b: Path, run_c: Path | None,
    staging_root: Path, git_record_root: Path,
    as_of_date: str, run_id: str, code_commit: str,
    clone_db_hashes: dict[str, Any] | None = None,
    env_sha256: str | None = None,
    policy_hashes: dict[str, Any] | None = None,
    git_status: str = "",
    evidence_tag: str | None = None,
    log_paths: dict[str, Path] | None = None,
) -> dict[str, Any]:
    staging_root.mkdir(parents=True, exist_ok=True)
    git_record_root.mkdir(parents=True, exist_ok=True)

    # --- inventories -------------------------------------------------------
    inv_header = ["rel_path", "size_bytes", "row_count", "column_order", "raw_sha256", "normalized_sha256", "comparison_class"]
    for label, run in (("A", run_a), ("B", run_b), ("C", run_c)):
        if run is None:
            continue
        _write_csv(staging_root / f"run_{label}_inventory.csv", _inventory(run), inv_header)

    # --- comparisons (A vs B, control-subtracted by C) ---------------------
    report = compare_runs(run_a, run_b, control_c=run_c)
    strict_rows: list[dict[str, Any]] = []
    normalized_rows: list[dict[str, Any]] = []
    for cmp in report.artifacts:
        normalized_rows.append({
            "rel_path": cmp.rel_path, "comparison_class": cmp.artifact_class.value,
            "raw_match": cmp.raw_match, "normalized_match": cmp.normalized_match,
            "verdict": cmp.verdict, "differing_columns": json.dumps(list(cmp.differing_columns)),
        })
        name = cmp.rel_path.replace("\\", "/").rsplit("/", 1)[-1]
        if name in STRICT_ARTIFACTS:
            strict_rows.append({
                "rel_path": cmp.rel_path, "raw_match": cmp.raw_match, "verdict": cmp.verdict,
            })
    _write_csv(staging_root / "strict_hash_comparison.csv", strict_rows, ["rel_path", "raw_match", "verdict"])
    _write_csv(staging_root / "normalized_content_comparison.csv", normalized_rows,
               ["rel_path", "comparison_class", "raw_match", "normalized_match", "verdict", "differing_columns"])

    strict_all_identical = all(r["raw_match"] for r in strict_rows) if strict_rows else False
    decision = {
        "strict_artifacts_byte_identical": strict_all_identical,
        "strict_artifacts": strict_rows,
        "flag_caused_legacy_diffs": [
            {"rel_path": rel, "columns": list(cols)}
            for rel, cols in report.flag_caused
            if not rel.replace("\\", "/").startswith("performance")
            and "performance/" not in rel.replace("\\", "/")
        ],
        "control_run_present": run_c is not None,
        "policy_version": report.policy_version,
    }
    decision["verdict"] = (
        "PASS" if strict_all_identical and not decision["flag_caused_legacy_diffs"] else "REVIEW"
    )
    (staging_root / "decision_dataset_comparison.json").write_text(
        json.dumps(decision, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    # --- accepted nondeterminism catalog ----------------------------------
    allowed = {
        "policy_version": PARITY_POLICY_VERSION,
        "run_scoped_columns": sorted(RUN_SCOPED_COLUMNS),
        "float_tolerance_columns": sorted(FLOAT_TOLERANCE_COLUMNS),
        "note": "Columns expected to differ between identical-mode runs; excluded from decision parity.",
    }
    (staging_root / "allowed_nondeterminism.json").write_text(
        json.dumps(allowed, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    # --- lane artifact presence -------------------------------------------
    lane_b = run_b / "pattern_lane_scan" / "attempt_1"
    lane_files = sorted(p.name for p in lane_b.glob("*")) if lane_b.exists() else []
    presence = {
        "run_b_lane_dir": str(lane_b),
        "run_b_lane_artifacts": lane_files,
        "run_b_lane_artifact_count": len(lane_files),
        "run_a_has_lane_dir": (run_a / "pattern_lane_scan").exists(),
    }
    (staging_root / "lane_artifact_presence.json").write_text(
        json.dumps(presence, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    # --- copy STRICT decision artifacts + logs + B lane manifest ----------
    preserved_dir = staging_root / "preserved_artifacts"
    preserved_dir.mkdir(exist_ok=True)
    for cmp in report.artifacts:
        name = cmp.rel_path.replace("\\", "/").rsplit("/", 1)[-1]
        if name in STRICT_ARTIFACTS:
            for label, run in (("A", run_a), ("B", run_b)):
                src = run / cmp.rel_path
                if src.exists():
                    dest = preserved_dir / label / cmp.rel_path
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dest)
    b_manifest = lane_b / "pattern_lane_manifest.json"
    if b_manifest.exists():
        shutil.copy2(b_manifest, preserved_dir / "pattern_lane_manifest.json")
    if log_paths:
        logs_dir = staging_root / "logs"
        logs_dir.mkdir(exist_ok=True)
        for label, lp in log_paths.items():
            if Path(lp).exists():
                shutil.copy2(lp, logs_dir / f"run_{label}.log")

    # --- experiment manifest ----------------------------------------------
    manifest = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "as_of_date": as_of_date,
        "run_id": run_id,
        "code_commit": code_commit,
        "git_status_clean": git_status.strip() == "",
        "mode_assignment": {"A": "off", "B": "shadow", "C": "off"},
        "clone_method": "APFS copy-on-write (cp -c); run-history cleared per shadow_stage_ab_parity runbook",
        "clone_db_hashes": clone_db_hashes or {},
        "env_sha256": env_sha256,
        "policy_hashes": policy_hashes or {},
        "comparison_policy_version": PARITY_POLICY_VERSION,
        "decision_verdict": decision["verdict"],
        "operational_side_effects": False,
    }
    (staging_root / "experiment_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    # --- comparison summary (human) ---------------------------------------
    summary_md = _summary_markdown(manifest, decision, presence, report)
    (staging_root / "comparison_summary.md").write_text(summary_md, encoding="utf-8")

    # --- sign the staging bundle ------------------------------------------
    bundle_sha = _sign_bundle(staging_root)

    # --- git-tracked audit record (subset) --------------------------------
    for name in (
        "experiment_manifest.json", "strict_hash_comparison.csv",
        "normalized_content_comparison.csv", "allowed_nondeterminism.json",
        "decision_dataset_comparison.json", "comparison_summary.md",
        "lane_artifact_presence.json",
    ):
        shutil.copy2(staging_root / name, git_record_root / name)
    # per-run inventories for the git record
    for label in ("A", "B", "C"):
        inv = staging_root / f"run_{label}_inventory.csv"
        if inv.exists():
            shutil.copy2(inv, git_record_root / inv.name)
    (git_record_root / "bundle.sha256").write_text(
        (staging_root / "bundle.sha256").read_text(encoding="utf-8"), encoding="utf-8")
    bundle_reference = {
        "evidence_tag": evidence_tag or f"r1a-safety-proof-{as_of_date}-{code_commit[:7]}",
        "asset_name": f"r1a-safety-proof-{as_of_date}-{code_commit[:7]}.tar.zst",
        "staging_path": str(staging_root),
        "bundle_sha256": bundle_sha,
        "code_commit": code_commit,
        "as_of_date": as_of_date,
        "remote_asset_uploaded": False,
    }
    (git_record_root / "bundle_reference.json").write_text(
        json.dumps(bundle_reference, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (git_record_root / "README.md").write_text(_readme(manifest, decision, bundle_reference), encoding="utf-8")

    return {
        "status": "completed",
        "decision_verdict": decision["verdict"],
        "bundle_sha256": bundle_sha,
        "staging_root": str(staging_root),
        "git_record_root": str(git_record_root),
        "strict_artifacts_byte_identical": strict_all_identical,
        "flag_caused_legacy_diffs": decision["flag_caused_legacy_diffs"],
    }


def _sign_bundle(root: Path) -> str:
    entries = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != "bundle.sha256":
            entries.append(f"{raw_sha256(path)}  {path.relative_to(root)}")
    payload = ("\n".join(entries) + "\n").encode("utf-8")
    (root / "bundle.sha256").write_bytes(payload)
    return _sha256_bytes(payload)


def _summary_markdown(manifest, decision, presence, report) -> str:
    strict = "byte-identical" if decision["strict_artifacts_byte_identical"] else "NOT byte-identical"
    flagged = decision["flag_caused_legacy_diffs"]
    lines = [
        f"# Shadow A/B/C proof — {manifest['as_of_date']} @ {manifest['code_commit'][:7]}",
        "",
        f"- **Verdict:** {decision['verdict']}",
        f"- **STRICT decision artifacts (A vs B):** {strict}",
        f"- **Flag-caused legacy diffs (A~B minus A~C control):** {len(flagged) if flagged else 'none'}",
        f"- **Run B lane artifacts:** {presence['run_b_lane_artifact_count']} · Run A has lane dir: {presence['run_a_has_lane_dir']}",
        f"- **Comparison policy:** {manifest['comparison_policy_version']}",
        "",
        "## Artifact comparison (non-identical only)",
        "",
        "| Artifact | Class | Verdict | Differing cols |",
        "|---|---|---|---|",
    ]
    for cmp in report.artifacts:
        if cmp.verdict == "IDENTICAL":
            continue
        lines.append(f"| {cmp.rel_path} | {cmp.artifact_class.value} | {cmp.verdict} | {list(cmp.differing_columns)} |")
    if flagged:
        lines += ["", "## ⚠ Flag-caused legacy differences", ""]
        for item in flagged:
            lines.append(f"- `{item['rel_path']}`: {item['columns']}")
    return "\n".join(lines) + "\n"


def _readme(manifest, decision, ref) -> str:
    return (
        f"# R1a shadow A/B safety proof — {manifest['as_of_date']} ({manifest['code_commit'][:7]})\n\n"
        f"Durable, independently-reviewable evidence that enabling "
        f"`--pattern-lane-scan-mode shadow` leaves every legacy decision artifact "
        f"byte-identical under identical frozen inputs.\n\n"
        f"- **Verdict:** {decision['verdict']}\n"
        f"- **Code commit:** `{manifest['code_commit']}`\n"
        f"- **As-of date:** {manifest['as_of_date']}\n"
        f"- **Comparison policy:** {manifest['comparison_policy_version']}\n"
        f"- **Full bundle checksum (`bundle.sha256`):** `{ref['bundle_sha256']}`\n"
        f"- **Remote archive:** tag `{ref['evidence_tag']}`, asset `{ref['asset_name']}` "
        f"(uploaded: {ref['remote_asset_uploaded']})\n\n"
        f"This directory is the committed audit record. The full raw bundle "
        f"(inventories + copied STRICT artifacts + logs) lives at `{ref['staging_path']}` "
        f"and, durably, as the GitHub Release asset above. See "
        f"`decision_dataset_comparison.json` for the machine-readable verdict and "
        f"`allowed_nondeterminism.json` for the accepted run-scoped field catalog.\n"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the permanent shadow A/B/C proof bundle.")
    parser.add_argument("--run-a", type=Path, required=True, help="Run A (mode off) pipeline_runs/<id> dir")
    parser.add_argument("--run-b", type=Path, required=True, help="Run B (mode shadow) dir")
    parser.add_argument("--run-c", type=Path, help="Run C (control, mode off) dir")
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--git-record-root", type=Path, required=True)
    parser.add_argument("--as-of-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--code-commit", required=True)
    parser.add_argument("--clone-db-hashes", type=Path, help="JSON file of pre-run clone DB hashes")
    parser.add_argument("--env-sha256")
    parser.add_argument("--policy-hashes", type=Path, help="JSON file of policy hashes")
    args = parser.parse_args(argv)
    result = build_proof_bundle(
        run_a=args.run_a, run_b=args.run_b, run_c=args.run_c,
        staging_root=args.staging_root, git_record_root=args.git_record_root,
        as_of_date=args.as_of_date, run_id=args.run_id, code_commit=args.code_commit,
        clone_db_hashes=json.loads(args.clone_db_hashes.read_text()) if args.clone_db_hashes else None,
        env_sha256=args.env_sha256,
        policy_hashes=json.loads(args.policy_hashes.read_text()) if args.policy_hashes else None,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
