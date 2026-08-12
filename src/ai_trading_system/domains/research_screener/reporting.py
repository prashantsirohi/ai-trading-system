from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd

from .store import canonical_json


PACK_FILES = (
    "P0_security_master.parquet",
    "P0_market_snapshot.parquet",
    "P0_fundamentals_annual.parquet",
    "P0_fundamentals_quarterly.parquet",
    "P0_ohlcv_adjusted.parquet",
    "P0_corporate_actions.parquet",
    "P0_data_quality.csv",
    "P0_data_repair_queue.csv",
    "P0_source_manifest.json",
    "canary_company_status.csv",
    "canary_decision_explanations.md",
    "canary_summary.md",
)
FULL_UNIVERSE_PACK_FILES = (
    *PACK_FILES[:9],
    "universe_company_status.csv",
    "universe_decision_explanations.md",
    "universe_summary.md",
)
FILING_DISCOVERY_PACK_FILES = (
    *PACK_FILES[:9],
    "filing_company_status.csv",
    "filing_decision_explanations.md",
    "filing_summary.md",
)


def write_output_pack(output_dir: Path, payload: dict) -> None:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"immutable run output already exists: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    source_dir = output_dir / "source"
    source_dir.mkdir(exist_ok=True)
    members = payload["members"]

    security_rows = []
    market_rows = []
    ohlcv_rows = []
    action_rows = []
    annual_rows = []
    quarterly_rows = []
    status_rows = []
    for member in members:
        identity = member["identity"]
        security_rows.append({
            "company_id": member["company_id"], "security_id": member["security_id"],
            "fixture_symbol": member["symbol"], "fixture_isin": member["isin"],
            "legal_name": member["company"], "identity_status": member["identity_status"],
            "observed_isins": json.dumps(identity.get("observed_isins", [])),
            "listings": json.dumps(identity.get("listings", []), default=str),
            "source_artifact_id": identity["source_artifact_id"],
        })
        market = member["inputs"].get("market", {})
        market_rows.append({"security_id": member["security_id"], "as_of_date": member["as_of_date"],
                            "full_market_cap_cr": member.get("market_cap_cr"), "market_cap_status": member["market_cap_status"],
                            **{k: market.get(k) for k in ("price_date", "raw_close", "adjusted_close", "provider", "freshness_status")}})
        for row in member["inputs"].get("ohlcv", []):
            ohlcv_rows.append({"security_id": member["security_id"], **row})
        for row in member["inputs"].get("corporate_actions", []):
            action_rows.append({"security_id": member["security_id"], "isin": member["isin"], "evidence_role": "STORED_ADJUSTMENT", **row})
        for row in member["inputs"].get("official_corporate_actions", []):
            action_rows.append({"security_id": member["security_id"], "isin": member["isin"], "evidence_role": "OFFICIAL_EXCHANGE", **row})
        fundamentals = member["inputs"].get("fundamentals", {})
        annual_rows.extend(_statement_rows(member, fundamentals, "annual"))
        quarterly_rows.extend(_statement_rows(member, fundamentals, "quarterly"))
        status_rows.append({
            "symbol": member["symbol"], "company": member["company"], "fixture_isin": member["isin"],
            "company_type": member.get("company_type"),
            "issuer_classification_state": member["inputs"].get("issuer_classification", {}).get("state"),
            "issuer_sector": member["inputs"].get("issuer_classification", {}).get("sector"),
            "issuer_industry": member["inputs"].get("issuer_classification", {}).get("industry"),
            "identity_status": member["identity_status"],
            "exchange_coverage": "+".join(sorted({x["exchange"] for x in identity.get("listings", [])})) or "UNRESOLVED",
            "market_cap_status": member["market_cap_status"], "market_cap_cr": member.get("market_cap_cr"),
            "statement_scope": member["statement_scope"], "annual_completeness": member.get("annual_completeness"),
            "quarterly_completeness": member.get("quarterly_completeness"),
            "corporate_action_status": member["corporate_action_status"], "data_confidence": member["data_confidence"],
            "fundamental_disposition": member["disposition"], "technical_status": member["technical_status"],
        })

    _parquet(output_dir / "P0_security_master.parquet", security_rows)
    _parquet(output_dir / "P0_market_snapshot.parquet", market_rows)
    _parquet(output_dir / "P0_fundamentals_annual.parquet", annual_rows)
    _parquet(output_dir / "P0_fundamentals_quarterly.parquet", quarterly_rows)
    _parquet(output_dir / "P0_ohlcv_adjusted.parquet", ohlcv_rows, empty_columns=["security_id", "trade_date"])
    _parquet(output_dir / "P0_corporate_actions.parquet", action_rows, empty_columns=["security_id", "isin", "action_type"])
    pd.DataFrame(payload.get("dq_issues", [])).to_csv(output_dir / "P0_data_quality.csv", index=False)
    pd.DataFrame(payload.get("repairs", [])).to_csv(output_dir / "P0_data_repair_queue.csv", index=False)
    full_universe = payload["run_mode"] == "full_universe"
    filing_discovery = payload["run_mode"] == "filing_discovery"
    status_name = "universe_company_status.csv" if full_universe else "filing_company_status.csv" if filing_discovery else "canary_company_status.csv"
    explanation_name = "universe_decision_explanations.md" if full_universe else "filing_decision_explanations.md" if filing_discovery else "canary_decision_explanations.md"
    summary_name = "universe_summary.md" if full_universe else "filing_summary.md" if filing_discovery else "canary_summary.md"
    pd.DataFrame(status_rows).to_csv(output_dir / status_name, index=False)

    manifest_artifacts = []
    for artifact in payload["artifacts"]:
        raw = artifact.get("_raw")
        raw_path = artifact.get("_raw_path")
        raw_name = None
        if isinstance(raw, bytes):
            raw_name = f"{artifact['artifact_id'].replace(':', '_')}.bin"
            (source_dir / raw_name).write_bytes(raw)
        elif raw_path:
            raw_name = f"{artifact['artifact_id'].replace(':', '_')}.bin"
            shutil.copyfile(raw_path, source_dir / raw_name)
        manifest_artifacts.append({k: v for k, v in artifact.items() if k not in {"_raw", "_raw_path"}} | {"frozen_raw_path": f"source/{raw_name}" if raw_name else None})
    manifest = {
        "run_id": payload["run_id"], "run_mode": payload["run_mode"], "as_of_date": str(payload["as_of_date"]),
        "input_snapshot_hash": payload["input_snapshot_hash"], "artifacts": manifest_artifacts,
    }
    (output_dir / "P0_source_manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n", encoding="utf-8")
    (output_dir / explanation_name).write_text(_explanations(payload), encoding="utf-8")
    (output_dir / summary_name).write_text(_summary(payload), encoding="utf-8")


def _parquet(path: Path, rows: list[dict], *, empty_columns: list[str] | None = None) -> None:
    frame = pd.DataFrame(rows)
    if frame.empty and empty_columns:
        frame = pd.DataFrame(columns=empty_columns)
    frame.to_parquet(path, index=False)


def _statement_rows(member: dict, fundamentals: dict, period_type: str) -> list[dict]:
    statements = fundamentals.get(f"{period_type}_statements", [])
    completeness = member.get(f"{period_type}_completeness")
    if not statements:
        return [{
            "company_id": member["company_id"], "security_id": member["security_id"],
            "period_type": period_type, "period_end": None,
            "statement_scope": member["statement_scope"], "completeness": completeness,
            "data_state": fundamentals.get("state"), "normalization_status": "NOT_NORMALIZED",
            "metrics_json": "{}", "raw_values_json": "{}", "source_document_url": None,
            "source_artifact_id": None, "source_row_hash": None, "published_at": None,
            "formula_version": None,
        }]
    return [{
        "company_id": member["company_id"], "security_id": member["security_id"],
        "period_type": period_type,
        "period_end": str(statement["period_end"]) if statement.get("period_end") is not None else None,
        "statement_scope": statement["scope"], "completeness": completeness,
        "data_state": fundamentals.get("state"),
        "normalization_status": statement.get(
            "normalization_status",
            "NORMALIZED_FROM_ISSUER_PDF"
            if statement.get("formula_version") == "issuer-pdf-curated-v1"
            else "NORMALIZED_FROM_EXCHANGE_XBRL",
        ),
        "metrics_json": json.dumps(statement.get("metrics", {}), sort_keys=True, default=str),
        "raw_values_json": json.dumps(statement.get("raw_values", {}), sort_keys=True, default=str),
        "source_document_url": statement.get("source_document_url"),
        "source_artifact_id": statement.get("source_artifact_id"),
        "source_row_hash": statement.get("source_row_hash"),
        "published_at": str(statement["published_at"]) if statement.get("published_at") is not None else None,
        "formula_version": statement.get("formula_version"),
    } for statement in statements]


def _explanations(payload: dict) -> str:
    label = "Universe" if payload["run_mode"] == "full_universe" else "Filing discovery" if payload["run_mode"] == "filing_discovery" else "Canary"
    lines = [f"# {label} decision explanations — {payload['run_id']}", ""]
    for member in payload["members"]:
        lines.extend([
            f"## {member['symbol']} — {member['company']}", "",
            f"- Identity: `{member['identity_status']}`; fixture ISIN `{member['isin']}`; observed {member['identity'].get('observed_isins', [])}.",
            f"- Eligibility: `{member['market_cap_status']}`; dated full market cap: `{member.get('market_cap_cr')}` as of `{member.get('market_cap_as_of')}`.",
            f"- Fundamentals: scope `{member['statement_scope']}`, annual completeness `{member.get('annual_completeness')}`, quarterly completeness `{member.get('quarterly_completeness')}`.",
            f"- Fundamental score: `{member.get('fundamental_score')}`; data-confidence score: `{member['data_confidence']}`.",
            f"- Corporate actions: `{member['corporate_action_status']}`; technical status (separate): `{member['technical_status']}`.",
            f"- Final disposition: `{member['disposition']}`.",
            f"- Reasons: {', '.join(r['code'] for r in member['reasons'])}.",
            f"- Source artifacts: {', '.join(sorted({a for r in member['reasons'] for a in r.get('source_artifact_ids', [])})) or 'none'}.",
            "",
        ])
    return "\n".join(lines)


def _summary(payload: dict) -> str:
    counts: dict[str, int] = {}
    for row in payload["members"]:
        counts[row["disposition"]] = counts.get(row["disposition"], 0) + 1
    full_universe = payload["run_mode"] == "full_universe"
    label = "Universe" if full_universe else "Filing discovery" if payload["run_mode"] == "filing_discovery" else "Canary"
    total = payload.get("universe_size", len(payload["members"]))
    lines = [
        f"# {label} summary — {payload['run_id']}", "",
        f"- Mode: `{payload['run_mode']}`", f"- AS_OF_DATE: `{payload['as_of_date']}`",
        f"- Companies/securities accounted for: `{len(payload['members'])}/{total}`", f"- Eligible with trusted dated cap: `{payload['eligible_count']}`",
        f"- Input snapshot hash: `{payload['input_snapshot_hash']}`", f"- Dispositions: `{canonical_json(counts)}`",
    ]
    if full_universe:
        lines.extend([
            f"- Security-master identity coverage: `{payload.get('identity_coverage', 0.0):.4%}`",
            f"- Resolved/unresolved company-equity identities: `{payload.get('identity_resolved_count', 0)}/{payload.get('identity_unresolved_count', 0)}`",
        ])
    if payload["run_mode"] == "filing_discovery":
        lines.extend([
            f"- Frozen parent run: `{payload.get('parent_run_id')}`",
            f"- Filing-grade pass count: `{payload.get('filing_pass_count', 0)}`",
            f"- Checkpoint hits: `{payload.get('checkpoint_hits', 0)}`",
        ])
    lines.extend(["", "This pack is authoritative only when the corresponding `screening_run.status` is `COMPLETED`.", ""])
    return "\n".join(lines)
