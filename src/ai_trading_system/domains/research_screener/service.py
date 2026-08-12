from __future__ import annotations

import csv
import copy
import hashlib
import json
import re
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import duckdb

from ai_trading_system.platform.db.paths import get_domain_paths

from .models import Disposition, RunMode, ScreeningParameters
from .issuer_filings import IssuerFilingRepairClient
from .issuer_classification import ExistingIssuerClassificationProvider
from .filing_replay import rebuild_fundamentals
from .providers import ExistingRepositoryProvider, OfficialExchangeClient, _artifact
from .reporting import write_output_pack
from .store import ResearchScreenerStore, canonical_json, content_hash

BANKS = {"BANKBARODA", "HDFCBANK"}
MARKET_INFRASTRUCTURE = {"MCX", "CAMS"}
RULES = {
    "version": "minimal-canary-v1",
    "market_cap": {"field": "full_market_cap_cr", "inclusive": True},
    "fundamental_min_completeness": 0.70,
    "technical_in_fundamental_admission": False,
    "archetype_quota": None,
    "missing_is_not_zero": True,
}
FULL_UNIVERSE_RULES = {
    "version": "controlled-full-universe-phase1-v1",
    "universe": "official-nse-bse-active-equity-isins",
    "nse_main_board_series": ["EQ", "BE", "BZ"],
    "bse_sme_groups": ["M", "MS", "MT", "TS"],
    "market_cap": {"field": "full_market_cap_cr", "inclusive": True},
    "fundamental_admission": "filing_grade_required_after_identity_and_cap_gate",
    "technical_in_fundamental_admission": False,
    "missing_is_not_zero": True,
    "provider_fallback": False,
}
FILING_DISCOVERY_RULES = {
    "version": "controlled-filing-discovery-v2",
    "cohort": "frozen-cap-eligible-members-from-completed-full-universe-run",
    "provider_order": ["NSE_XBRL", "BSE_XBRL_EXACT_DUAL_LISTING_FALLBACK"],
    "snapshot_selection": "whole-provider-snapshot-no-period-splicing",
    "annual_periods": 6,
    "quarterly_periods": 12,
    "minimum_completeness": 0.70,
    "issuer_classification": "exact-isin-current-master-snapshot-fail-closed",
    "point_in_time": True,
    "corporate_action_continuity": "required",
    "technical_in_fundamental_admission": False,
    "missing_is_not_zero": True,
    "ranking": False,
}


class PersistentScreenerService:
    def __init__(self, *, project_root: str | Path | None = None, store_path: str | Path | None = None,
                 output_root: str | Path | None = None, exchange_client: OfficialExchangeClient | None = None,
                 issuer_classifier=None):
        self.project_root = Path(project_root or Path.cwd()).resolve()
        paths = get_domain_paths(project_root=self.project_root, data_domain="operational")
        self.store_path = Path(store_path) if store_path else paths.root_dir / "research_screener" / "control_plane.duckdb"
        self.output_root = Path(output_root) if output_root else paths.root_dir / "research_screener" / "runs"
        self.exchange = exchange_client or OfficialExchangeClient()
        self.issuer_classifier = issuer_classifier or ExistingIssuerClassificationProvider(
            paths.master_db_path,
        )

    def run(self, params: ScreeningParameters) -> dict:
        registry_path = self.project_root / "configs/research_screener/source_registry.yaml"
        registry_raw = registry_path.read_bytes()
        registry = json.loads(registry_raw)
        kpi_path = self.project_root / "configs/research_screener/kpi_contracts.json"
        kpi_raw = kpi_path.read_bytes()
        kpi_contracts = json.loads(kpi_raw)
        issuer_repair_path = self.project_root / "configs/research_screener/issuer_filing_repairs.json"
        issuer_repair_raw = issuer_repair_path.read_bytes()
        issuer_repairs = json.loads(issuer_repair_raw)
        started_at = datetime.now(UTC)

        if params.run_mode == RunMode.FULL_UNIVERSE:
            payload = self._full_universe_payload(
                params, registry, registry_raw, started_at,
            )
        elif params.run_mode == RunMode.FILING_DISCOVERY:
            payload = self._filing_discovery_payload(
                params, registry, registry_raw, issuer_repairs, issuer_repair_raw, started_at,
            )
        else:
            fixture_versions_path = self.project_root / "configs/research_screener/canary_fixture_versions.json"
            fixture_versions_raw = fixture_versions_path.read_bytes()
            fixture_versions = json.loads(fixture_versions_raw)
            fixture_path = self._fixture_path(params, fixture_versions)
            fixture_raw = fixture_path.read_bytes()
            fixture = list(csv.DictReader(fixture_raw.decode("utf-8").splitlines()))
            fixture_version = self._validate_fixture(fixture, params.run_mode, fixture_versions)
            expected_fixture_hash = fixture_versions["versions"][fixture_version].get("sha256")
            actual_fixture_hash = hashlib.sha256(fixture_raw).hexdigest()
            if actual_fixture_hash != expected_fixture_hash:
                raise ValueError(
                    f"canary fixture {fixture_version} checksum mismatch: expected {expected_fixture_hash}, got {actual_fixture_hash}"
                )
        if params.run_mode == RunMode.REGRESSION_REPLAY:
            regression_path = self.project_root / "tests/fixtures/research_screener/regression_2026-08-08.json"
            regression_raw = regression_path.read_bytes()
            regression = json.loads(regression_raw)
            payload = self._regression_payload(
                params, fixture, fixture_raw, fixture_path, fixture_version, fixture_versions_raw,
                registry, registry_raw, kpi_contracts, kpi_raw, regression, regression_raw, started_at,
            )
        elif params.run_mode == RunMode.LIVE_CANARY:
            payload = self._live_payload(
                params, fixture, fixture_raw, fixture_path, fixture_version, fixture_versions, fixture_versions_raw,
                registry, registry_raw, kpi_contracts, kpi_raw, issuer_repairs, issuer_repair_raw, started_at,
            )

        store = ResearchScreenerStore(self.store_path)
        base_run_id = payload["run_id"]
        allocated_run_id, failed_predecessor = store.allocate_run_id(base_run_id)
        payload["run_id"] = allocated_run_id
        payload["supersedes_failed_run_id"] = failed_predecessor
        output_dir = self.output_root / payload["run_id"]
        if store.completed_run(allocated_run_id):
            if params.run_mode == RunMode.LIVE_CANARY:
                store.compare_latest_regression(allocated_run_id)
            return {"run_id": allocated_run_id, "status": "COMPLETED", "output_dir": str(self.output_root / allocated_run_id), "idempotent_replay": True}
        try:
            write_output_pack(output_dir, payload)
            store.persist_success(payload)
            if params.run_mode == RunMode.LIVE_CANARY:
                store.compare_latest_regression(payload["run_id"])
        except Exception as exc:
            store.persist_failure(payload, error_code=type(exc).__name__, error_message=str(exc))
            raise
        return {"run_id": payload["run_id"], "status": "COMPLETED", "output_dir": str(output_dir), "members": payload["members"]}

    def _filing_discovery_payload(self, params, registry, registry_raw, issuer_repairs,
                                  issuer_repair_raw, started_at):
        store = ResearchScreenerStore(self.store_path)
        parent_run_id = params.parent_run_id or store.latest_completed_run(
            RunMode.FULL_UNIVERSE.value, params.as_of_date,
        )
        if not parent_run_id:
            raise ValueError(
                f"no completed full_universe parent exists for {params.as_of_date}; pass --parent-run-id"
            )
        parent_dir = self.output_root / parent_run_id
        required = {
            "security": parent_dir / "P0_security_master.parquet",
            "market": parent_dir / "P0_market_snapshot.parquet",
            "status": parent_dir / "universe_company_status.csv",
            "manifest": parent_dir / "P0_source_manifest.json",
        }
        missing = [str(path) for path in required.values() if not path.is_file()]
        if missing:
            raise FileNotFoundError(f"parent full-universe pack is incomplete: {missing}")

        artifacts = [
            _artifact(
                f"parent_full_universe_{name}", "RESEARCH_SCREENER", path.read_bytes(),
                url=f"runs/{parent_run_id}/{path.name}", effective_date=params.as_of_date,
                row_count=None, metadata={"parent_run_id": parent_run_id, "immutable_parent": True},
            )
            for name, path in required.items()
        ]
        artifacts.extend([
            _artifact(
                "source_registry", "REPOSITORY", registry_raw,
                url="configs/research_screener/source_registry.yaml",
                effective_date=params.as_of_date, row_count=len(registry["sources"]),
            ),
            _artifact(
                "issuer_filing_repairs", "REPOSITORY", issuer_repair_raw,
                url="configs/research_screener/issuer_filing_repairs.json",
                effective_date=params.as_of_date, row_count=len(issuer_repairs["symbols"]),
                metadata={"contract_version": issuer_repairs["contract_version"]},
            ),
        ])
        parent_artifact_id = artifacts[0]["artifact_id"]
        security = pd.read_parquet(required["security"])
        market = pd.read_parquet(required["market"])
        status = pd.read_csv(required["status"])
        eligible_status = status[status["market_cap_status"] == "ELIGIBLE"].copy()
        if eligible_status.empty:
            raise ValueError(f"parent run {parent_run_id} has no cap-eligible filing cohort")
        security_by_isin = {
            str(row.fixture_isin): row for row in security.itertuples(index=False)
        }
        market_by_security = {
            str(row.security_id): row for row in market.itertuples(index=False)
        }
        cohort: list[dict] = []
        for status_row in eligible_status.itertuples(index=False):
            isin = str(status_row.fixture_isin)
            master = security_by_isin.get(isin)
            if master is None:
                raise ValueError(f"eligible parent member {isin} is missing from P0_security_master")
            listings = json.loads(master.listings) if isinstance(master.listings, str) else []
            cohort.append({
                "symbol": str(status_row.symbol), "company": str(status_row.company), "isin": isin,
                "company_id": str(master.company_id), "security_id": str(master.security_id),
                "listings": listings, "face_value": None,
                "market": market_by_security.get(str(master.security_id)),
            })
        cohort.sort(key=lambda row: (row["isin"], row["symbol"]))

        classification_by_isin, classification_raw = self.issuer_classifier.snapshot(
            cohort, params.as_of_date,
        )
        classification_artifact = _artifact(
            "existing_issuer_classification", "EXISTING_TRADING_SYSTEM", classification_raw,
            url="masterdata.db:symbols:exact_isin_classification_snapshot",
            effective_date=params.as_of_date, row_count=len(classification_by_isin),
            metadata={"read_only": True, "schema_version": "issuer-classification-snapshot-v1"},
        )
        artifacts.append(classification_artifact)
        for row in cohort:
            classification = dict(classification_by_isin[row["isin"].upper()])
            classification["source_artifact_id"] = classification_artifact["artifact_id"]
            row["issuer_classification"] = classification

        parent_digest = content_hash({
            **{name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in required.items()},
            "issuer_classification": hashlib.sha256(classification_raw).hexdigest(),
            "issuer_filing_repairs": hashlib.sha256(issuer_repair_raw).hexdigest(),
        })
        predecessor_parent_digest = content_hash({
            name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in required.items()
        })
        checkpoint_dir = (
            self.output_root.parent / "checkpoints" / "filing_discovery"
            / parent_run_id / str(params.screen_version)
        )
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        predecessor_checkpoint_dir = (
            self.output_root.parent / "checkpoints" / "filing_discovery"
            / parent_run_id / "1.1.0"
        )
        local = ExistingRepositoryProvider(
            {row["isin"]: {"symbol": row["symbol"]} for row in cohort},
            project_root=self.project_root,
        )
        thread_state = threading.local()
        member_results: dict[int, dict] = {}
        artifact_results: dict[int, list[dict]] = {}
        checkpoint_hits = 0
        evidence_replay_hits = 0

        def acquire(index: int, row: dict) -> tuple[int, dict, list[dict], bool]:
            cached = self._load_filing_checkpoint(
                checkpoint_dir, row["isin"], parent_digest=parent_digest,
                as_of_date=params.as_of_date,
            )
            if cached is not None:
                member, member_artifacts = cached
                return index, member, member_artifacts, True
            if str(params.screen_version) == "1.2.0":
                predecessor = self._load_filing_checkpoint(
                    predecessor_checkpoint_dir, row["isin"],
                    parent_digest=predecessor_parent_digest, as_of_date=params.as_of_date,
                )
                if predecessor is not None:
                    member, member_artifacts = predecessor
                    replayed = self._replay_filing_member(
                        member, member_artifacts,
                        issuer_classification=row["issuer_classification"],
                        as_of_date=params.as_of_date,
                    )
                    return index, replayed, member_artifacts, "evidence_replay"
            if not hasattr(thread_state, "exchange"):
                if params.workers == 1:
                    thread_state.exchange = self.exchange
                else:
                    thread_state.exchange = OfficialExchangeClient(
                        timeout=self.exchange.timeout,
                        # Four sessions preserve the serial aggregate cadence.
                        # Explicitly using 5-64 sessions permits at most a 2x
                        # aggregate rate while every session remains paced.
                        min_interval=self.exchange.min_interval * max(4, params.workers / 2),
                    )
                thread_state.issuer_repair = IssuerFilingRepairClient(
                    thread_state.exchange, issuer_repairs,
                )
            member, member_artifacts = self._discover_member_filings(
                row, params=params, parent_artifact_id=parent_artifact_id,
                local=local, issuer_repair_client=thread_state.issuer_repair,
                exchange=thread_state.exchange,
            )
            self._save_filing_checkpoint(
                checkpoint_dir, row["isin"], member, member_artifacts,
                parent_digest=parent_digest, as_of_date=params.as_of_date,
            )
            loaded = self._load_filing_checkpoint(
                checkpoint_dir, row["isin"], parent_digest=parent_digest,
                as_of_date=params.as_of_date,
            )
            if loaded is None:
                raise RuntimeError(f"fresh filing checkpoint failed verification for {row['isin']}")
            return index, loaded[0], loaded[1], False

        completed = 0
        with ThreadPoolExecutor(max_workers=params.workers, thread_name_prefix="filing-discovery") as pool:
            futures = [pool.submit(acquire, index, row) for index, row in enumerate(cohort, start=1)]
            for future in as_completed(futures):
                index, member, member_artifacts, cache_state = future.result()
                member_results[index] = member
                artifact_results[index] = member_artifacts
                checkpoint_hits += int(cache_state is True)
                evidence_replay_hits += int(cache_state == "evidence_replay")
                completed += 1
                if completed % params.batch_size == 0 or completed == len(cohort):
                    progress = {
                        "parent_run_id": parent_run_id, "parent_digest": parent_digest,
                        "as_of_date": str(params.as_of_date), "screen_version": params.screen_version,
                        "cohort_size": len(cohort), "members_processed": completed,
                        "checkpoint_hits": checkpoint_hits,
                        "evidence_replay_hits": evidence_replay_hits,
                        "filing_grade_passed": sum(
                            item["disposition"] == Disposition.BOUNDARY_REVIEW.value
                            for item in member_results.values()
                        ),
                        "workers": params.workers,
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                    temporary = checkpoint_dir / "progress.json.tmp"
                    temporary.write_text(json.dumps(progress, indent=2) + "\n", encoding="utf-8")
                    temporary.replace(checkpoint_dir / "progress.json")

        members = [member_results[index] for index in sorted(member_results)]
        for index in sorted(artifact_results):
            artifacts.extend(artifact_results[index])

        dq_issues: list[dict] = []
        repairs: list[dict] = []
        for member in members:
            for reason in member["reasons"]:
                if reason["code"] == "FILING_GRADE_DISCOVERY_PASSED":
                    continue
                issue = {
                    "issue_id": f"dq:{member['isin']}:{reason['code']}",
                    "company_id": member["company_id"], "security_id": member["security_id"],
                    "domain": "corporate_action" if "CORPORATE_ACTION" in reason["code"] else "fundamental",
                    "code": reason["code"], "severity": "BLOCKING", "state": "OPEN",
                    "message": reason["message"],
                    "source_artifact_id": (reason.get("source_artifact_ids") or [None])[0],
                }
                dq_issues.append(issue)
                repairs.append({
                    "repair_id": f"repair:{member['isin']}:{reason['code']}",
                    "company_id": member["company_id"], "domain": issue["domain"],
                    "reason_code": reason["code"], "required_action": reason["message"],
                })
        payload = self._finish_payload(
            params, registry, artifacts, members, started_at,
            eligible_count=len(members), dq_issues=dq_issues, repairs=repairs,
            price_cutoff=params.as_of_date,
        )
        payload["parent_run_id"] = parent_run_id
        payload["universe_size"] = len(members)
        payload["filing_pass_count"] = sum(
            member["disposition"] == Disposition.BOUNDARY_REVIEW.value for member in members
        )
        payload["checkpoint_hits"] = checkpoint_hits
        payload["evidence_replay_hits"] = evidence_replay_hits
        return payload

    @staticmethod
    def _replay_filing_member(member: dict, artifacts: list[dict], *,
                              issuer_classification: dict, as_of_date: date) -> dict:
        replayed = copy.deepcopy(member)
        company_type = issuer_classification["company_type"]
        fundamentals = rebuild_fundamentals(
            replayed["inputs"]["fundamentals"], artifacts,
            company_type=company_type, as_of_date=as_of_date,
        )
        classification = dict(issuer_classification)
        reasons = []
        if classification["state"] != "PRESENT":
            reasons.append({
                "code": "ISSUER_CLASSIFICATION_UNRESOLVED",
                "message": (
                    "Exact-ISIN issuer classification is unresolved; no bank, financial-institution, "
                    "market-infrastructure, or industrial metric contract was applied: "
                    f"{classification['reason']}."
                ),
                "source_artifact_ids": [classification["source_artifact_id"]],
            })
        if fundamentals["state"] != "PRESENT":
            reasons.append({
                "code": "FUNDAMENTAL_PROVENANCE_OR_COMPLETENESS_FAILED",
                "message": fundamentals["provenance_validation"]["reason"],
                "source_artifact_ids": fundamentals["source_artifact_ids"],
            })
        action_status = replayed["corporate_action_status"]
        if action_status != "VALIDATED":
            validation = replayed["inputs"]["corporate_action_validation"]
            action_ids = [
                artifact["artifact_id"] for artifact in artifacts
                if "corporate_actions" in str(artifact.get("source_key") or "")
            ]
            reasons.append({
                "code": "CORPORATE_ACTION_CONTINUITY_FAILED",
                "message": validation["reason"], "source_artifact_ids": action_ids,
            })
        if not reasons:
            reasons.append({
                "code": "FILING_GRADE_DISCOVERY_PASSED",
                "message": "Filing provenance, scope, required period coverage, point-in-time cutoff, and corporate-action continuity passed; no ranking or recommendation was performed.",
                "source_artifact_ids": fundamentals["source_artifact_ids"],
            })
        replayed.update({
            "company_type": company_type, "statement_scope": fundamentals["scope"],
            "annual_completeness": fundamentals["annual_completeness"],
            "quarterly_completeness": fundamentals["quarterly_completeness"],
            "data_confidence": round(
                0.5 + (0.3 if fundamentals["state"] == "PRESENT" else 0.0)
                + (0.2 if action_status == "VALIDATED" else 0.0), 2,
            ),
            "disposition": (
                Disposition.BOUNDARY_REVIEW.value
                if reasons[0]["code"] == "FILING_GRADE_DISCOVERY_PASSED"
                else Disposition.DATA_REPAIR_REQUIRED.value
            ),
            "reasons": reasons,
        })
        replayed["inputs"]["fundamentals"] = fundamentals
        replayed["inputs"]["issuer_classification"] = classification
        replayed["inputs"]["phase1_discovery_state"] = (
            "FILING_GRADE_RENORMALIZED_FROM_CHECKSUM_VERIFIED_PREDECESSOR"
        )
        return replayed

    def _discover_member_filings(self, row, *, params, parent_artifact_id, local,
                                 issuer_repair_client, exchange=None):
        exchange = exchange or self.exchange
        symbol, isin = row["symbol"], row["isin"]
        listings = row["listings"]
        nse = next((listing for listing in listings if listing.get("exchange") == "NSE"), None)
        bse = next((listing for listing in listings if listing.get("exchange") == "BSE"), None)
        filing_symbol = str(nse.get("symbol") or symbol) if nse else symbol
        bse_code = str(bse.get("bse_code") or "") if bse else ""
        issuer_classification = row["issuer_classification"]
        company_type = issuer_classification["company_type"]
        try:
            stored_actions = local.get_corporate_actions(isin, params.as_of_date)
        except (OSError, RuntimeError, KeyError, duckdb.Error):
            stored_actions = []
        stored_raw = canonical_json(stored_actions).encode("utf-8")
        stored_artifact = _artifact(
            "existing_corporate_actions", "EXISTING_TRADING_SYSTEM", stored_raw,
            url=f"ohlcv.duckdb:_corporate_actions:{filing_symbol}",
            effective_date=params.as_of_date, row_count=len(stored_actions),
            metadata={"read_only": True, "isin": isin},
        )
        identifier_history = self._filing_identifier_history(isin, stored_actions, [])
        if nse:
            primary, filing_artifacts = exchange.nse_fundamental_snapshot(
                filing_symbol, isin, params.as_of_date, company_type=company_type,
                identifier_history=identifier_history,
            )
            fundamentals = primary
            if primary.get("state") != "PRESENT" and bse_code:
                fallback, fallback_artifacts = exchange.bse_fundamental_snapshot(
                    bse_code, isin, params.as_of_date, company_type=company_type,
                    identifier_history=identifier_history,
                )
                filing_artifacts.extend(fallback_artifacts)
                fundamentals = self._select_fundamental_provider(primary, fallback)
        elif bse_code:
            fundamentals, filing_artifacts = exchange.bse_fundamental_snapshot(
                bse_code, isin, params.as_of_date, company_type=company_type,
                identifier_history=identifier_history,
            )
        else:
            fundamentals = exchange._empty_fundamentals("eligible parent member has no exact exchange listing")
            filing_artifacts = []
        fundamentals, issuer_artifacts = issuer_repair_client.augment(
            symbol, isin, fundamentals, params.as_of_date, company_type=company_type,
        )
        filing_artifacts.extend(issuer_artifacts)
        action_window_start = params.as_of_date - timedelta(days=730)
        if nse:
            official_actions, action_artifact = exchange.nse_corporate_actions(
                filing_symbol, isin, action_window_start, params.as_of_date,
            )
        elif bse_code:
            official_actions, action_artifact = exchange.bse_corporate_actions(
                bse_code, symbol, isin, action_window_start, params.as_of_date,
            )
        else:
            official_actions = []
            action_artifact = _artifact(
                "corporate_actions", "RESEARCH_SCREENER", b"exact listing unavailable",
                url=f"derived://missing-listing/{isin}", effective_date=params.as_of_date,
                row_count=0, status="FAILED", metadata={"error": "exact listing unavailable"},
            )
        ca_status, action_validation = self._validate_action_history(
            official_actions, stored_actions, action_artifact,
        )
        fundamentals["source_artifact_ids"] = [artifact["artifact_id"] for artifact in filing_artifacts]
        reasons: list[dict] = []
        if issuer_classification["state"] != "PRESENT":
            reasons.append({
                "code": "ISSUER_CLASSIFICATION_UNRESOLVED",
                "message": (
                    "Exact-ISIN issuer classification is unresolved; no bank, financial-institution, "
                    "market-infrastructure, or industrial metric contract was applied: "
                    f"{issuer_classification['reason']}."
                ),
                "source_artifact_ids": [issuer_classification["source_artifact_id"]],
            })
        if fundamentals.get("state") != "PRESENT":
            reasons.append({
                "code": "FUNDAMENTAL_PROVENANCE_OR_COMPLETENESS_FAILED",
                "message": fundamentals.get("provenance_validation", {}).get(
                    "reason", "Filing-grade annual/quarterly completeness is below the discovery contract."
                ),
                "source_artifact_ids": fundamentals["source_artifact_ids"],
            })
        if ca_status != "VALIDATED":
            reasons.append({
                "code": "CORPORATE_ACTION_CONTINUITY_FAILED",
                "message": action_validation["reason"],
                "source_artifact_ids": [action_artifact["artifact_id"], stored_artifact["artifact_id"]],
            })
        disposition = Disposition.DATA_REPAIR_REQUIRED.value
        if not reasons:
            disposition = Disposition.BOUNDARY_REVIEW.value
            reasons.append({
                "code": "FILING_GRADE_DISCOVERY_PASSED",
                "message": "Filing provenance, scope, required period coverage, point-in-time cutoff, and corporate-action continuity passed; no ranking or recommendation was performed.",
                "source_artifact_ids": fundamentals["source_artifact_ids"] + [action_artifact["artifact_id"]],
            })
        market_row = row.get("market")
        market_cap_cr = getattr(market_row, "full_market_cap_cr", None) if market_row is not None else None
        market_as_of = getattr(market_row, "as_of_date", params.as_of_date) if market_row is not None else params.as_of_date
        identity = {
            "source_artifact_id": parent_artifact_id, "source_artifact_ids": [parent_artifact_id],
            "observed_isins": [isin], "listings": listings, "face_value": row.get("face_value"),
        }
        confidence = round(
            0.3 + 0.2 + (0.3 if fundamentals.get("state") == "PRESENT" else 0.0)
            + (0.2 if ca_status == "VALIDATED" else 0.0), 2,
        )
        member = {
            "symbol": symbol, "company": row["company"], "isin": isin,
            "member_key": isin, "company_id": row["company_id"], "security_id": row["security_id"],
            "company_type": company_type, "as_of_date": params.as_of_date,
            "identity": identity, "identity_status": "RESOLVED",
            "market_cap_status": "ELIGIBLE", "market_cap_cr": market_cap_cr,
            "market_cap_as_of": market_as_of, "statement_scope": fundamentals["scope"],
            "annual_completeness": fundamentals["annual_completeness"],
            "quarterly_completeness": fundamentals["quarterly_completeness"],
            "corporate_action_status": ca_status, "data_confidence": confidence,
            "technical_status": "UNAVAILABLE", "fundamental_score": None,
            "disposition": disposition, "reasons": reasons,
            "inputs": {
                "market": {"state": "PRESENT", "full_market_cap_cr": market_cap_cr,
                           "as_of_date": market_as_of, "parent_run_id": params.parent_run_id},
                "fundamentals": fundamentals, "ohlcv": [],
                "issuer_classification": issuer_classification,
                "corporate_actions": stored_actions, "official_corporate_actions": official_actions,
                "corporate_action_validation": action_validation,
                "phase1_discovery_state": "FILING_GRADE_DISCOVERY_COMPLETE",
            },
        }
        return member, list(filing_artifacts) + [action_artifact, stored_artifact]

    @staticmethod
    def _broad_company_type(symbol: str, company: str) -> str:
        if symbol in MARKET_INFRASTRUCTURE:
            return "MARKET_INFRASTRUCTURE"
        if symbol in BANKS or re.search(r"\bBANK\b", company.upper()):
            return "BANK"
        return "CORPORATE"

    @staticmethod
    def _filing_checkpoint_stem(isin: str) -> str:
        return f"{isin}_{content_hash(isin)[:12]}"

    def _save_filing_checkpoint(self, root: Path, isin: str, member: dict,
                                artifacts: list[dict], *, parent_digest: str,
                                as_of_date: date) -> None:
        stem = self._filing_checkpoint_stem(isin)
        target = root / stem
        target.mkdir(parents=True, exist_ok=True)
        saved_artifacts = []
        for index, artifact in enumerate(artifacts):
            raw = artifact.get("_raw")
            if not isinstance(raw, bytes):
                raise ValueError(f"checkpoint artifact has no raw bytes: {artifact['artifact_id']}")
            raw_name = f"artifact_{index:03d}_{artifact['content_hash'][:16]}.bin"
            temporary = target / f"{raw_name}.tmp"
            temporary.write_bytes(raw)
            temporary.replace(target / raw_name)
            saved_artifacts.append(
                {key: value for key, value in artifact.items() if key != "_raw"} | {"checkpoint_raw": raw_name}
            )
        payload = {
            "isin": isin, "as_of_date": str(as_of_date), "parent_digest": parent_digest,
            "member": member, "artifacts": saved_artifacts,
        }
        temporary = target / "result.json.tmp"
        temporary.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
        temporary.replace(target / "result.json")

    def _load_filing_checkpoint(self, root: Path, isin: str, *, parent_digest: str,
                                as_of_date: date) -> tuple[dict, list[dict]] | None:
        target = root / self._filing_checkpoint_stem(isin)
        result_path = target / "result.json"
        if not result_path.is_file():
            return None
        try:
            saved = json.loads(result_path.read_text(encoding="utf-8"))
            if saved.get("isin") != isin or saved.get("as_of_date") != str(as_of_date):
                return None
            if saved.get("parent_digest") != parent_digest:
                return None
            artifacts = []
            for artifact in saved["artifacts"]:
                raw_path = target / artifact.pop("checkpoint_raw")
                with raw_path.open("rb") as raw_file:
                    if hashlib.file_digest(raw_file, "sha256").hexdigest() != artifact["content_hash"]:
                        return None
                artifact["_raw_path"] = str(raw_path)
                artifacts.append(artifact)
            return saved["member"], artifacts
        except (KeyError, OSError, ValueError, TypeError, json.JSONDecodeError):
            return None

    def _full_universe_payload(self, params, registry, registry_raw, started_at):
        identity = self.exchange.acquire_identity(params.as_of_date)
        artifacts = list(identity["artifacts"])
        artifacts.append(_artifact(
            "source_registry", "REPOSITORY", registry_raw,
            url="configs/research_screener/source_registry.yaml",
            effective_date=params.as_of_date, row_count=len(registry["sources"]),
        ))
        combined_artifact_id = identity["artifacts"][0]["artifact_id"]
        nse_artifact_id = identity["artifacts"][1]["artifact_id"]
        bse_artifact_id = identity["artifacts"][2]["artifact_id"]
        normalized = self._normalize_full_universe(
            identity, combined_artifact_id=combined_artifact_id,
            nse_artifact_id=nse_artifact_id, bse_artifact_id=bse_artifact_id,
        )
        normalized_raw = canonical_json(normalized).encode("utf-8")
        normalized_artifact = _artifact(
            "full_universe_normalized", "RESEARCH_SCREENER", normalized_raw,
            url="derived://official-nse-bse-active-equity-isins",
            effective_date=identity["effective_date"], row_count=len(normalized),
            metadata={"dedupe_key": "ISIN", "non_equity_and_unresolved_retained": True},
        )
        artifacts.append(normalized_artifact)

        members: list[dict] = []
        dq_issues: list[dict] = []
        repairs: list[dict] = []
        checkpoint_dir = (
            self.output_root.parent / "checkpoints" / "full_universe"
            / str(params.as_of_date) / str(params.screen_version)
        )
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        main_board_count = sum(
            row["identity_status"] == "RESOLVED"
            and any(item.get("board") == "MAIN" for item in row["listings"])
            for row in normalized
        )
        market_cap_attempts = 0
        checkpoint_hits = 0
        for index, row in enumerate(normalized, start=1):
            symbol = row["symbol"]
            isin = row["isin"]
            identity_status = row["identity_status"]
            main_board = any(item.get("board") == "MAIN" for item in row["listings"])
            market = {"state": "NOT_EVALUATED"}
            cap = None
            cap_artifact = None
            reasons: list[dict] = []

            if identity_status != "RESOLVED":
                market_status = "NOT_EVALUATED"
                disposition = Disposition.INELIGIBLE_BOARD_OR_INSTRUMENT.value
                code = "NON_EQUITY_INSTRUMENT" if identity_status == "NON_EQUITY_INSTRUMENT" else "SECURITY_IDENTITY_UNRESOLVED"
                reasons.append({
                    "code": code,
                    "message": row["identity_reason"],
                    "source_artifact_ids": row["source_artifact_ids"],
                })
                if identity_status == "UNRESOLVED":
                    disposition = Disposition.DATA_REPAIR_REQUIRED.value
            elif not main_board:
                market_status = "NOT_EVALUATED"
                disposition = Disposition.INELIGIBLE_BOARD_OR_INSTRUMENT.value
                reasons.append({
                    "code": "INELIGIBLE_SME_BOARD",
                    "message": "All active listings are classified as SME by official exchange series/group fields.",
                    "source_artifact_ids": row["source_artifact_ids"],
                })
            else:
                nse_listing = next((item for item in row["listings"] if item["exchange"] == "NSE"), None)
                bse_listing = next((item for item in row["listings"] if item["exchange"] == "BSE"), None)
                if nse_listing is not None:
                    market_cap_attempts += 1
                    cached = self._load_market_cap_checkpoint(
                        checkpoint_dir, symbol=nse_listing["symbol"], isin=isin,
                        as_of_date=params.as_of_date,
                    )
                    if cached is not None:
                        cap, cap_artifact = cached
                        checkpoint_hits += 1
                    else:
                        cap, cap_artifact = self.exchange.nse_market_cap(
                            nse_listing["symbol"], params.as_of_date, expected_isin=isin,
                        )
                        if cap is not None and cap_artifact["validation_status"] == "VALID":
                            self._save_market_cap_checkpoint(
                                checkpoint_dir, symbol=nse_listing["symbol"], isin=isin,
                                as_of_date=params.as_of_date, cap=cap, artifact=cap_artifact,
                            )
                    artifacts.append(cap_artifact)
                elif bse_listing is not None:
                    bse_row = row["bse_market_row"]
                    cap = self.exchange.bse_market_cap(bse_row, artifact_id=bse_artifact_id)
                if cap is None:
                    market_status = "ELIGIBILITY_UNKNOWN"
                    disposition = Disposition.ELIGIBILITY_UNKNOWN.value
                    reasons.append({
                        "code": "OFFICIAL_MARKET_CAP_UNAVAILABLE",
                        "message": "The fixed official market-cap source did not return a usable dated full market cap; no estimate or provider switch was made.",
                        "source_artifact_ids": [cap_artifact["artifact_id"]] if cap_artifact else [bse_artifact_id],
                    })
                else:
                    market = dict(cap) | {"state": "PRESENT"}
                    cap_value = cap["full_market_cap_cr"]
                    if params.min_market_cap_cr <= cap_value <= params.max_market_cap_cr:
                        market_status = "ELIGIBLE"
                        disposition = Disposition.DATA_REPAIR_REQUIRED.value
                        reasons.append({
                            "code": "PHASE1_FILING_DISCOVERY_REQUIRED",
                            "message": "Identity and dated market-cap gates passed; filing-grade scope, annual, quarterly, and corporate-action discovery must complete before fundamental admission.",
                            "source_artifact_ids": row["source_artifact_ids"] + ([cap_artifact["artifact_id"]] if cap_artifact else []),
                        })
                    else:
                        market_status = "INELIGIBLE_MARKET_CAP"
                        disposition = Disposition.INELIGIBLE_MARKET_CAP.value
                        reasons.append({
                            "code": "DATED_FULL_MARKET_CAP_OUTSIDE_BAND",
                            "message": f"Official full market cap {cap_value} crore is outside [{params.min_market_cap_cr}, {params.max_market_cap_cr}].",
                            "source_artifact_ids": row["source_artifact_ids"] + ([cap_artifact["artifact_id"]] if cap_artifact else []),
                        })

            company_id = f"company:{isin}" if identity_status == "RESOLVED" else f"company:unresolved:{row['member_key']}"
            security_id = f"security:{isin}" if identity_status == "RESOLVED" else f"security:unresolved:{row['member_key']}"
            member = {
                "symbol": symbol, "company": row["company"], "isin": isin,
                "member_key": row["member_key"], "company_id": company_id,
                "security_id": security_id, "company_type": "UNCLASSIFIED",
                "as_of_date": params.as_of_date,
                "identity": {
                    "source_artifact_id": row["source_artifact_ids"][0],
                    "source_artifact_ids": row["source_artifact_ids"],
                    "observed_isins": [isin] if isin else [], "listings": row["listings"],
                    "face_value": row.get("face_value"),
                },
                "identity_status": identity_status,
                "market_cap_status": market_status,
                "market_cap_cr": cap.get("full_market_cap_cr") if cap else None,
                "market_cap_as_of": cap.get("as_of_date", identity["effective_date"]) if cap else None,
                "statement_scope": "SCOPE_UNRESOLVED",
                "annual_completeness": 0.0, "quarterly_completeness": 0.0,
                "corporate_action_status": "NOT_EVALUATED_PHASE1_DISCOVERY",
                "data_confidence": 0.5 if market_status == "ELIGIBLE" else 0.3 if identity_status == "RESOLVED" else 0.0,
                "technical_status": "UNAVAILABLE", "fundamental_score": None,
                "disposition": disposition, "reasons": reasons,
                "inputs": {
                    "market": market,
                    "fundamentals": {"scope": "SCOPE_UNRESOLVED", "state": "DATA_REPAIR_REQUIRED"},
                    "ohlcv": [], "corporate_actions": [], "official_corporate_actions": [],
                    "phase1_discovery_state": "IDENTITY_AND_MARKET_CAP_ONLY",
                },
            }
            members.append(member)
            if index % 25 == 0 or index == len(normalized):
                progress = {
                    "as_of_date": str(params.as_of_date), "screen_version": params.screen_version,
                    "normalized_members": len(normalized), "members_processed": index,
                    "main_board_company_equities": main_board_count,
                    "nse_market_cap_attempts": market_cap_attempts,
                    "nse_market_cap_checkpoint_hits": checkpoint_hits,
                    "updated_at": datetime.now(UTC).isoformat(),
                }
                temporary = checkpoint_dir / "progress.json.tmp"
                temporary.write_text(json.dumps(progress, indent=2) + "\n", encoding="utf-8")
                temporary.replace(checkpoint_dir / "progress.json")
            for reason in reasons:
                if reason["code"] in {"SECURITY_IDENTITY_UNRESOLVED", "OFFICIAL_MARKET_CAP_UNAVAILABLE", "PHASE1_FILING_DISCOVERY_REQUIRED"}:
                    issue_id = f"dq:{row['member_key']}:{reason['code']}"
                    dq_issues.append({
                        "issue_id": issue_id, "company_id": company_id, "security_id": security_id,
                        "domain": "identity" if "IDENTITY" in reason["code"] else "market" if "MARKET_CAP" in reason["code"] else "fundamental",
                        "code": reason["code"], "severity": "BLOCKING", "state": "OPEN",
                        "message": reason["message"],
                        "source_artifact_id": (reason.get("source_artifact_ids") or [None])[0],
                    })
                    if reason["code"] != "OFFICIAL_MARKET_CAP_UNAVAILABLE":
                        repairs.append({
                            "repair_id": f"repair:{row['member_key']}:{reason['code']}",
                            "company_id": company_id, "domain": "identity" if "IDENTITY" in reason["code"] else "fundamental",
                            "reason_code": reason["code"], "required_action": reason["message"],
                        })

        resolved = sum(member["identity_status"] == "RESOLVED" for member in members)
        unresolved = sum(member["identity_status"] == "UNRESOLVED" for member in members)
        eligible_count = sum(member["market_cap_status"] == "ELIGIBLE" for member in members)
        payload = self._finish_payload(
            params, registry, artifacts, members, started_at,
            eligible_count=eligible_count, dq_issues=dq_issues, repairs=repairs,
            price_cutoff=identity["effective_date"],
        )
        payload["identity_coverage"] = resolved / (resolved + unresolved) if resolved + unresolved else 0.0
        payload["identity_resolved_count"] = resolved
        payload["identity_unresolved_count"] = unresolved
        payload["universe_size"] = len(members)
        return payload

    @staticmethod
    def _market_cap_checkpoint_stem(symbol: str, isin: str) -> str:
        safe_symbol = re.sub(r"[^A-Z0-9_.-]+", "_", symbol.upper()).strip("_") or "UNKNOWN"
        return f"{safe_symbol}_{content_hash([symbol.upper(), isin])[:16]}"

    def _load_market_cap_checkpoint(self, root: Path, *, symbol: str, isin: str,
                                    as_of_date: date) -> tuple[dict, dict] | None:
        stem = self._market_cap_checkpoint_stem(symbol, isin)
        metadata_path = root / f"{stem}.json"
        raw_path = root / f"{stem}.bin"
        if not metadata_path.is_file() or not raw_path.is_file():
            return None
        try:
            saved = json.loads(metadata_path.read_text(encoding="utf-8"))
            if saved.get("symbol") != symbol or saved.get("isin") != isin:
                return None
            if saved.get("as_of_date") != str(as_of_date):
                return None
            raw = raw_path.read_bytes()
            artifact = saved["artifact"]
            if hashlib.sha256(raw).hexdigest() != artifact.get("content_hash"):
                return None
            if artifact.get("validation_status") != "VALID":
                return None
            artifact["_raw"] = raw
            artifact["effective_date"] = date.fromisoformat(artifact["effective_date"])
            artifact["retrieved_at"] = datetime.fromisoformat(artifact["retrieved_at"])
            cap = saved["cap"]
            if cap.get("as_of_date"):
                cap["as_of_date"] = date.fromisoformat(cap["as_of_date"])
            return cap, artifact
        except (KeyError, TypeError, ValueError, OSError, json.JSONDecodeError):
            return None

    def _save_market_cap_checkpoint(self, root: Path, *, symbol: str, isin: str,
                                    as_of_date: date, cap: dict, artifact: dict) -> None:
        raw = artifact.get("_raw")
        if not isinstance(raw, bytes):
            return
        stem = self._market_cap_checkpoint_stem(symbol, isin)
        raw_temporary = root / f"{stem}.bin.tmp"
        metadata_temporary = root / f"{stem}.json.tmp"
        raw_temporary.write_bytes(raw)
        saved = {
            "symbol": symbol, "isin": isin, "as_of_date": str(as_of_date),
            "cap": cap, "artifact": {key: value for key, value in artifact.items() if key != "_raw"},
        }
        metadata_temporary.write_text(
            json.dumps(saved, indent=2, default=str) + "\n", encoding="utf-8",
        )
        raw_temporary.replace(root / f"{stem}.bin")
        metadata_temporary.replace(root / f"{stem}.json")

    @staticmethod
    def _normalize_full_universe(identity: dict, *, combined_artifact_id: str,
                                 nse_artifact_id: str, bse_artifact_id: str) -> list[dict]:
        by_key: dict[str, dict] = {}
        valid_equity_isin = re.compile(r"^INE[A-Z0-9]{9}$")
        non_equity_isin = re.compile(r"^IN[A-Z0-9]{10}$")

        def bucket(isin: str, fallback: str) -> dict:
            cleaned = isin.strip().upper()
            key = cleaned if cleaned and cleaned not in {"NA", "N/A", "NONE"} else fallback
            return by_key.setdefault(key, {
                "isin": cleaned if valid_equity_isin.fullmatch(cleaned) else "",
                "raw_isin": cleaned, "nse_rows": [], "bse_rows": [],
            })

        for row in identity["nse_rows"]:
            symbol = str(row.get("SYMBOL") or "").strip().upper()
            isin = str(row.get("ISIN NUMBER") or "").strip().upper()
            bucket(isin, f"NSE:{symbol}")["nse_rows"].append(row)
        for row in identity["bse_rows"]:
            if str(row.get("Status") or "").strip().upper() != "ACTIVE":
                continue
            if str(row.get("Segment") or "").strip().upper() != "EQUITY":
                continue
            code = str(row.get("SCRIP_CD") or "").strip()
            isin = str(row.get("ISIN_NUMBER") or "").strip().upper()
            bucket(isin, f"BSE:{code}")["bse_rows"].append(row)

        result: list[dict] = []
        used_symbols: dict[str, int] = {}
        for key, grouped in sorted(by_key.items()):
            nse_rows = grouped["nse_rows"]
            bse_rows = grouped["bse_rows"]
            raw_isin = grouped["raw_isin"]
            isin = grouped["isin"]
            primary_nse = nse_rows[0] if nse_rows else None
            primary_bse = bse_rows[0] if bse_rows else None
            base_symbol = str(
                (primary_nse or {}).get("SYMBOL")
                or (primary_bse or {}).get("scrip_id")
                or (primary_bse or {}).get("SCRIP_CD")
                or key
            ).strip().upper()
            used_symbols[base_symbol] = used_symbols.get(base_symbol, 0) + 1
            symbol = base_symbol if used_symbols[base_symbol] == 1 else f"{base_symbol}#{used_symbols[base_symbol]}"
            company = str(
                (primary_nse or {}).get("NAME OF COMPANY")
                or (primary_bse or {}).get("Issuer_Name")
                or (primary_bse or {}).get("Scrip_Name")
                or symbol
            ).strip()
            listings: list[dict] = []
            for row in nse_rows:
                series = str(row.get("SERIES") or "").strip().upper()
                listings.append({
                    "listing_id": f"listing:NSE:{row.get('SYMBOL')}:{raw_isin or key}",
                    "exchange": "NSE", "symbol": str(row.get("SYMBOL") or "").strip().upper(),
                    "series": series, "board": "MAIN" if series in {"EQ", "BE", "BZ"} else "BOARD_UNKNOWN",
                    "active_flag": True,
                    "listing_date": PersistentScreenerService._parse_listing_date(row.get("DATE OF LISTING")),
                })
            for row in bse_rows:
                group = str(row.get("GROUP") or "").strip().upper()
                listings.append({
                    "listing_id": f"listing:BSE:{row.get('SCRIP_CD')}:{raw_isin or key}",
                    "exchange": "BSE", "symbol": str(row.get("scrip_id") or "").strip().upper(),
                    "bse_code": str(row.get("SCRIP_CD") or "").strip(),
                    "exchange_security_id": str(row.get("SCRIP_CD") or "").strip(),
                    "series": group, "board": "SME" if group in {"M", "MS", "MT", "TS"} else "MAIN",
                    "active_flag": True, "listing_date": None,
                })
            if isin:
                identity_status = "RESOLVED"
                identity_reason = "Exact valid equity ISIN from active official exchange master."
            elif non_equity_isin.fullmatch(raw_isin):
                identity_status = "NON_EQUITY_INSTRUMENT"
                identity_reason = f"Official identifier {raw_isin} is not an INE company-equity ISIN."
            else:
                identity_status = "UNRESOLVED"
                identity_reason = "Active exchange equity listing has no valid company-equity ISIN."
            source_ids = [combined_artifact_id]
            if nse_rows:
                source_ids.append(nse_artifact_id)
            if bse_rows:
                source_ids.append(bse_artifact_id)
            result.append({
                "member_key": content_hash([key, symbol])[:24], "symbol": symbol,
                "company": company, "isin": isin, "raw_isin": raw_isin,
                "identity_status": identity_status, "identity_reason": identity_reason,
                "listings": listings, "source_artifact_ids": list(dict.fromkeys(source_ids)),
                "face_value": OfficialExchangeClient._float(
                    (primary_bse or {}).get("FACE_VALUE") or (primary_nse or {}).get("FACE VALUE")
                ),
                "bse_market_row": primary_bse,
            })
        return result

    @staticmethod
    def _parse_listing_date(value) -> date | None:
        text = str(value or "").strip()
        if not text:
            return None
        for fmt in ("%d-%b-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
        return None

    def _regression_payload(self, params, fixture, fixture_raw, fixture_path, fixture_version,
                            fixture_versions_raw, registry, registry_raw, kpi_contracts, kpi_raw,
                            regression, regression_raw, started_at):
        artifacts = [
            _artifact("canary_fixture", "REPOSITORY", fixture_raw, url=self._relative_url(fixture_path), effective_date=params.as_of_date, row_count=17, metadata={"fixture_version": fixture_version}),
            _artifact("canary_fixture_versions", "REPOSITORY", fixture_versions_raw, url="configs/research_screener/canary_fixture_versions.json", effective_date=params.as_of_date, row_count=len(json.loads(fixture_versions_raw)["versions"])),
            _artifact("historical_regression", "FROZEN_FIXTURE", regression_raw, url="tests/fixtures/research_screener/regression_2026-08-08.json", effective_date=date.fromisoformat(regression["as_of_date"]), row_count=17),
            _artifact("source_registry", "REPOSITORY", registry_raw, url="configs/research_screener/source_registry.yaml", effective_date=params.as_of_date, row_count=len(registry["sources"])),
            _artifact("kpi_contracts", "REPOSITORY", kpi_raw, url="configs/research_screener/kpi_contracts.json", effective_date=params.as_of_date, row_count=len(kpi_contracts["contracts"]), metadata={"contract_version": kpi_contracts["contract_version"]}),
        ]
        members = []
        for row in fixture:
            reasons = [{"code": "HISTORICAL_AGGREGATE_ONLY", "message": "Replay preserves the supplied pilot facts without treating them as current company-level market data.", "source_artifact_ids": [artifacts[2]["artifact_id"]]}]
            if row["symbol"] in regression["silently_lost_symbols"]:
                reasons.append({"code": "OLD_INDEX_SILENT_LOSS_REPRODUCED", "message": "The old index route lost this company; ISIN-first membership preserves it.", "source_artifact_ids": [artifacts[2]["artifact_id"]]})
            if row["symbol"] in regression["forced_consolidated_required_fields"]:
                reasons.append({"code": "FORCED_CONSOLIDATED_FAILURE_REPRODUCED", "message": f"Old consolidated completeness was {regression['forced_consolidated_required_fields'][row['symbol']]}; standalone was usable.", "source_artifact_ids": [artifacts[2]["artifact_id"]]})
            if row["symbol"] == "SJS":
                reasons.append({"code": "SHORT_HISTORY_NOT_ZERO", "message": "Five annual observations cannot be converted to a zero five-year CAGR.", "source_artifact_ids": [artifacts[2]["artifact_id"]]})
            member = self._base_member(row, params, artifacts[0]["artifact_id"]) | {
                "identity_status": "FROZEN_FIXTURE_ONLY", "market_cap_status": "HISTORICAL_AGGREGATE_ONLY",
                "market_cap_cr": None, "market_cap_as_of": None, "statement_scope": "standalone" if row["symbol"] in {"HAWKINCOOK", "E2E"} else "SCOPE_NOT_REPLAYED",
                "annual_completeness": None, "quarterly_completeness": None, "corporate_action_status": "NOT_REPLAYED",
                "data_confidence": 0.5, "technical_status": "UNAVAILABLE", "fundamental_score": None,
                "disposition": Disposition.BOUNDARY_REVIEW.value, "reasons": reasons,
                "inputs": {"historical_regression": regression, "market": {}, "fundamentals": {}, "ohlcv": [], "corporate_actions": [], "kpi_contract": self._select_kpi_contract(row["symbol"], "BANK" if row["symbol"] in BANKS else "MARKET_INFRASTRUCTURE" if row["symbol"] in MARKET_INFRASTRUCTURE else "CORPORATE", kpi_contracts, artifacts[-1]["artifact_id"])},
            }
            member["company_id"] = f"company:fixture:{row['symbol']}"
            member["security_id"] = f"security:fixture:{row['symbol']}"
            members.append(member)
        return self._finish_payload(params, registry, artifacts, members, started_at, eligible_count=regression["historical_in_band_count"])

    def _live_payload(self, params, fixture, fixture_raw, fixture_path, fixture_version,
                      fixture_versions, fixture_versions_raw, registry, registry_raw,
                      kpi_contracts, kpi_raw, issuer_repairs, issuer_repair_raw, started_at):
        identity = self.exchange.acquire_identity(params.as_of_date)
        artifacts = list(identity["artifacts"])
        artifacts.extend([
            _artifact("canary_fixture", "REPOSITORY", fixture_raw, url=self._relative_url(fixture_path), effective_date=params.as_of_date, row_count=17, metadata={"fixture_version": fixture_version}),
            _artifact("canary_fixture_versions", "REPOSITORY", fixture_versions_raw, url="configs/research_screener/canary_fixture_versions.json", effective_date=params.as_of_date, row_count=len(fixture_versions["versions"])),
            _artifact("source_registry", "REPOSITORY", registry_raw, url="configs/research_screener/source_registry.yaml", effective_date=params.as_of_date, row_count=len(registry["sources"])),
            _artifact("kpi_contracts", "REPOSITORY", kpi_raw, url="configs/research_screener/kpi_contracts.json", effective_date=params.as_of_date, row_count=len(kpi_contracts["contracts"]), metadata={"contract_version": kpi_contracts["contract_version"]}),
            _artifact("issuer_filing_repairs", "REPOSITORY", issuer_repair_raw, url="configs/research_screener/issuer_filing_repairs.json", effective_date=params.as_of_date, row_count=len(issuer_repairs["symbols"]), metadata={"contract_version": issuer_repairs["contract_version"]}),
        ])
        kpi_artifact_id = artifacts[-2]["artifact_id"]
        issuer_repair_client = IssuerFilingRepairClient(self.exchange, issuer_repairs)
        combined_artifact_id = identity["artifacts"][0]["artifact_id"]
        resolved: dict[str, dict] = {}
        for row in fixture:
            resolved[row["isin"]] = {"symbol": row["symbol"], **self.exchange.resolve_fixture(row, identity)}
        local = ExistingRepositoryProvider(resolved, project_root=self.project_root)
        members: list[dict] = []
        dq_issues: list[dict] = []
        repairs: list[dict] = []
        local_snapshot_material: list[dict] = []

        bse_artifact_id = identity["artifacts"][2]["artifact_id"]
        for row in fixture:
            symbol, isin = row["symbol"], row["isin"]
            company_type = "BANK" if symbol in BANKS else "MARKET_INFRASTRUCTURE" if symbol in MARKET_INFRASTRUCTURE else "CORPORATE"
            ident = resolved[isin] | {"source_artifact_id": combined_artifact_id}
            market = local.market_snapshot(isin, params.as_of_date)
            ohlcv = local.get_ohlcv(isin, params.as_of_date - timedelta(days=370), params.as_of_date)
            actions = local.get_corporate_actions(isin, params.as_of_date)
            # Cover the adjusted-history continuity test as well as the 370-day output slice.
            # This captures known restructuring boundaries such as STLTECH's 2025 demerger.
            action_window_start = params.as_of_date - timedelta(days=730)
            identifier_history, transition_error = self._identifier_transition(
                row, actions, fixture_version, fixture_versions
            )
            if transition_error and ident["status"] == "RESOLVED":
                ident["status"] = "IDENTITY_CONFLICT"
                ident["identifier_transition_error"] = transition_error
            elif identifier_history:
                ident["identifier_history"] = identifier_history
                ident["security_valid_from"] = identifier_history[-1]["valid_from"]
            filing_identifier_history = self._filing_identifier_history(
                isin, actions, identifier_history,
            )
            local_fundamentals = local.fundamental_snapshot(isin, params.as_of_date, company_type=company_type)
            bse_rows = [x for x in identity["bse_rows"] if str(x.get("scrip_id", "")).upper() == symbol]
            if row["expected_exchange_scope"] == "BSE":
                bse_code = str(bse_rows[0].get("SCRIP_CD")) if bse_rows else ""
                fundamentals, filing_artifacts = self.exchange.bse_fundamental_snapshot(
                    bse_code, isin, params.as_of_date, company_type=company_type,
                    identifier_history=filing_identifier_history,
                ) if bse_code else (self.exchange._empty_fundamentals("BSE code unavailable"), [])
            else:
                nse_fundamentals, nse_artifacts = self.exchange.nse_fundamental_snapshot(
                    symbol, isin, params.as_of_date, company_type=company_type,
                    identifier_history=filing_identifier_history,
                )
                fundamentals, filing_artifacts = nse_fundamentals, list(nse_artifacts)
                if nse_fundamentals.get("state") != "PRESENT" and bse_rows:
                    bse_fundamentals, bse_artifacts = self.exchange.bse_fundamental_snapshot(
                        str(bse_rows[0].get("SCRIP_CD")), isin, params.as_of_date,
                        company_type=company_type, identifier_history=filing_identifier_history,
                    )
                    filing_artifacts.extend(bse_artifacts)
                    fundamentals = self._select_fundamental_provider(
                        nse_fundamentals, bse_fundamentals,
                    )
            fundamentals, issuer_artifacts = issuer_repair_client.augment(
                symbol, isin, fundamentals, params.as_of_date, company_type=company_type,
            )
            filing_artifacts.extend(issuer_artifacts)
            artifacts.extend(filing_artifacts)
            if row["expected_exchange_scope"] == "BSE" and bse_rows:
                official_actions, action_artifact = self.exchange.bse_corporate_actions(
                    str(bse_rows[0].get("SCRIP_CD")), symbol, isin, action_window_start, params.as_of_date,
                )
            else:
                official_actions, action_artifact = self.exchange.nse_corporate_actions(
                    symbol, isin, action_window_start, params.as_of_date,
                )
            artifacts.append(action_artifact)
            fundamentals["secondary_snapshot"] = local_fundamentals
            fundamentals["source_artifact_ids"] = [a["artifact_id"] for a in filing_artifacts]
            technical = local.technical_snapshot(isin, params.as_of_date)
            cap = None
            cap_artifact = None
            if row["expected_exchange_scope"] == "BSE" and ident["status"] == "RESOLVED":
                cap = self.exchange.bse_market_cap(bse_rows[0], artifact_id=bse_artifact_id) if bse_rows else None
            elif ident["status"] == "RESOLVED":
                cap, cap_artifact = self.exchange.nse_market_cap(symbol, params.as_of_date, expected_isin=isin)
                artifacts.append(cap_artifact)

            cap_value = cap.get("full_market_cap_cr") if cap else None
            if cap_value is None:
                cap_status = "ELIGIBILITY_UNKNOWN"
            elif params.min_market_cap_cr <= cap_value <= params.max_market_cap_cr:
                cap_status = "ELIGIBLE"
            else:
                cap_status = "INELIGIBLE_MARKET_CAP"

            ca_status, action_validation = self._validate_action_history(
                official_actions, actions, action_artifact,
            )
            technical_status = technical.get("status", "UNAVAILABLE") if ca_status == "VALIDATED" else "UNAVAILABLE"
            reasons: list[dict] = []
            reason_artifacts = [combined_artifact_id]
            if cap:
                reason_artifacts.append(cap["artifact_id"])
            if ident["status"] != "RESOLVED":
                disposition = Disposition.DATA_REPAIR_REQUIRED.value
                if transition_error:
                    reasons.append({"code": "IDENTIFIER_TRANSITION_UNPROVEN", "message": transition_error, "source_artifact_ids": [combined_artifact_id]})
                else:
                    reasons.append({"code": "IDENTITY_CONFLICT", "message": f"Fixture ISIN {isin} does not match the official symbol identity; observed {ident.get('observed_isins')} and fixture ISIN maps to {ident.get('fixture_isin_matches_other_bse')}.", "source_artifact_ids": [combined_artifact_id, bse_artifact_id]})
            elif cap_status == "ELIGIBILITY_UNKNOWN":
                disposition = Disposition.ELIGIBILITY_UNKNOWN.value
                reasons.append({"code": "OFFICIAL_MARKET_CAP_UNAVAILABLE", "message": "The fixed official market-cap source did not return a usable dated full market cap; no estimate or provider switch was made.", "source_artifact_ids": [cap_artifact["artifact_id"]] if cap_artifact else [bse_artifact_id]})
            elif cap_status == "INELIGIBLE_MARKET_CAP":
                disposition = Disposition.INELIGIBLE_MARKET_CAP.value
                reasons.append({"code": "DATED_FULL_MARKET_CAP_OUTSIDE_BAND", "message": f"Official full market cap {cap_value} crore is outside [{params.min_market_cap_cr}, {params.max_market_cap_cr}].", "source_artifact_ids": reason_artifacts})
            elif fundamentals["state"] == "DATA_REPAIR_REQUIRED" or min(fundamentals["annual_completeness"], fundamentals["quarterly_completeness"]) < 0.70:
                disposition = Disposition.DATA_REPAIR_REQUIRED.value
                reasons.append({"code": "FUNDAMENTAL_PROVENANCE_OR_COMPLETENESS_FAILED", "message": fundamentals.get("provenance_validation", {}).get("reason", "Mandatory discovery completeness below 70%."), "source_artifact_ids": fundamentals["source_artifact_ids"]})
            else:
                disposition = Disposition.BOUNDARY_REVIEW.value
                reasons.append({"code": "MINIMAL_CANARY_REVIEW", "message": "Canary evidence passed Phase 0 checks and awaits boundary review; no production ranking weights are implied.", "source_artifact_ids": reason_artifacts})
            if identifier_history:
                reasons.append({"code": "EFFECTIVE_DATED_IDENTIFIER_TRANSITION_VALIDATED", "message": f"Official split evidence bridges {identifier_history[0]['identifier_value']} through {identifier_history[0]['valid_to']} to {identifier_history[1]['identifier_value']} from {identifier_history[1]['valid_from']}.", "source_artifact_ids": []})
            if ca_status != "VALIDATED":
                reasons.append({"code": "CORPORATE_ACTION_TAXONOMY_INCOMPLETE", "message": action_validation["reason"], "source_artifact_ids": [action_artifact["artifact_id"]]})
            identity_score = 0.3 if ident["status"] == "RESOLVED" else 0.0
            confidence = round(identity_score + (0.2 if cap_value is not None else 0) + (0.1 if market.get("freshness_status") == "FRESH" else 0) + (0.1 if fundamentals["state"] == "PRESENT" else 0) + (0.1 if ca_status == "VALIDATED" else 0), 2)
            kpi_contract = self._select_kpi_contract(symbol, company_type, kpi_contracts, kpi_artifact_id)
            member = self._base_member(row, params, combined_artifact_id) | {
                "company_type": company_type, "identity": ident, "identity_status": ident["status"],
                "market_cap_status": cap_status, "market_cap_cr": cap_value,
                "market_cap_as_of": cap.get("as_of_date", identity["effective_date"]) if cap_value is not None else None,
                "statement_scope": fundamentals["scope"], "annual_completeness": fundamentals["annual_completeness"],
                "quarterly_completeness": fundamentals["quarterly_completeness"], "corporate_action_status": ca_status,
                "data_confidence": confidence, "technical_status": technical_status, "fundamental_score": None,
                "disposition": disposition, "reasons": reasons,
                "inputs": {"market": market, "fundamentals": fundamentals, "technical_observation_suppressed": technical,
                           "ohlcv": ohlcv, "corporate_actions": actions,
                           "official_corporate_actions": official_actions,
                           "corporate_action_validation": action_validation,
                           "kpi_contract": kpi_contract},
            }
            if ident["status"] != "RESOLVED":
                member["company_id"] = f"company:fixture:{symbol}"
                member["security_id"] = f"security:unresolved:{symbol}"
            members.append(member)
            local_snapshot_material.append({"symbol": symbol, "market": market, "fundamentals": fundamentals, "ohlcv": ohlcv, "actions": actions, "official_actions": official_actions})
            for reason in reasons:
                if reason["code"] in {"IDENTITY_CONFLICT", "IDENTIFIER_TRANSITION_UNPROVEN", "OFFICIAL_MARKET_CAP_UNAVAILABLE", "FUNDAMENTAL_PROVENANCE_OR_COMPLETENESS_FAILED", "CORPORATE_ACTION_TAXONOMY_INCOMPLETE"}:
                    issue_id = f"dq:{symbol}:{reason['code']}"
                    dq_issues.append({"issue_id": issue_id, "company_id": member["company_id"], "security_id": member["security_id"],
                                      "domain": reason["code"].split("_")[0].lower(), "code": reason["code"], "severity": "BLOCKING",
                                      "state": "OPEN", "message": reason["message"], "source_artifact_id": (reason.get("source_artifact_ids") or [None])[0]})
                    if reason["code"] != "OFFICIAL_MARKET_CAP_UNAVAILABLE":
                        repairs.append({"repair_id": f"repair:{symbol}:{reason['code']}", "company_id": member["company_id"],
                                        "domain": reason["code"].split("_")[0].lower(), "reason_code": reason["code"],
                                        "required_action": reason["message"]})

        local_raw = canonical_json(local_snapshot_material).encode("utf-8")
        local_artifact = _artifact("existing_repository_snapshot", "EXISTING_TRADING_SYSTEM", local_raw,
                                   url="ohlcv.duckdb + fundamentals/screener_financials.db", effective_date=params.as_of_date,
                                   row_count=len(local_snapshot_material), metadata={"read_only": True})
        artifacts.append(local_artifact)
        for member in members:
            for identifier in member["identity"].get("identifier_history", []):
                identifier["source_artifact_id"] = (
                    local_artifact["artifact_id"]
                    if identifier.pop("source_role") == "CORPORATE_ACTION"
                    else combined_artifact_id
                )
            for reason in member["reasons"]:
                if reason["code"] == "CORPORATE_ACTION_TAXONOMY_INCOMPLETE":
                    reason["source_artifact_ids"] = list(dict.fromkeys(
                        reason.get("source_artifact_ids", []) + [local_artifact["artifact_id"]]
                    ))
                elif reason["code"] == "EFFECTIVE_DATED_IDENTIFIER_TRANSITION_VALIDATED":
                    reason["source_artifact_ids"] = [combined_artifact_id, local_artifact["artifact_id"]]
        eligible_count = sum(m["market_cap_status"] == "ELIGIBLE" for m in members)
        return self._finish_payload(params, registry, artifacts, members, started_at, eligible_count=eligible_count, dq_issues=dq_issues, repairs=repairs, price_cutoff=identity["effective_date"])

    def _finish_payload(self, params, registry, artifacts, members, started_at, *, eligible_count, dq_issues=None, repairs=None, price_cutoff=None):
        artifacts = list({artifact["artifact_id"]: artifact for artifact in artifacts}.values())
        ingestions = []
        for artifact in artifacts:
            artifact_key = content_hash(artifact["artifact_id"])[:24]
            ingestion_run_id = (
                f"ingest:{params.run_mode.value}:{params.as_of_date}:"
                f"{params.screen_version}:{artifact_key}"
            )
            artifact["ingestion_run_id"] = ingestion_run_id
            ingestions.append({
                "ingestion_run_id": ingestion_run_id,
                "source_key": artifact["source_key"],
                "effective_date": artifact.get("effective_date") or params.as_of_date,
                "started_at": artifact["retrieved_at"],
                "ended_at": artifact["retrieved_at"],
                "status": "COMPLETED" if artifact["validation_status"] == "VALID" else "FAILED",
                "error_code": None if artifact["validation_status"] == "VALID" else artifact["validation_status"],
                "error_message": artifact.get("metadata", {}).get("error"),
            })
        input_material = {
            "mode": params.run_mode.value, "as_of_date": params.as_of_date, "screen_version": params.screen_version,
            "artifact_hashes": sorted(a["content_hash"] for a in artifacts),
            "members": [
                {
                    "symbol": m["symbol"], "fixture_isin": m["isin"], "identity": m["identity"],
                    "identity_status": m["identity_status"], "market_cap_cr": m.get("market_cap_cr"),
                    "market_cap_status": m["market_cap_status"], "statement_scope": m["statement_scope"],
                    "annual_completeness": m.get("annual_completeness"),
                    "quarterly_completeness": m.get("quarterly_completeness"),
                    "corporate_action_status": m["corporate_action_status"],
                    "technical_status": m["technical_status"], "disposition": m["disposition"],
                    "reasons": m["reasons"], "inputs": m["inputs"],
                }
                for m in members
            ],
        }
        snapshot_hash = content_hash(input_material)
        run_id = f"screen-{params.run_mode.value}-{params.as_of_date}-{snapshot_hash[:16]}"
        return {
            "run_id": run_id, "run_mode": params.run_mode.value, "as_of_date": params.as_of_date,
            "price_cutoff": price_cutoff or params.as_of_date, "definition": params.screen_definition,
            "screen_version": params.screen_version, "min_market_cap_cr": params.min_market_cap_cr,
            "max_market_cap_cr": params.max_market_cap_cr, "source_registry": registry,
            "rules": FULL_UNIVERSE_RULES if params.run_mode == RunMode.FULL_UNIVERSE
            else FILING_DISCOVERY_RULES if params.run_mode == RunMode.FILING_DISCOVERY
            else RULES,
            "artifacts": artifacts, "ingestions": ingestions, "members": members, "eligible_count": eligible_count,
            "dq_issues": dq_issues or [], "repairs": repairs or [], "started_at": started_at,
            "input_snapshot_hash": snapshot_hash,
            "universe_hash": content_hash([[m["security_id"], m["symbol"]] for m in members]),
            "code_version": self._git_sha(),
        }

    @staticmethod
    def _select_kpi_contract(symbol: str, company_type: str, contracts: dict,
                             source_artifact_id: str) -> dict:
        override = contracts.get("company_overrides", {}).get(symbol, {})
        contract_name = override.get("contract") or contracts["routing"][company_type]
        return {
            "contract_version": contracts["contract_version"], "contract_name": contract_name,
            "definition": contracts["contracts"][contract_name], "company_override": override,
            "observation_state": "NOT_DISCLOSED", "source_artifact_id": source_artifact_id,
        }

    @staticmethod
    def _select_fundamental_provider(primary: dict, fallback: dict) -> dict:
        """Choose one official filing snapshot without cross-provider period splicing."""
        def quality(snapshot: dict) -> tuple[bool, bool, float, float]:
            disclosed = snapshot.get("latest_disclosed_periods", {})
            parsed = snapshot.get("latest_parsed_periods", {})
            latest_matched = all(
                disclosed.get(kind) is not None and parsed.get(kind) == disclosed.get(kind)
                for kind in ("annual", "quarterly")
            )
            annual = float(snapshot.get("annual_completeness") or 0.0)
            quarterly = float(snapshot.get("quarterly_completeness") or 0.0)
            return snapshot.get("state") == "PRESENT", latest_matched, min(annual, quarterly), annual + quarterly

        selected_name = "fallback" if quality(fallback) > quality(primary) else "primary"
        selected = dict(fallback if selected_name == "fallback" else primary)
        selected["provider_selection"] = {
            "policy": "ordered-official-snapshot-v1-no-period-splicing",
            "primary_provider": primary.get("provenance_validation", {}).get("provider", []),
            "fallback_provider": fallback.get("provenance_validation", {}).get("provider", []),
            "selected": selected_name,
            "primary_quality": quality(primary),
            "fallback_quality": quality(fallback),
        }
        return selected

    @staticmethod
    def _validate_action_history(official_actions: list[dict], stored_actions: list[dict],
                                 source_artifact: dict) -> tuple[str, dict]:
        if source_artifact["validation_status"] != "VALID":
            return "ADJUSTMENT_INCOMPLETE", {
                "reason": "The fixed official corporate-action feed failed; adjusted technical and per-share conclusions are suppressed.",
                "required_events": [], "unmatched_events": [],
            }
        adjustment_types = {"split", "bonus", "rights", "consolidation", "demerger", "merger"}
        required = [row for row in official_actions if row["action_type"] in adjustment_types]
        unmatched = []
        for event in required:
            match = next((row for row in stored_actions if str(row.get("action_type", "")).lower() == event["action_type"]
                          and str(row.get("ex_date")) == str(event["ex_date"])
                          and str(row.get("status", "")).lower() not in {"failed", "invalid", "quarantined"}), None)
            if match is None:
                unmatched.append(event)
        if unmatched:
            return "ADJUSTMENT_INCOMPLETE", {
                "reason": f"Official actions requiring adjustment are not validated in the adjusted-price store: {[(x['action_type'], str(x['ex_date'])) for x in unmatched]}.",
                "required_events": required, "unmatched_events": unmatched,
            }
        return "VALIDATED", {
            "reason": "The official action feed was acquired and every adjustment-requiring event in the requested OHLC window matched the stored action history.",
            "required_events": required, "unmatched_events": [],
        }

    def _base_member(self, row: dict, params: ScreeningParameters, source_artifact_id: str) -> dict:
        company_id = f"company:{row['isin']}"
        security_id = f"security:{row['isin']}"
        return {
            "symbol": row["symbol"], "company": row["company"], "isin": row["isin"],
            "company_id": company_id, "security_id": security_id, "company_type": "BANK" if row["symbol"] in BANKS else "MARKET_INFRASTRUCTURE" if row["symbol"] in MARKET_INFRASTRUCTURE else "CORPORATE",
            "as_of_date": params.as_of_date,
            "identity": {"source_artifact_id": source_artifact_id, "listings": [], "observed_isins": [row["isin"]]},
        }

    def _resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.project_root / path

    def _fixture_path(self, params: ScreeningParameters, manifest: dict) -> Path:
        if params.canary_file is not None:
            return self._resolve(params.canary_file)
        version = "1.0.0" if params.run_mode == RunMode.REGRESSION_REPLAY else manifest["current_fixture_version"]
        return self._resolve(Path(manifest["versions"][version]["path"]))

    def _relative_url(self, path: Path) -> str:
        try:
            return str(path.relative_to(self.project_root))
        except ValueError:
            return str(path)

    @staticmethod
    def _identifier_transition(row: dict, actions: list[dict], fixture_version: str,
                               manifest: dict) -> tuple[list[dict], str | None]:
        corrections = manifest["versions"][fixture_version].get("corrections", [])
        contract = next((item for item in corrections if item.get("symbol") == row["symbol"]
                         and item.get("transition_kind") == "effective_dated_identifier_transition"), None)
        if contract is None:
            return [], None
        if contract.get("field") != "isin" or contract.get("new_value") != row["isin"]:
            return [], f"{row['symbol']} identifier-transition contract does not match the current fixture ISIN."
        evidence = contract["corporate_action_evidence"]
        expected = {
            "stored_isin": evidence["isin"], "action_type": evidence["action_type"],
            "ratio": evidence["parsed_ratio"], "ex_date": evidence["ex_date"],
            "source": evidence["source"], "source_row_hash": evidence["raw_payload_hash"],
        }
        match = next((action for action in actions if all(
            str(action.get(key)) == str(value) for key, value in expected.items()
        )), None)
        raw_payload_json = match.get("raw_payload_json") if match else None
        raw_payload_matches_hash = bool(
            raw_payload_json
            and hashlib.sha256(raw_payload_json.encode("utf-8")).hexdigest() == evidence["raw_payload_hash"]
        )
        if match is None or not raw_payload_matches_hash:
            return [], (
                f"{row['symbol']} current ISIN {row['isin']} is visible in official masters, but the frozen "
                "corporate-action row does not match the registered old ISIN, split ratio, ex-date, source, "
                "raw payload, and payload hash."
            )
        effective_date = date.fromisoformat(contract["effective_date"])
        prior_start = date.fromisoformat(contract["prior_identifier_observation_start"])
        prior_end = effective_date - timedelta(days=1)
        if prior_start > prior_end:
            return [], f"{row['symbol']} prior identifier observation starts after its transition boundary."
        return [
            {
                "identifier_type": "ISIN", "identifier_value": contract["old_value"],
                "exchange": None, "valid_from": prior_start, "valid_to": prior_end,
                "source_role": "CORPORATE_ACTION",
            },
            {
                "identifier_type": "ISIN", "identifier_value": contract["new_value"],
                "exchange": None, "valid_from": effective_date, "valid_to": None,
                "source_role": "CURRENT_IDENTITY_MASTER",
            },
        ], None

    @staticmethod
    def _filing_identifier_history(current_isin: str, actions: list[dict],
                                   canonical_history: list[dict]) -> list[dict]:
        """Add action-proven prior ISIN windows for document validation only.

        The split row proves the prior ISIN through the day before the ex-date,
        but not its original issue date. These supplemental rows therefore do
        not enter canonical identifier history.
        """
        history = [dict(row) for row in canonical_history]
        known = {
            str(row.get("identifier_value") or "").upper(): row
            for row in history
        }
        for action in actions:
            prior_isin = str(action.get("stored_isin") or "").strip().upper()
            if not prior_isin or prior_isin == current_isin.upper():
                continue
            if str(action.get("action_type") or "").lower() not in {"split", "consolidation"}:
                continue
            if str(action.get("source") or "") != "nse_corporate_actions":
                continue
            if str(action.get("status") or "").lower() in {"failed", "invalid", "quarantined", "inactive"}:
                continue
            raw_payload = action.get("raw_payload_json")
            raw_hash = action.get("source_row_hash")
            if not raw_payload or hashlib.sha256(str(raw_payload).encode("utf-8")).hexdigest() != raw_hash:
                continue
            ex_date = action.get("ex_date")
            if isinstance(ex_date, str):
                ex_date = date.fromisoformat(ex_date)
            if not isinstance(ex_date, date):
                continue
            filing_window = {
                "identifier_type": "ISIN", "identifier_value": prior_isin,
                "exchange": "NSE", "valid_from": None,
                "valid_to": ex_date - timedelta(days=1),
                "source_role": "CORPORATE_ACTION_FILING_ONLY",
            }
            existing = known.get(prior_isin)
            if existing is None:
                history.append(filing_window)
                known[prior_isin] = filing_window
            else:
                # The canonical transition may deliberately prove only the day
                # immediately before the split.  A checksum-valid official
                # action under the prior ISIN separately proves that identifier
                # for historical filing validation through the ex-date boundary.
                existing.update(filing_window)
        return history

    @staticmethod
    def _validate_fixture(rows: list[dict], run_mode: RunMode, manifest: dict) -> str:
        if len(rows) != 17:
            raise ValueError(f"canary fixture must contain exactly 17 rows, found {len(rows)}")
        symbols = [row["symbol"] for row in rows]
        isins = [row["isin"] for row in rows]
        if len(set(symbols)) != 17 or len(set(isins)) != 17:
            raise ValueError("canary fixture symbols and fixture ISINs must be unique")
        versions = {row.get("fixture_version") for row in rows}
        if len(versions) != 1 or None in versions or "" in versions:
            raise ValueError("canary fixture must declare exactly one non-empty fixture_version")
        fixture_version = versions.pop()
        version_contract = manifest.get("versions", {}).get(fixture_version)
        if version_contract is None:
            raise ValueError(f"canary fixture version {fixture_version!r} is not registered")
        allowed_modes = version_contract.get("allowed_run_modes", [RunMode.LIVE_CANARY.value])
        if run_mode.value not in allowed_modes:
            raise ValueError(f"canary fixture version {fixture_version} is not allowed for {run_mode.value}")
        if run_mode == RunMode.LIVE_CANARY and fixture_version != manifest["current_fixture_version"]:
            raise ValueError("live canary must use the registered current fixture version")
        return fixture_version

    def _git_sha(self) -> str | None:
        try:
            return subprocess.run(["git", "rev-parse", "HEAD"], cwd=self.project_root, check=True, capture_output=True, text=True).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return None
