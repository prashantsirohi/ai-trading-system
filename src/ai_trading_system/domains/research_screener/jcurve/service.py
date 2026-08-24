from __future__ import annotations

import csv
import json
import shutil
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.domains.research_screener.store import content_hash

from .cohort import BaselineCohort
from .documents import select_relevant_pages
from .market_intel_adapter import MarketIntelAnnouncementAdapter
from .model_router import ModelRouterError, OpenRouterModelRouter
from .models import AnnouncementRecord, EvidencePacket, EvidencePage, ModelRoute
from .policy import CapexStagePolicy, MaterialityInputs
from .store import JCurveStore


IMPORT_POLICY_VERSION = "jcurve-market-intel-import-v1"
IMPORT_POLICY_HASH = content_hash({
    "version": IMPORT_POLICY_VERSION,
    "sources": ["nse_rss", "bse_corp", "nse_api"],
    "availability": "published_at_and_ingested_at_not_after_cutoff",
    "identity": "research_screener_point_in_time",
})


class JCurveImportService:
    def __init__(self, *, store_path: Path, output_root: Path):
        self.store = JCurveStore(store_path)
        self.store_path = Path(store_path)
        self.output_root = Path(output_root)

    def run(
        self, *, market_intel_db: Path, published_from: date, as_of_date: date,
        run_type: str = "BOOTSTRAP", cohort: BaselineCohort | None = None,
        upstream_filter_policy: str | None = None,
    ) -> dict[str, Any]:
        if published_from > as_of_date:
            raise ValueError("published_from must not be after as_of_date")
        started_at = datetime.now(UTC)
        resolved_cohort = cohort.resolve(store_path=self.store_path, as_of_date=as_of_date) if cohort else None
        adapter = MarketIntelAnnouncementAdapter(
            market_intel_db=market_intel_db, screener_db=self.store_path,
        )
        records = adapter.read(
            published_from=published_from, as_of_date=as_of_date,
            filter_policy_version=upstream_filter_policy,
        )
        coverage = adapter.coverage_receipts(
            published_from=published_from, as_of_date=as_of_date,
            filter_policy_version=upstream_filter_policy,
        ) if upstream_filter_policy else None
        if resolved_cohort:
            company_set = set(resolved_cohort.company_ids)
            records = [record for record in records if record.company_id in company_set]
        snapshot_hash = content_hash({
            "policy_hash": IMPORT_POLICY_HASH, "from": published_from,
            "as_of": as_of_date, "events": [
                [record.upstream_event_hash, record.raw_content_hash, record.attachment_content_hash]
                for record in records
            ],
            "cohort_policy_hash": resolved_cohort.policy_hash if resolved_cohort else None,
            "cohort_company_ids": resolved_cohort.company_ids if resolved_cohort else None,
            "upstream_filter_policy": upstream_filter_policy,
            "upstream_coverage": coverage,
        })
        run_id = f"jcurve-import-{as_of_date}-{snapshot_hash[:16]}"
        output_dir = self.output_root / run_id
        if self.store.completed_run(run_id) and output_dir.is_dir():
            return {"run_id": run_id, "status": "COMPLETED", "reused": True, "output_dir": str(output_dir)}
        if output_dir.exists():
            raise FileExistsError(f"incomplete J-curve output already exists: {output_dir}")
        output_dir.mkdir(parents=True)
        source_dir = output_dir / "source"
        source_dir.mkdir()
        artifacts: list[dict[str, Any]] = []
        announcements: list[dict[str, Any]] = []
        manifest: list[dict[str, Any]] = []
        try:
            for record in records:
                raw_artifact_id = f"artifact:jcurve:{content_hash([record.source, record.upstream_event_hash, record.raw_content_hash])[:28]}"
                raw_name = f"{record.announcement_id.replace(':', '_')}.json"
                raw_target = source_dir / raw_name
                raw_target.write_bytes(record.raw_payload)
                artifacts.append(self._artifact(
                    record=record, artifact_id=raw_artifact_id, source_key="market_intel_announcement",
                    content_hash_value=record.raw_content_hash, byte_count=len(record.raw_payload),
                    local_dataset_id=f"market_intel.raw_event:{record.upstream_raw_event_id}",
                    source_url=record.source_url,
                ))
                attachment_artifact_id = None
                attachment_relative = None
                if record.attachment_path and record.attachment_content_hash:
                    attachment_artifact_id = f"artifact:jcurve-attachment:{content_hash([record.attachment_url, record.attachment_content_hash])[:28]}"
                    suffix = Path(record.attachment_path).suffix.lower() or ".bin"
                    attachment_relative = f"source/{attachment_artifact_id.replace(':', '_')}{suffix}"
                    attachment_target = output_dir / attachment_relative
                    shutil.copyfile(record.attachment_path, attachment_target)
                    artifacts.append(self._artifact(
                        record=record, artifact_id=attachment_artifact_id,
                        source_key="market_intel_announcement_attachment",
                        content_hash_value=record.attachment_content_hash,
                        byte_count=attachment_target.stat().st_size,
                        local_dataset_id=f"market_intel.filing_document:{record.upstream_raw_event_id}",
                        parent_artifact_id=raw_artifact_id, source_url=record.attachment_url,
                    ))
                row = asdict(record) | {
                    "source_artifact_id": raw_artifact_id,
                    "attachment_artifact_id": attachment_artifact_id,
                }
                announcements.append(row)
                manifest.append({
                    "announcement_id": record.announcement_id,
                    "raw_artifact_id": raw_artifact_id,
                    "raw_path": f"source/{raw_name}",
                    "attachment_artifact_id": attachment_artifact_id,
                    "attachment_path": attachment_relative,
                    "attachment_validation_status": record.attachment_validation_status,
                    "identity_candidates": list(record.identity_candidates),
                })
            degraded = None
            unresolved = sum(row["identity_status"] != "RESOLVED" for row in announcements)
            invalid_attachments = sum(
                row["attachment_validation_status"] not in {"VALID", "ABSENT"}
                for row in announcements
            )
            degraded_reasons = []
            if unresolved:
                degraded_reasons.append(f"IDENTITY_UNRESOLVED:{unresolved}")
            if invalid_attachments:
                degraded_reasons.append(f"ATTACHMENT_INVALID:{invalid_attachments}")
            if run_type == "BOOTSTRAP" and not (coverage and coverage["coverage_proven"]):
                degraded_reasons.append("HISTORICAL_SOURCE_COVERAGE_UNPROVEN")
            if coverage and not coverage["coverage_proven"]:
                degraded_reasons.append("UPSTREAM_FILTER_COVERAGE_INCOMPLETE")
            cohort_summary = None
            if resolved_cohort:
                observed_company_ids = {str(row["company_id"]) for row in announcements if row.get("company_id")}
                missing_members = [
                    row for row in resolved_cohort.members
                    if str(row["company_id"]) not in observed_company_ids
                ]
                cohort_summary = {
                    "cohort_version": resolved_cohort.version,
                    "cohort_policy_hash": resolved_cohort.policy_hash,
                    "required_company_count": len(resolved_cohort.company_ids),
                    "observed_company_count": len(observed_company_ids),
                    "missing_symbols": [row["nse_symbol"] for row in missing_members],
                    "members": list(resolved_cohort.members),
                }
                (output_dir / "cohort.json").write_text(
                    json.dumps(cohort_summary, indent=2, default=str) + "\n", encoding="utf-8",
                )
            degraded = ";".join(degraded_reasons) or None
            payload = {
                "run_id": run_id, "run_type": run_type, "parent_run_id": None,
                "as_of_date": as_of_date, "policy_version": IMPORT_POLICY_VERSION,
                "policy_hash": IMPORT_POLICY_HASH, "snapshot_hash": snapshot_hash,
                "announcement_count": len(announcements), "claim_count": 0,
                "episode_count": 0, "degraded_reason": degraded, "started_at": started_at,
                "artifacts": artifacts, "announcements": announcements,
            }
            (output_dir / "manifest.json").write_text(
                json.dumps({
                    "run_id": run_id, "snapshot_hash": snapshot_hash,
                    "cohort": cohort_summary, "announcements": manifest,
                    "upstream_filter_policy": upstream_filter_policy,
                    "upstream_coverage": coverage,
                }, indent=2, default=str) + "\n",
                encoding="utf-8",
            )
            self._write_announcement_csv(output_dir / "announcements.csv", announcements)
            self.store.persist_import(payload)
            return {
                "run_id": run_id, "status": "COMPLETED", "reused": False,
                "output_dir": str(output_dir), "announcement_count": len(announcements),
                "resolved_count": len(announcements) - unresolved,
                "unresolved_count": unresolved, "degraded_reason": degraded,
                "cohort_version": resolved_cohort.version if resolved_cohort else None,
                "cohort_company_count": len(resolved_cohort.company_ids) if resolved_cohort else None,
                "observed_company_count": cohort_summary["observed_company_count"] if cohort_summary else None,
                "upstream_filter_policy": upstream_filter_policy,
                "upstream_coverage_proven": coverage["coverage_proven"] if coverage else None,
            }
        except Exception:
            shutil.rmtree(output_dir)
            raise

    def incremental_from(self, *, as_of_date: date, overlap_days: int) -> date:
        latest = self.store.latest_import_publication()
        if latest is None:
            return as_of_date - timedelta(days=overlap_days)
        return max(date(1970, 1, 1), latest.date() - timedelta(days=overlap_days))

    @staticmethod
    def _artifact(
        *, record: AnnouncementRecord, artifact_id: str, source_key: str,
        content_hash_value: str, byte_count: int, local_dataset_id: str,
        parent_artifact_id: str | None = None, source_url: str | None = None,
    ) -> dict[str, Any]:
        return {
            "artifact_id": artifact_id,
            "ingestion_run_id": f"ingest:{record.announcement_id}:{artifact_id}",
            "source_key": source_key, "provider": record.source.upper(),
            "source_url": source_url, "local_dataset_id": local_dataset_id,
            "effective_date": record.event_at.date() if record.event_at else None,
            "published_at": record.published_at, "retrieved_at": record.retrieved_at,
            "content_hash": content_hash_value, "byte_count": byte_count, "row_count": 1,
            "parser_version": IMPORT_POLICY_VERSION, "schema_version": "jcurve-announcement-v1",
            "validation_status": "VALID", "parent_artifact_id": parent_artifact_id,
            "metadata": {
                "upstream_event_hash": record.upstream_event_hash,
                "attachment_validation_status": record.attachment_validation_status,
            },
        }

    @staticmethod
    def _write_announcement_csv(path: Path, rows: list[dict[str, Any]]) -> None:
        fields = [
            "announcement_id", "upstream_raw_event_id", "source", "category", "title",
            "published_at", "event_at", "identity_status", "company_id", "security_id",
            "listing_id", "source_artifact_id", "attachment_artifact_id",
        ]
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows({key: row.get(key) for key in fields} for row in rows)


class JCurveEvaluationService:
    def __init__(
        self, *, store_path: Path, output_root: Path, project_root: Path,
        router: OpenRouterModelRouter,
    ):
        self.store = JCurveStore(store_path)
        self.output_root = Path(output_root)
        self.project_root = Path(project_root)
        self.router = router
        self.stage_policy = CapexStagePolicy.from_root(project_root)

    def run(
        self, *, parent_run_id: str, as_of_date: date,
        materiality_inputs: dict[str, dict[str, float | None]],
        human_verifications: list[dict[str, str]] | None = None,
        company_limit: int = 25,
    ) -> dict[str, Any]:
        if not self.store.completed_run(parent_run_id):
            raise ValueError("parent must be a completed J-curve import run")
        parent_dir = self.output_root / parent_run_id
        manifest_path = parent_dir / "manifest.json"
        if not manifest_path.is_file():
            raise ValueError("parent immutable manifest is missing")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        file_map = {row["announcement_id"]: row for row in manifest["announcements"]}
        announcements = self.store.load_announcements(parent_run_id)
        company_ids = sorted({
            row["company_id"] for row in announcements
            if row["identity_status"] == "RESOLVED" and row.get("company_id")
        })[:company_limit]
        company_set = set(company_ids)
        announcements = [row for row in announcements if row.get("company_id") in company_set]
        started_at = datetime.now(UTC)
        calls: list[dict[str, Any]] = []
        claims: list[dict[str, Any]] = []
        reviews: list[dict[str, Any]] = []
        episodes: list[dict[str, Any]] = []
        links: list[dict[str, Any]] = []
        observations: list[dict[str, Any]] = []
        episode_groups: dict[tuple[str, str], dict[str, Any]] = {}
        for row in human_verifications or []:
            if set(row) != {"claim_id", "reviewer", "decision", "note"}:
                raise ValueError("each human verification must contain claim_id, reviewer, decision, and note")
            if row["decision"] != "ACCEPT" or not row["reviewer"].strip() or not row["note"].strip():
                raise ValueError("human verifications require ACCEPT, a reviewer, and a non-empty note")
        human_by_claim = {
            row["claim_id"]: row for row in (human_verifications or [])
            if row.get("decision") == "ACCEPT" and row.get("claim_id") and row.get("reviewer")
        }
        snapshot_hash = content_hash({
            "parent": manifest["snapshot_hash"],
            "as_of_date": as_of_date,
            "router_policy": self.router.policy.policy_hash,
            "stage_policy": self.stage_policy.policy_hash,
            "company_limit": company_limit,
            "selected_company_ids": company_ids,
            "materiality_inputs": materiality_inputs,
            "human_verifications": human_verifications or [],
        })
        run_id = f"jcurve-evaluate-{as_of_date}-{snapshot_hash[:16]}"
        output_dir = self.output_root / run_id
        if self.store.completed_run(run_id) and output_dir.is_dir():
            return {
                "run_id": run_id,
                "status": "COMPLETED",
                "reused": True,
                "output_dir": str(output_dir),
            }
        used_human_claims: set[str] = set()
        for announcement in announcements:
            if announcement["identity_status"] != "RESOLVED":
                continue
            packet, route = self._packet(parent_dir, file_map[announcement["announcement_id"]], announcement)
            if not packet.pages:
                continue
            if len(calls) + 2 > int(self.router.policy.raw["max_requests_per_run"]):
                raise ValueError("J-curve model request budget exhausted")
            reported_cost = sum(float(row.get("cost_usd") or 0) for row in calls)
            if reported_cost >= float(self.router.policy.raw["halt_new_requests_at_reported_cost_usd"]):
                raise ValueError("J-curve reported cost budget exhausted")
            try:
                extraction = self.router.extract(packet, route=route)
            except ModelRouterError:
                if route != ModelRoute.VISION:
                    raise
                route = ModelRoute.VISION_FALLBACK
                extraction = self.router.extract(packet, route=route)
            verification = self.router.verify(packet, claims=extraction.payload["claims"], extraction_route=route)
            calls.extend([
                self._call_row(extraction.call, announcement["announcement_id"]),
                self._call_row(verification.call, announcement["announcement_id"]),
            ])
            announcement_claims, announcement_reviews = self._claims(
                announcement=announcement, packet=packet, extracted=extraction.payload["claims"],
                review_payload=verification.payload["reviews"], verification_call=verification.call,
            )
            for claim in announcement_claims:
                human = human_by_claim.get(claim["claim_id"])
                if human and claim["status"] != "AGENT_REJECTED":
                    used_human_claims.add(claim["claim_id"])
                    claim["status"] = "HUMAN_VERIFIED"
                    announcement_reviews.append({
                        "review_id": f"jcurve-review:{content_hash([claim['claim_id'], human])[:28]}",
                        "claim_id": claim["claim_id"], "request_id": f"human:{content_hash(human)[:24]}",
                        "decision": "ACCEPT", "reviewer_model": f"human:{human['reviewer']}",
                        "provider": None, "prompt_version": "jcurve-human-review-v1",
                        "independent_context": True, "normalized_claim_hash": content_hash(claim["claim_json"]),
                        "issue_codes": [], "review_json": human,
                    })
            project_key, project_name, project_location = self.stage_policy.project_key(
                announcement_claims, announcement["source_artifact_id"]
            )
            group_key = (announcement["company_id"], project_key)
            group = episode_groups.setdefault(group_key, {
                "claims": [], "announcements": [], "project_name": project_name,
                "project_location": project_location,
            })
            group["claims"].extend(announcement_claims)
            group["announcements"].append(announcement)
            if group["project_name"] is None:
                group["project_name"] = project_name
            if group["project_location"] is None:
                group["project_location"] = project_location
            claims.extend(announcement_claims)
            reviews.extend(announcement_reviews)

        unknown_human_claims = set(human_by_claim) - used_human_claims
        if unknown_human_claims:
            raise ValueError(f"human verification claim IDs were not eligible: {sorted(unknown_human_claims)}")

        for (company_id, project_key), group in sorted(episode_groups.items()):
            group_claims = group["claims"]
            inputs_raw = materiality_inputs.get(company_id, {})
            if not inputs_raw:
                inputs_raw = next(
                    (materiality_inputs[row["announcement_id"]] for row in group["announcements"]
                     if row["announcement_id"] in materiality_inputs),
                    {},
                )
            inputs = MaterialityInputs(**inputs_raw)
            preliminary = self.stage_policy.evaluate(group_claims, inputs=inputs)
            if preliminary.materiality_passed:
                for claim in group_claims:
                    if claim["claim_type"] in {"CAPEX_AMOUNT", "CAPEX_ANNOUNCED", "CAPACITY_INCREMENT", "EXPECTED_INCREMENTAL_REVENUE"} and claim["status"] == "AGENT_VERIFIED":
                        claim["status"] = "HUMAN_REVIEW_REQUIRED"
            decision = self.stage_policy.evaluate(group_claims, inputs=inputs)
            episode_id = f"jcurve-episode:{content_hash([company_id, 'CAPEX', project_key])[:28]}"
            first_announcement = min(group["announcements"], key=lambda row: row["published_at"])
            episode = {
                "episode_id": episode_id, "company_id": company_id, "archetype": "CAPEX",
                "project_key": project_key, "project_name": group["project_name"],
                "project_location": group["project_location"],
                "opened_at": first_announcement["published_at"], "closed_at": None,
                "status": "ACTIVE" if decision.stage == "J1" else "REVIEW_REQUIRED",
                "first_trigger_artifact_id": first_announcement["source_artifact_id"],
                "clustering_policy_version": "jcurve-capex-clustering-v1",
            }
            episodes.append(episode)
            links.extend({"episode_id": episode_id, "claim_id": claim["claim_id"], "relation": "EVIDENCE"} for claim in group_claims)
            observation_id = f"jcurve-stage:{content_hash([episode_id, as_of_date, self.stage_policy.policy_version, decision])[:28]}"
            observations.append({
                "observation_id": observation_id, "episode_id": episode_id, "as_of_date": as_of_date,
                "stage": decision.stage, "score": decision.score, "confidence": decision.confidence,
                "materiality_passed": decision.materiality_passed,
                "materiality_metrics": decision.materiality_metrics,
                "reason_codes": list(decision.reason_codes), "evidence_ids": list(decision.evidence_ids),
                "policy_version": self.stage_policy.policy_version, "policy_hash": self.stage_policy.policy_hash,
            })
        output_dir.mkdir(parents=True, exist_ok=False)
        payload = {
            "run_id": run_id, "run_type": "EVALUATION", "parent_run_id": parent_run_id,
            "as_of_date": as_of_date, "policy_version": self.router.policy.raw["policy_version"],
            "policy_hash": content_hash({"router": self.router.policy.policy_hash, "stage": self.stage_policy.policy_hash}),
            "snapshot_hash": snapshot_hash, "announcement_count": len(announcements),
            "claim_count": len(claims), "episode_count": len({row['episode_id'] for row in episodes}),
            "degraded_reason": None, "started_at": started_at, "calls": calls,
            "claims": claims, "reviews": reviews, "episodes": self._dedupe(episodes, "episode_id"),
            "episode_evidence": self._dedupe(links, ("episode_id", "claim_id")), "observations": observations,
        }
        try:
            (output_dir / "result.json").write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
            self.store.persist_evaluation(payload)
        except Exception:
            shutil.rmtree(output_dir)
            raise
        return {
            "run_id": run_id, "status": "COMPLETED", "reused": False, "output_dir": str(output_dir),
            "claim_count": len(claims), "episode_count": payload["episode_count"],
            "j1_count": sum(row["stage"] == "J1" for row in observations),
            "human_review_count": sum(claim["status"] == "HUMAN_REVIEW_REQUIRED" for claim in claims),
        }

    def _packet(self, parent_dir: Path, manifest: dict[str, Any], announcement: dict[str, Any]):
        attachment = manifest.get("attachment_path")
        source_hash = announcement["source_content_hash"]
        if attachment:
            selected = select_relevant_pages(parent_dir / attachment)
            if announcement.get("attachment_artifact_id"):
                source_hash = announcement["attachment_content_hash"]
            pages, route = selected.pages, selected.route
        else:
            text = "\n".join(filter(None, [announcement["title"], announcement.get("description")]))
            pages, route = (EvidencePage(page=1, text=text),), ModelRoute.TEXT
        packet = EvidencePacket(
            announcement_id=announcement["announcement_id"], source_artifact_id=announcement.get("attachment_artifact_id") or announcement["source_artifact_id"],
            source_content_hash=source_hash,
            company_id=announcement["company_id"], security_id=announcement["security_id"], isin=announcement["isin"],
            published_at=announcement["published_at"], title=announcement["title"], pages=tuple(pages),
        )
        return packet, route

    @staticmethod
    def _claims(*, announcement, packet, extracted, review_payload, verification_call):
        review_by_index = {row["claim_index"]: row for row in review_payload}
        claims, reviews = [], []
        for index, raw in enumerate(extracted):
            review = review_by_index[index]
            excerpt_matches = review.get("supported_excerpt") == raw.get("exact_excerpt")
            page_matches = review.get("supported_page") == raw.get("page")
            status = "AGENT_VERIFIED" if review["decision"] == "ACCEPT" and excerpt_matches and page_matches else (
                "HUMAN_REVIEW_REQUIRED" if review["decision"] == "AMBIGUOUS" else "AGENT_REJECTED"
            )
            if raw.get("claim_type") == "DEMAND_PATH_REPORTED" and status == "AGENT_VERIFIED":
                status = "HUMAN_REVIEW_REQUIRED"
            claim_id = f"jcurve-claim:{content_hash([announcement['announcement_id'], index, raw])[:28]}"
            claim = dict(raw) | {
                "claim_id": claim_id, "announcement_id": announcement["announcement_id"],
                "company_id": announcement["company_id"], "security_id": announcement["security_id"],
                "schema_version": "jcurve-claim-v1", "source_artifact_id": packet.source_artifact_id,
                "source_content_hash": packet.source_content_hash, "published_at": announcement["published_at"],
                "status": status, "claim_json": raw,
            }
            claims.append(claim)
            normalized_hash = content_hash(raw if excerpt_matches and page_matches else review)
            reviews.append({
                "review_id": f"jcurve-review:{content_hash([claim_id, verification_call.request_id])[:28]}",
                "claim_id": claim_id, "request_id": verification_call.request_id,
                "decision": review["decision"], "reviewer_model": verification_call.model_id,
                "provider": verification_call.provider, "prompt_version": verification_call.prompt_version,
                "independent_context": True, "normalized_claim_hash": normalized_hash,
                "issue_codes": list(review.get("issue_codes") or []) + ([] if excerpt_matches else ["EXCERPT_MISMATCH"]) + ([] if page_matches else ["PAGE_MISMATCH"]),
                "review_json": review,
            })
        return claims, reviews

    @staticmethod
    def _call_row(call, announcement_id: str) -> dict[str, Any]:
        return asdict(call) | {"announcement_id": announcement_id}

    @staticmethod
    def _dedupe(rows: list[dict[str, Any]], key):
        keys = (key,) if isinstance(key, str) else key
        out = {}
        for row in rows:
            out[tuple(row[name] for name in keys)] = row
        return list(out.values())
