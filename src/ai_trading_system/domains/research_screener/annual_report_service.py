from __future__ import annotations

import argparse
import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths, require_data_root_available

from .annual_reports import AnnualReportClient
from .providers import OfficialExchangeClient
from .store import ResearchScreenerStore, canonical_json, content_hash


RESEARCH_VERSION = "1.1.0"


class AnnualReportDiscoveryService:
    def __init__(self, *, store_path: Path | None = None, output_root: Path | None = None):
        require_data_root_available()
        root = get_domain_paths(data_domain="operational").root_dir / "research_screener"
        self.store_path = store_path or root / "control_plane.duckdb"
        self.output_root = output_root or root / "research_runs"

    def run(self, *, parent_run_id: str, as_of_date: date, workers: int = 4,
            batch_size: int = 25) -> dict:
        store = ResearchScreenerStore(self.store_path)
        parent_dir = self.output_root.parent / "runs" / parent_run_id
        status_path = parent_dir / "filing_company_status.csv"
        security_path = parent_dir / "P0_security_master.parquet"
        if not store.completed_run(parent_run_id) or not status_path.is_file() or not security_path.is_file():
            raise ValueError("parent must be a completed filing-discovery run with an immutable output pack")
        status = pd.read_csv(status_path)
        cohort = status[status["fundamental_disposition"] == "BOUNDARY_REVIEW"].copy()
        if cohort.empty:
            raise ValueError("parent has no filing-grade BOUNDARY_REVIEW cohort")
        security = pd.read_parquet(security_path).set_index("fixture_isin")
        members = []
        for row in cohort.sort_values(["fixture_isin", "symbol"]).itertuples(index=False):
            master = security.loc[str(row.fixture_isin)]
            members.append({
                "symbol": str(row.symbol), "company": str(row.company), "isin": str(row.fixture_isin),
                "company_id": str(master.company_id), "security_id": str(master.security_id),
                "company_type": str(row.company_type),
                "listings": json.loads(master.listings) if isinstance(master.listings, str) else [],
            })
        parent_hash = hashlib.sha256(status_path.read_bytes() + security_path.read_bytes()).hexdigest()
        checkpoint_root = self.output_root.parent / "checkpoints" / "annual_reports" / parent_run_id / RESEARCH_VERSION
        checkpoint_root.mkdir(parents=True, exist_ok=True)
        predecessor_root = self.output_root.parent / "checkpoints" / "annual_reports" / parent_run_id / "1.0.2"
        results: dict[int, dict] = {}
        artifacts: dict[int, list[dict]] = {}
        thread_state = threading.local()

        def acquire(index: int, member: dict) -> tuple[int, dict, list[dict]]:
            checkpoint = checkpoint_root / f"{member['isin']}.json"
            if checkpoint.is_file():
                cached = json.loads(checkpoint.read_text(encoding="utf-8"))
                if cached.get("parent_hash") == parent_hash and cached.get("as_of_date") == as_of_date.isoformat():
                    return index, cached["document"], self._restore_artifacts(cached["artifacts"], checkpoint_root)
            predecessor = predecessor_root / f"{member['isin']}.json"
            if predecessor.is_file():
                cached = json.loads(predecessor.read_text(encoding="utf-8"))
                if (cached.get("parent_hash") == parent_hash
                        and cached.get("as_of_date") == as_of_date.isoformat()
                        and cached.get("document", {}).get("state") == "PRESENT"):
                    return index, cached["document"], self._restore_artifacts(
                        cached["artifacts"], predecessor_root,
                    )
            if not hasattr(thread_state, "client"):
                exchange = OfficialExchangeClient(min_interval=1.4 if workers <= 4 else 0.7 * workers)
                thread_state.client = AnnualReportClient(exchange)
            document, member_artifacts = thread_state.client.discover(member, as_of_date)
            raw_dir = checkpoint_root / member["isin"]
            raw_dir.mkdir(exist_ok=True)
            frozen = []
            for position, artifact in enumerate(member_artifacts):
                raw = artifact.pop("_raw", None)
                name = None
                if raw is not None:
                    name = f"{position:02d}_{artifact['content_hash']}.bin"
                    (raw_dir / name).write_bytes(raw)
                frozen.append(artifact | {"checkpoint_raw": f"{member['isin']}/{name}" if name else None})
            payload = {
                "parent_hash": parent_hash, "as_of_date": as_of_date.isoformat(),
                "member": member, "document": document, "artifacts": frozen,
            }
            temporary = checkpoint.with_suffix(".json.tmp")
            temporary.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
            temporary.replace(checkpoint)
            return index, document, self._restore_artifacts(frozen, checkpoint_root)

        started_at = datetime.now(UTC)
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="annual-report") as pool:
            futures = {
                pool.submit(acquire, index, member): (index, member)
                for index, member in enumerate(members)
            }
            completed = 0
            for future in as_completed(futures):
                expected_index, expected_member = futures[future]
                try:
                    index, document, member_artifacts = future.result()
                except Exception as first_error:
                    # Retry once outside the worker that failed. One malformed or
                    # unusually large filing must become an explicit company-level
                    # result, never stop cohort accounting while other workers drain.
                    try:
                        index, document, member_artifacts = acquire(expected_index, expected_member)
                    except Exception as retry_error:
                        index = expected_index
                        document = AnnualReportClient._unavailable(expected_member, [
                            f"ACQUISITION_EXCEPTION:{type(first_error).__name__}:{first_error}",
                            f"RETRY_EXCEPTION:{type(retry_error).__name__}:{retry_error}",
                        ])
                        member_artifacts = []
                results[index] = document
                artifacts[index] = member_artifacts
                completed += 1
                if completed % batch_size == 0 or completed == len(members):
                    progress = {
                        "parent_run_id": parent_run_id, "as_of_date": as_of_date.isoformat(),
                        "cohort_size": len(members), "processed": completed,
                        "documents_present": sum(row["state"] == "PRESENT" for row in results.values()),
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                    (checkpoint_root / "progress.json.tmp").write_text(json.dumps(progress, indent=2) + "\n")
                    (checkpoint_root / "progress.json.tmp").replace(checkpoint_root / "progress.json")

        documents = []
        evidence = []
        all_artifacts = []
        for index, member in enumerate(members):
            document = results[index]
            all_artifacts.extend(artifacts[index])
            document_id = f"research-document:{content_hash([parent_run_id, RESEARCH_VERSION, member['isin'], document.get('source_artifact_id')])[:28]}"
            documents.append(member | document | {"document_id": document_id})
            for ordinal, observation in enumerate(document["evidence"]):
                evidence.append(member | observation | {
                    "document_id": document_id,
                    "evidence_id": f"research-evidence:{content_hash([document_id, ordinal, observation])[:28]}",
                    "source_artifact_id": document.get("source_artifact_id"),
                })
        snapshot_hash = content_hash({
            "parent_run_id": parent_run_id, "parent_hash": parent_hash,
            "as_of_date": as_of_date, "version": RESEARCH_VERSION,
            "documents": [{k: v for k, v in row.items() if k != "evidence"} for row in documents],
        })
        run_id = f"research-annual_reports-{as_of_date}-{snapshot_hash[:16]}"
        output_dir = self.output_root / run_id
        if output_dir.exists() and store.completed_research_run(run_id):
            return {"run_id": run_id, "status": "COMPLETED", "reused": True, "output_dir": str(output_dir)}
        output_dir.mkdir(parents=True, exist_ok=False)
        self._write_pack(output_dir, run_id, parent_run_id, as_of_date, snapshot_hash, documents, evidence, all_artifacts)
        payload = {
            "run_id": run_id, "parent_run_id": parent_run_id, "as_of_date": as_of_date,
            "version": RESEARCH_VERSION, "snapshot_hash": snapshot_hash, "started_at": started_at,
            "documents": documents, "evidence": evidence, "artifacts": all_artifacts,
        }
        store.persist_research_success(payload)
        return {
            "run_id": run_id, "status": "COMPLETED", "reused": False,
            "output_dir": str(output_dir), "cohort_size": len(members),
            "documents_present": sum(row["state"] == "PRESENT" for row in documents),
            "documents_unavailable": sum(row["state"] != "PRESENT" for row in documents),
            "evidence_rows": len(evidence), "snapshot_hash": snapshot_hash,
        }

    @staticmethod
    def _restore_artifacts(rows: list[dict], root: Path) -> list[dict]:
        restored = []
        for row in rows:
            item = dict(row)
            raw_name = item.pop("checkpoint_raw", None)
            if raw_name:
                item["_raw_path"] = str(root / raw_name)
            restored.append(item)
        return restored

    @staticmethod
    def _write_pack(output_dir: Path, run_id: str, parent_run_id: str, as_of_date: date,
                    snapshot_hash: str, documents: list[dict], evidence: list[dict], artifacts: list[dict]) -> None:
        document_rows = [{k: v for k, v in row.items() if k not in {"listings", "evidence", "errors"}} |
                         {"errors": canonical_json(row.get("errors", []))} for row in documents]
        pd.DataFrame(document_rows).to_csv(output_dir / "research_documents.csv", index=False)
        pd.DataFrame(evidence).drop(columns=["listings"], errors="ignore").to_csv(output_dir / "research_evidence.csv", index=False)
        source_dir = output_dir / "source"
        source_dir.mkdir()
        manifest_rows = []
        for artifact in artifacts:
            raw_path = artifact.get("_raw_path")
            frozen = None
            if raw_path:
                source = Path(raw_path)
                name = f"{artifact['artifact_id'].replace(':', '_')}_{artifact['content_hash']}.bin"
                target = source_dir / name
                target.write_bytes(source.read_bytes())
                frozen = f"source/{name}"
            manifest_rows.append({k: v for k, v in artifact.items() if k not in {"_raw", "_raw_path"}} | {"frozen_raw_path": frozen})
        manifest = {
            "run_id": run_id, "parent_run_id": parent_run_id, "as_of_date": as_of_date.isoformat(),
            "snapshot_hash": snapshot_hash, "artifacts": manifest_rows,
        }
        (output_dir / "source_manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n")
        present = sum(row["state"] == "PRESENT" for row in documents)
        topic_counts = pd.DataFrame(evidence).groupby(["topic", "state"]).size().to_dict()
        lines = [
            "# Annual-report research discovery", "", f"- Run: `{run_id}`",
            f"- Parent filing snapshot: `{parent_run_id}`", f"- Cutoff: `{as_of_date}`",
            f"- Filing-grade cohort: {len(documents)}", f"- Annual reports acquired and parsed: {present}",
            f"- Source unavailable: {len(documents) - present}", "",
            "Text matches are discovery anchors only. Every excerpt is attributable to a page and source hash, carries LOW confidence, and requires human review. Missing matches are stored as NOT_DISCLOSED. No statement values, scores, ranks, recommendations, schedules, or broker state were changed.",
            "", "## Topic/state counts", "",
        ]
        lines.extend(f"- {topic} / {state}: {count}" for (topic, state), count in sorted(topic_counts.items()))
        (output_dir / "summary.md").write_text("\n".join(lines) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Discover annual-report evidence for a filing-grade cohort.")
    parser.add_argument("--parent-run-id", required=True)
    parser.add_argument("--as-of-date", required=True, type=date.fromisoformat)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--store-path", type=Path)
    parser.add_argument("--output-root", type=Path)
    args = parser.parse_args(argv)
    if args.workers < 1 or args.workers > 16:
        parser.error("workers must be between 1 and 16")
    result = AnnualReportDiscoveryService(store_path=args.store_path, output_root=args.output_root).run(
        parent_run_id=args.parent_run_id, as_of_date=args.as_of_date,
        workers=args.workers, batch_size=args.batch_size,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
