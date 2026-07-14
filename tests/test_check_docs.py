from __future__ import annotations

from pathlib import Path

from scripts import check_docs


EXPECTED_LOGICAL_STAGES = [
    "ingest",
    "features",
    "rank",
    "weekly_stage",
    "scan_router",
    "investigator",
    "opportunities",
    "fundamentals",
    "candidates",
    "candidate_tracker",
    "events",
    "execute",
    "insight",
    "narrative",
    "publish",
    "perf_tracker",
]


def test_extract_orchestrator_stages_expands_features_once() -> None:
    logical, feature_substages, persisted = check_docs.extract_orchestrator_stages()

    assert logical == EXPECTED_LOGICAL_STAGES
    assert feature_substages == [
        "features_technical",
        "features_sector_rs",
        "features_valuation",
        "features_stock_valuation_bands",
        "features_sector_earnings",
        "features_phase1",
        "features_snapshot",
    ]
    assert persisted[1:8] == feature_substages


def test_system_guide_matches_code_and_has_valid_links() -> None:
    assert check_docs.check_system_guide() == []
    assert check_docs.check_links(check_docs.SYSTEM_GUIDE) == []


def test_missing_stage_document_is_reported(tmp_path: Path) -> None:
    stages = tmp_path / "stages"
    stages.mkdir()
    (stages / "ingest.md").write_text("# ingest\n")

    assert check_docs.check_stage_documents(["ingest", "rank"], tmp_path) == [
        "docs/stages/rank.md: missing detailed document for logical stage 'rank'"
    ]


def test_design_change_requires_guide_and_detailed_contract() -> None:
    errors = check_docs.check_change_impact(
        {"src/ai_trading_system/pipeline/orchestrator.py"}
    )

    assert errors == [
        "design change requires docs/SYSTEM_GUIDE.md to change in the same commit",
        "design change requires docs/architecture/operational_data_flow.md to change in the same commit",
    ]


def test_design_change_passes_when_required_docs_change() -> None:
    changed = {
        "src/ai_trading_system/platform/db/paths.py",
        "docs/SYSTEM_GUIDE.md",
        "docs/architecture/storage_and_lineage.md",
    }

    assert check_docs.check_change_impact(changed) == []


def test_agents_and_current_docs_route_to_system_guide() -> None:
    assert check_docs.check_canonical_routing() == []
