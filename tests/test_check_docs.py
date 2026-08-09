from __future__ import annotations

from pathlib import Path

from scripts import check_docs


EXPECTED_LOGICAL_STAGES = [
    "ingest",
    "features",
    "rank",
    "weekly_stage",
    "pattern_lane_scan",
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


def test_repo_satisfies_coverage_checks() -> None:
    assert check_docs.check_index_completeness(list(check_docs.iter_current_docs())) == []
    assert check_docs.check_console_scripts_documented() == []
    assert check_docs.check_env_vars_documented() == []


def test_unlinked_doc_is_reported(tmp_path: Path, monkeypatch) -> None:
    docs = tmp_path / "docs"
    (docs / "stages").mkdir(parents=True)
    index = docs / "INDEX.md"
    index.write_text("# Index\n- [ingest](stages/ingest.md)\n")
    (docs / "stages" / "ingest.md").write_text("# ingest\n")
    orphan = docs / "stages" / "rank.md"
    orphan.write_text("# rank\n")
    monkeypatch.setattr(check_docs, "REPO", tmp_path)
    monkeypatch.setattr(check_docs, "DOCS", docs)
    monkeypatch.setattr(check_docs, "INDEX", index)

    errors = check_docs.check_index_completeness([index, docs / "stages" / "ingest.md", orphan])

    assert errors == [
        "docs/INDEX.md: does not link stages/rank.md (the index claims to be a complete map)"
    ]


def test_env_vars_found_through_local_helper_wrappers(tmp_path: Path) -> None:
    """Settings read via a module-local wrapper must still be discovered."""
    (tmp_path / "settings.py").write_text(
        "import os\n"
        "def _bool(name, default):\n"
        "    value = os.getenv(name)\n"
        "    return default if value is None else value == '1'\n"
        "DIRECT = os.getenv('DIRECT_SETTING')\n"
        "SUBSCRIPT = os.environ['SUBSCRIPT_SETTING']\n"
        "DOTGET = os.environ.get('DOTGET_SETTING')\n"
        "WRAPPED = _bool('WRAPPED_SETTING', True)\n"
        "IGNORED = _bool(some_variable, True)\n"
    )

    assert check_docs.env_vars_in_source(tmp_path) == {
        "DIRECT_SETTING",
        "SUBSCRIPT_SETTING",
        "DOTGET_SETTING",
        "WRAPPED_SETTING",
    }


def test_env_var_scan_ignores_non_env_string_constants(tmp_path: Path) -> None:
    (tmp_path / "noise.py").write_text(
        "import os\n"
        "STAGE = 'INGEST'\n"          # single word, no underscore
        "LABEL = 'Some Title Case'\n"
        "REAL = os.getenv('A_REAL_VAR')\n"
    )

    assert check_docs.env_vars_in_source(tmp_path) == {"A_REAL_VAR"}


def test_stale_verification_stamp_is_advisory_only(tmp_path: Path, monkeypatch) -> None:
    doc = tmp_path / "doc.md"
    doc.write_text("# Doc\n\n- **Last verified:** 2026-01-01\n")
    monkeypatch.setattr(check_docs, "REPO", tmp_path)
    monkeypatch.setattr(check_docs, "last_commit_dates", lambda paths: {doc: "2026-06-01"})

    assert check_docs.check_verification_stamps([doc]) == [
        "doc.md: last verified 2026-01-01 but last changed 2026-06-01"
    ]


def test_malformed_verification_stamp_is_reported(tmp_path: Path, monkeypatch) -> None:
    doc = tmp_path / "doc.md"
    doc.write_text("# Doc\n\n- **Last verified:** 2026-07-19 (during the R1a run).\n")
    monkeypatch.setattr(check_docs, "REPO", tmp_path)
    monkeypatch.setattr(check_docs, "last_commit_dates", lambda paths: {})

    assert check_docs.check_verification_stamps([doc]) == [
        "doc.md: 'Last verified' is not a bare YYYY-MM-DD date"
    ]
