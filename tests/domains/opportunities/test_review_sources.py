from datetime import datetime, timezone, date
import hashlib

import duckdb
import pytest

from ai_trading_system.domains.opportunities.review_sources import _read_source
from ai_trading_system.interfaces.cli.replay_final_review import replay
from ai_trading_system.pipeline.contracts import StageArtifact

CUTOFF = datetime(2026, 9, 13, tzinfo=timezone.utc)


@pytest.fixture
def source_db(tmp_path):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE pipeline_run(run_id VARCHAR,run_date DATE)")
    conn.execute(
        "CREATE TABLE pipeline_stage_run(run_id VARCHAR,stage_name VARCHAR,attempt_number INTEGER,status VARCHAR,ended_at TIMESTAMP)"
    )
    conn.execute(
        "CREATE TABLE pipeline_artifact(run_id VARCHAR,stage_name VARCHAR,attempt_number INTEGER,artifact_type VARCHAR,uri VARCHAR,content_hash VARCHAR,row_count INTEGER,created_at TIMESTAMP,promoted_at TIMESTAMP,lifecycle_status VARCHAR)"
    )
    conn.execute("INSERT INTO pipeline_run VALUES ('run','2026-09-11')")
    conn.execute(
        "INSERT INTO pipeline_stage_run VALUES ('run','rank',1,'completed','2026-09-11 22:00:00')"
    )
    path = tmp_path / "pipeline_runs/run/rank/attempt_1/ranked_universe.csv"
    path.parent.mkdir(parents=True)
    path.write_text("exchange,symbol_id\nNSE,TEST\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    conn.execute(
        "INSERT INTO pipeline_artifact VALUES (?,?,?,?,?,?,?,?,?,?)",
        [
            "run",
            "rank",
            1,
            "ranked_universe",
            "/original/runtime/pipeline_runs/run/rank/attempt_1/ranked_universe.csv",
            digest,
            1,
            "2026-09-11 21:00:00",
            "2026-09-11 22:01:00",
            "promoted",
        ],
    )
    yield conn, path
    conn.close()


def load(conn, root, selected=None, cutoff=CUTOFF):
    return _read_source(conn, root, "run", "rank", "ranked_universe", cutoff, selected)


def test_registered_sources_relocate_to_copy_without_live_fallback(tmp_path, source_db):
    conn, path = source_db
    result = load(conn, tmp_path)
    assert result.issue is None and len(result.frame) == 1
    selected = StageArtifact.from_file("ranked_universe", path, 1, attempt_number=1)
    assert load(conn, tmp_path, selected).issue is None
    path.unlink()
    assert load(conn, tmp_path).issue.startswith("SOURCE_UNREADABLE")


@pytest.mark.parametrize(
    "statement,issue",
    [
        (
            "UPDATE pipeline_stage_run SET status='failed'",
            "NO_COMPLETED_PROMOTED_SOURCE",
        ),
        (
            "UPDATE pipeline_artifact SET lifecycle_status='written'",
            "NO_COMPLETED_PROMOTED_SOURCE",
        ),
        ("UPDATE pipeline_artifact SET content_hash='bad'", "ARTIFACT_HASH_MISMATCH"),
        ("UPDATE pipeline_artifact SET row_count=2", "ARTIFACT_ROW_COUNT_MISMATCH"),
        (
            "UPDATE pipeline_artifact SET promoted_at='2026-09-14'",
            "SOURCE_AFTER_CUTOFF",
        ),
    ],
)
def test_unusable_sources_never_supply_rows(tmp_path, source_db, statement, issue):
    conn, _ = source_db
    conn.execute(statement)
    result = load(conn, tmp_path)
    assert result.issue == issue and result.frame.empty


def test_symlink_cannot_redirect_copy_reader(tmp_path, source_db):
    conn, path = source_db
    elsewhere = tmp_path / "elsewhere.csv"
    elsewhere.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(elsewhere)
    assert load(conn, tmp_path).issue == "SOURCE_UNREADABLE:ValueError"


def test_replay_rejects_operational_root_before_writing(tmp_path, monkeypatch):
    live = tmp_path / "live"
    live.mkdir()
    monkeypatch.setenv("DATA_ROOT", str(live))
    out = tmp_path / "out"
    with pytest.raises(ValueError, match="isolated copy"):
        replay(
            copied_data_root=live,
            run_id="run",
            session=date(2026, 9, 11),
            decision_at=CUTOFF,
            output_dir=out,
        )
    assert not out.exists()
