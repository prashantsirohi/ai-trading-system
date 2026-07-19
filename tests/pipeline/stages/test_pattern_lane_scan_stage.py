from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.pattern_lane_scan import PatternLaneScanStage

_ARTIFACT_TYPES = {
    "pattern_lane_scan", "pattern_lane_summary", "pattern_lane_runtime",
    "pattern_lane_source_diagnostics", "pattern_lane_parity_report",
    "pattern_lane_manifest", "pattern_lane_shadow_report",
}


def _seed_ohlcv(path: Path) -> str:
    dates = pd.bdate_range("2025-01-01", periods=210)
    rows = []
    for symbol_index, symbol in enumerate(("AAA", "BBB", "CCC")):
        for index, session in enumerate(dates):
            close = 100.0 + symbol_index * 5 + index * 0.2
            rows.append((symbol, "NSE", session.to_pydatetime(), close - 0.5, close + 1, close - 1, close, 1_000_000.0))
    conn = duckdb.connect(str(path))
    conn.execute(
        "CREATE TABLE _catalog(symbol_id VARCHAR, exchange VARCHAR, timestamp TIMESTAMP, "
        "open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE)"
    )
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()
    return dates[-1].date().isoformat()


def _context(tmp_path, monkeypatch, *, mode: str) -> StageContext:
    data_root = tmp_path / "runtime"
    data_root.mkdir(exist_ok=True)
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    ohlcv = data_root / "ohlcv.duckdb"
    run_date = _seed_ohlcv(ohlcv)
    # control_plane.duckdb must exist for the governed source read (no tables → empty).
    duckdb.connect(str(data_root / "control_plane.duckdb")).close()
    params = {"data_domain": "operational", "pattern_lane_scan_mode": mode, "exchange": "NSE"}
    return StageContext(Path.cwd(), ohlcv, "r1a-test", run_date, "pattern_lane_scan", 1, registry=None, params=params)


def test_mode_off_skips_without_artifacts_or_writes(tmp_path, monkeypatch) -> None:
    context = _context(tmp_path, monkeypatch, mode="off")

    result = PatternLaneScanStage().run(context)

    assert result.metadata == {"status": "skipped", "mode": "off"}
    assert result.artifacts == []


def test_mode_shadow_writes_all_seven_artifacts_without_side_effects(tmp_path, monkeypatch) -> None:
    context = _context(tmp_path, monkeypatch, mode="shadow")

    result = PatternLaneScanStage().run(context)

    written = {artifact.artifact_type for artifact in result.artifacts}
    assert written == _ARTIFACT_TYPES
    for artifact in result.artifacts:
        assert Path(artifact.uri).exists()
    assert result.metadata["operational_side_effects"] is False
    assert result.metadata["status"] == "completed"

    manifest_path = next(Path(a.uri) for a in result.artifacts if a.artifact_type == "pattern_lane_manifest")
    manifest = json.loads(manifest_path.read_text())
    assert manifest["operational_side_effects"] is False
    assert manifest["weekly_stage_policy_version"] == "weekly-stage-v2"
    assert set(manifest["dataset_hashes"]) >= {
        "pattern_lane_scan.csv", "pattern_lane_summary.json", "pattern_lane_shadow_report.html",
    }

    report_html = next(Path(a.uri).read_text() for a in result.artifacts if a.artifact_type == "pattern_lane_shadow_report")
    assert "SHADOW — NON-ACTIONABLE" in report_html

    # No decision-consumer stores were created by the shadow stage.
    data_root = tmp_path / "runtime"
    assert not (data_root / "execution.duckdb").exists()
    # control_plane has no opportunity/candidate tables (registry was None).
    conn = duckdb.connect(str(data_root / "control_plane.duckdb"), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    finally:
        conn.close()
    assert not any(name.startswith(("opportunity", "candidate")) for name in tables)


def test_mode_shadow_records_legacy_parity_when_pattern_scan_present(tmp_path, monkeypatch) -> None:
    context = _context(tmp_path, monkeypatch, mode="shadow")
    legacy_path = tmp_path / "pattern_scan.csv"
    pd.DataFrame({"symbol_id": ["AAA", "ZZZ"]}).to_csv(legacy_path, index=False)
    legacy = StageArtifact.from_file("pattern_scan", legacy_path, row_count=2, attempt_number=1)
    context.artifacts = {"rank": {"pattern_scan": legacy}}

    result = PatternLaneScanStage().run(context)

    parity_path = next(Path(a.uri) for a in result.artifacts if a.artifact_type == "pattern_lane_parity_report")
    parity = json.loads(parity_path.read_text())
    assert parity["legacy_pattern_scan"]["content_hash"] == legacy.content_hash
    assert parity["legacy_pattern_scan"]["present"] is True
    assert parity["operational_side_effects"] is False
