from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pytest

from ai_trading_system.domains.ingest import new_symbol_onboarding
from ai_trading_system.domains.ingest.new_symbol_onboarding import (
    BSEClassification,
    BSEProfileClient,
    SymbolTarget,
    apply_bse_classifications,
    apply_discovered_master_candidates,
    discover_bse_missing_symbols,
    load_symbol_targets,
    run_new_symbol_onboarding,
)


def _create_runtime(tmp_path: Path) -> tuple[Path, Path]:
    data_root = tmp_path / "data"
    data_root.mkdir()
    master_path = data_root / "masterdata.db"
    master = sqlite3.connect(master_path)
    try:
        master.execute(
            """
            CREATE TABLE symbols (
                symbol_id TEXT PRIMARY KEY, security_id TEXT, symbol_name TEXT,
                exchange TEXT, instrument_type TEXT, isin TEXT, sector TEXT,
                industry TEXT, nse_symbol TEXT, bse_symbol TEXT, last_updated TEXT
            )
            """
        )
        master.execute(
            """
            CREATE TABLE sector_mapping (
                industry TEXT PRIMARY KEY, system_sector TEXT NOT NULL, last_updated TEXT
            )
            """
        )
        master.execute(
            "INSERT INTO symbols VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "ONLYBSE", "500999", "Only BSE Ltd", "BSE", "EQUITY", "INEONLY01010",
                "Unknown", "Unknown", None, "ONLYBSE", "2026-08-01",
            ),
        )
        master.commit()
    finally:
        master.close()

    ohlcv_path = data_root / "ohlcv.duckdb"
    catalog = duckdb.connect(str(ohlcv_path))
    try:
        catalog.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR, exchange VARCHAR, timestamp TIMESTAMP,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT
            )
            """
        )
    finally:
        catalog.close()
    return master_path, ohlcv_path


def _classification() -> BSEClassification:
    return BSEClassification(
        symbol_id="ONLYBSE",
        exchange="BSE",
        security_id="500999",
        isin="INEONLY01010",
        sector="Industrials",
        industry="Industrial Products",
        industry_new="Capital Goods",
        industry_group="Industrial Manufacturing",
        industry_subgroup="Industrial Products",
        source="BSE_COMHEADERNEW",
        source_url="https://api.bseindia.com/profile/500999",
        raw_payload_hash="a" * 64,
    )


def _discovered_candidate() -> dict[str, object]:
    classification = BSEClassification(
        symbol_id="ONLYNEW",
        exchange="BSE",
        security_id="500111",
        isin="INENEW01010",
        sector="Industrials",
        industry="Industrial Products",
        industry_new="Capital Goods",
        industry_group="Industrial Manufacturing",
        industry_subgroup="Industrial Products",
        source="BSE_COMHEADERNEW",
        source_url="https://api.bseindia.com/profile/500111",
        raw_payload_hash="b" * 64,
    )
    return {
        "symbol_id": "ONLYNEW",
        "security_id": "500111",
        "symbol_name": "Only New Ltd",
        "exchange": "BSE",
        "instrument_type": "EQUITY",
        "isin": "INENEW01010",
        "bse_symbol": "ONLYNEW",
        "market_cap_cr": 1500.5,
        "classification": classification.__dict__,
    }


def test_load_symbol_targets_requires_bse_master_row(tmp_path: Path) -> None:
    master_path, _ = _create_runtime(tmp_path)

    targets = load_symbol_targets(master_path, ["onlybse"])

    assert targets == [
        SymbolTarget(
            symbol_id="ONLYBSE",
            security_id="500999",
            exchange="BSE",
            isin="INEONLY01010",
            symbol_name="Only BSE Ltd",
            sector="Unknown",
            industry="Unknown",
        )
    ]


def test_bse_profile_client_checks_identity_and_maps_taxonomy() -> None:
    class Response:
        url = "https://api.bseindia.com/profile/500999"

        @staticmethod
        def raise_for_status() -> None:
            return None

        @staticmethod
        def json() -> dict[str, str]:
            return {
                "SecurityCode": "500999",
                "SecurityId": "ONLYBSE",
                "ISIN": "INEONLY01010",
                "Sector": "Industrials",
                "IndustryNew": "Capital Goods",
                "IGroup": "Industrial Manufacturing",
                "ISubGroup": "Industrial Products",
            }

    class Session:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

        @staticmethod
        def get(*args, **kwargs):  # noqa: ANN002, ANN003
            assert kwargs["params"]["scripcode"] == "500999"
            return Response()

    target = SymbolTarget(
        "ONLYBSE", "500999", "BSE", "INEONLY01010", "Only BSE Ltd", "Unknown", "Unknown"
    )

    result = BSEProfileClient(session=Session()).fetch(target)  # type: ignore[arg-type]

    assert result.sector == "Industrials"
    assert result.industry == "Industrial Products"
    assert len(result.raw_payload_hash) == 64


def test_apply_bse_classification_preserves_lineage(tmp_path: Path) -> None:
    master_path, _ = _create_runtime(tmp_path)

    updated = apply_bse_classifications(master_path, [_classification()])

    conn = sqlite3.connect(master_path)
    try:
        symbol = conn.execute(
            "SELECT sector, industry FROM symbols WHERE symbol_id = ?",
            ["ONLYBSE"],
        ).fetchone()
        lineage = conn.execute(
            "SELECT source, industry_new, industry_group, industry_subgroup FROM symbol_classification"
        ).fetchone()
        mapping = conn.execute(
            "SELECT system_sector FROM sector_mapping WHERE industry = ?",
            ["Industrials"],
        ).fetchone()
    finally:
        conn.close()
    assert updated == 1
    assert symbol == ("Industrials", "Industrial Products")
    assert lineage == (
        "BSE_COMHEADERNEW", "Capital Goods", "Industrial Manufacturing", "Industrial Products"
    )
    assert mapping == ("Industrials",)


def test_apply_bse_classification_supports_legacy_nonunique_sector_mapping(tmp_path: Path) -> None:
    master_path, _ = _create_runtime(tmp_path)
    conn = sqlite3.connect(master_path)
    try:
        conn.execute("DROP TABLE sector_mapping")
        conn.execute(
            """
            CREATE TABLE sector_mapping (
                id INTEGER PRIMARY KEY, industry TEXT NOT NULL,
                system_sector TEXT NOT NULL, last_updated TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    updated = apply_bse_classifications(master_path, [_classification()])

    conn = sqlite3.connect(master_path)
    try:
        mapping = conn.execute(
            "SELECT industry, system_sector FROM sector_mapping"
        ).fetchall()
    finally:
        conn.close()
    assert updated == 1
    assert mapping == [("Industrials", "Industrials")]


def test_preview_is_read_only_and_lists_complete_workflow(tmp_path: Path) -> None:
    master_path, _ = _create_runtime(tmp_path)
    before = master_path.stat().st_mtime_ns

    report = run_new_symbol_onboarding(
        project_root=tmp_path,
        symbols=["ONLYBSE"],
        from_date="2020-01-01",
        to_date="2026-08-10",
        apply=False,
    )

    assert report["status"] == "preview"
    assert "bse_official_classification" in report["planned_steps"]
    assert "targeted_technical_features" in report["planned_steps"]
    assert master_path.stat().st_mtime_ns == before
    assert not (tmp_path / "data" / "backups").exists()


def test_preview_reports_writer_lock_without_mutating(monkeypatch, tmp_path: Path) -> None:
    master_path, _ = _create_runtime(tmp_path)

    def locked(*args, **kwargs):  # noqa: ANN002, ANN003, ARG001
        raise duckdb.IOException("conflicting writer lock")

    monkeypatch.setattr(new_symbol_onboarding, "inspect_onboarding_coverage", locked)

    report = run_new_symbol_onboarding(
        project_root=tmp_path,
        symbols=["ONLYBSE"],
        from_date="2020-01-01",
        to_date="2026-08-10",
        apply=False,
    )

    assert report["status"] == "preview"
    assert "conflicting writer lock" in report["before_coverage_error"]
    assert report["before"] == {}
    assert master_path.exists()
    assert not (tmp_path / "data" / "backups").exists()


def test_discover_missing_resolves_candidates_and_surfaces_identity_gaps(tmp_path: Path) -> None:
    master_path, _ = _create_runtime(tmp_path)

    class ActiveClient:
        session = object()

        @staticmethod
        def fetch():
            return (
                [
                    {
                        "SCRIP_CD": "500111",
                        "Scrip_Name": "Only New Ltd",
                        "Status": "Active",
                        "GROUP": "B",
                        "FACE_VALUE": "10",
                        "ISIN_NUMBER": "INENEW01010",
                        "scrip_id": "ONLYNEW",
                        "Mktcap": "1500.5",
                        "NSURL": "https://www.bseindia.com/onlynew",
                    },
                    {
                        "SCRIP_CD": "500999",
                        "Scrip_Name": "Only BSE Ltd",
                        "Status": "Active",
                        "GROUP": "B",
                        "FACE_VALUE": "10",
                        "ISIN_NUMBER": "INEONLY01010",
                        "scrip_id": "ONLYBSE",
                        "Mktcap": "1200",
                    },
                    {
                        "SCRIP_CD": "500222",
                        "Scrip_Name": "Collision Ltd",
                        "Status": "Active",
                        "GROUP": "X",
                        "FACE_VALUE": "10",
                        "ISIN_NUMBER": "INEONLY01010",
                        "scrip_id": "COLLIDE",
                        "Mktcap": "1300",
                    },
                    {
                        "SCRIP_CD": "590111",
                        "Scrip_Name": "Example ETF",
                        "Status": "Active",
                        "GROUP": "B",
                        "FACE_VALUE": "10",
                        "ISIN_NUMBER": "INFETF01010",
                        "scrip_id": "ETFTEST",
                        "Mktcap": "2000",
                    },
                ],
                "https://api.bseindia.com/active",
            )

    class ProfileClient:
        @staticmethod
        def fetch(target: SymbolTarget) -> BSEClassification:
            assert target.symbol_id == "ONLYNEW"
            return BSEClassification(
                symbol_id=target.symbol_id,
                exchange="BSE",
                security_id=target.security_id,
                isin=target.isin,
                sector="Industrials",
                industry="Industrial Products",
                industry_new="Capital Goods",
                industry_group="Industrial Manufacturing",
                industry_subgroup="Industrial Products",
                source="BSE_COMHEADERNEW",
                source_url="https://api.bseindia.com/profile/500111",
                raw_payload_hash="b" * 64,
            )

    result = discover_bse_missing_symbols(
        master_db_path=master_path,
        symbols=["ONLYNEW", "ONLYBSE", "COLLIDE", "ETFTEST", "NOTFOUND"],
        active_client=ActiveClient(),  # type: ignore[arg-type]
        profile_client=ProfileClient(),  # type: ignore[arg-type]
    )

    assert result["candidate_count"] == 1
    assert result["candidates"][0]["symbol_id"] == "ONLYNEW"
    assert result["candidates"][0]["classification"]["sector"] == "Industrials"
    assert result["already_mastered_count"] == 1
    assert result["conflict_count"] == 1
    assert result["invalid"] == {"ETFTEST": "not_a_company_equity_isin=INFETF01010"}
    assert result["not_found"] == ["NOTFOUND"]


def test_discovery_preview_is_read_only_before_master_insertion(tmp_path: Path) -> None:
    master_path, _ = _create_runtime(tmp_path)
    before = master_path.stat().st_mtime_ns

    def discovery_runner(**kwargs):  # noqa: ANN003
        assert kwargs["symbols"] == ["ONLYNEW"]
        return {
            "candidate_count": 1,
            "conflict_count": 0,
            "not_found_count": 0,
            "invalid_count": 0,
            "classification_failure_count": 0,
            "candidates": [{"symbol_id": "ONLYNEW"}],
        }

    report = run_new_symbol_onboarding(
        project_root=tmp_path,
        symbols=["ONLYNEW"],
        from_date="2020-01-01",
        to_date="2026-08-10",
        apply=False,
        discover_missing=True,
        discovery_runner=discovery_runner,
    )

    assert report["status"] == "discovery_preview"
    assert report["writes_performed"] is False
    assert report["discovery"]["candidate_count"] == 1
    assert master_path.stat().st_mtime_ns == before
    assert not (tmp_path / "data" / "backups").exists()


def test_apply_discovered_master_candidates_inserts_without_replacement(tmp_path: Path) -> None:
    master_path, _ = _create_runtime(tmp_path)

    inserted = apply_discovered_master_candidates(master_path, [_discovered_candidate()])

    conn = sqlite3.connect(master_path)
    try:
        row = conn.execute(
            """
            SELECT symbol_id, security_id, exchange, isin, sector, industry, nse_symbol, bse_symbol
            FROM symbols WHERE symbol_id = ?
            """,
            ["ONLYNEW"],
        ).fetchone()
    finally:
        conn.close()
    assert inserted == 1
    assert row == (
        "ONLYNEW", "500111", "BSE", "INENEW01010", "Industrials",
        "Industrial Products", None, "ONLYNEW",
    )

    with pytest.raises(RuntimeError, match="Master identity collision"):
        apply_discovered_master_candidates(master_path, [_discovered_candidate()])


def test_promote_discovered_apply_checkpoints_before_insert_and_runs_workflow(tmp_path: Path) -> None:
    master_path, ohlcv_path = _create_runtime(tmp_path)
    candidate = _discovered_candidate()

    def discovery_runner(**kwargs):  # noqa: ANN003
        assert kwargs["symbols"] == ["ONLYNEW"]
        return {
            "requested_count": 1,
            "candidate_count": 1,
            "already_mastered_count": 0,
            "conflict_count": 0,
            "not_found_count": 0,
            "invalid_count": 0,
            "classification_failure_count": 0,
            "candidates": [candidate],
            "already_mastered": [],
            "conflicts": [],
            "not_found": [],
            "invalid": {},
            "classification_failures": {},
        }

    def classification_fetcher(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("promotion must reuse the identity-checked discovery classification")

    def history_runner(**kwargs):  # noqa: ANN003
        conn = duckdb.connect(str(ohlcv_path))
        try:
            conn.execute(
                "INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ["ONLYNEW", "BSE", "2026-08-10", 10.0, 11.0, 9.0, 10.5, 1000],
            )
        finally:
            conn.close()
        return {"candidate_rows": 1, "rows_written": 1, "backup": {"path": "ohlcv-copy"}}

    def technical_runner(**kwargs):  # noqa: ANN003
        assert kwargs["symbols"] == ["ONLYNEW"]
        return {"rows_written_total": 9, "symbols_targeted": 1}

    class Phase1Result:
        @staticmethod
        def to_dict() -> dict[str, object]:
            return {"symbol_rows": 1, "breadth_rows": 1, "latest_date": "2026-08-10"}

    report = run_new_symbol_onboarding(
        project_root=tmp_path,
        symbols=["ONLYNEW"],
        from_date="2020-01-01",
        to_date="2026-08-10",
        apply=True,
        discover_missing=True,
        promote_discovered=True,
        include_fundamentals=False,
        discovery_runner=discovery_runner,
        classification_fetcher=classification_fetcher,
        history_runner=history_runner,
        technical_runner=technical_runner,
        phase1_runner=lambda **kwargs: Phase1Result(),  # noqa: ARG005
    )

    assert report["steps"]["master_promotion"]["inserted"] == 1
    assert report["steps"]["classification"]["updated"] == 1
    assert report["after"]["ONLYNEW"]["ohlcv_rows"] == 1
    backup_path = Path(report["steps"]["checkpoint"]["backup_dir"]) / "masterdata.db"
    backup = sqlite3.connect(f"file:{backup_path}?mode=ro", uri=True)
    try:
        assert backup.execute(
            "SELECT COUNT(*) FROM symbols WHERE symbol_id = ?", ["ONLYNEW"]
        ).fetchone()[0] == 0
    finally:
        backup.close()
    assert master_path.exists()


def test_promote_discovered_requires_explicit_apply_and_clean_discovery(tmp_path: Path) -> None:
    _create_runtime(tmp_path)

    with pytest.raises(ValueError, match="requires --apply"):
        run_new_symbol_onboarding(
            project_root=tmp_path,
            symbols=["ONLYNEW"],
            from_date="2020-01-01",
            to_date="2026-08-10",
            discover_missing=True,
            promote_discovered=True,
        )


def test_apply_orchestrates_checkpoint_history_features_and_verification(tmp_path: Path) -> None:
    master_path, ohlcv_path = _create_runtime(tmp_path)

    def classification_fetcher(targets, *, client=None):  # noqa: ANN001, ARG001
        assert [target.symbol_id for target in targets] == ["ONLYBSE"]
        return [_classification()], {}

    def history_runner(**kwargs):  # noqa: ANN003
        assert kwargs["recompute_features"] is False
        conn = duckdb.connect(str(ohlcv_path))
        try:
            conn.execute(
                "INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ["ONLYBSE", "BSE", "2026-08-10", 10.0, 11.0, 9.0, 10.5, 1000],
            )
        finally:
            conn.close()
        return {"candidate_rows": 1, "rows_written": 1, "backup": {"path": "ohlcv-copy"}}

    def technical_runner(**kwargs):  # noqa: ANN003
        assert kwargs["symbols"] == ["ONLYBSE"]
        assert kwargs["exchanges"] == ["BSE"]
        return {"rows_written_total": 9, "symbols_targeted": 1}

    class Phase1Result:
        @staticmethod
        def to_dict() -> dict[str, object]:
            return {"symbol_rows": 1, "breadth_rows": 1, "latest_date": "2026-08-10"}

    def phase1_runner(**kwargs):  # noqa: ANN003
        assert kwargs["exchange"] == "BSE"
        return Phase1Result()

    def fundamentals_runner(**kwargs):  # noqa: ANN003
        assert kwargs["symbols"] == ["ONLYBSE"]
        return {"total": 1, "succeeded": 1, "failed": 0}

    report = run_new_symbol_onboarding(
        project_root=tmp_path,
        symbols=["ONLYBSE"],
        from_date="2020-01-01",
        to_date="2026-08-10",
        apply=True,
        classification_fetcher=classification_fetcher,
        history_runner=history_runner,
        technical_runner=technical_runner,
        phase1_runner=phase1_runner,
        fundamentals_runner=fundamentals_runner,
    )

    assert report["status"] == "completed_with_gaps"
    assert report["steps"]["history"]["status"] == "completed"
    assert report["steps"]["technical_features"]["status"] == "completed"
    assert report["after"]["ONLYBSE"]["ohlcv_rows"] == 1
    assert report["after"]["ONLYBSE"]["classification_complete"] is True
    assert Path(report["steps"]["checkpoint"]["backup_dir"]).exists()
    assert Path(report["report_path"]).exists()
    assert master_path.exists()
