from datetime import date, timedelta
from types import SimpleNamespace
import json
import sqlite3

import pandas as pd
import pytest

from ai_trading_system.domains.ingest import universe_refresh as refresh
from ai_trading_system.domains.ingest.new_symbol_onboarding import BSEClassification


def sources():
    return {
        "nse": [
            {
                "SYMBOL": "NEWCO",
                "ISIN NUMBER": "INE123A01012",
                "SERIES": "EQ",
                "DATE OF LISTING": "01-Jan-2024",
            }
        ],
        "bse": [
            {
                "SCRIP_CD": "500123",
                "ISIN_NUMBER": "INE123A01012",
                "scrip_id": "NEWCO",
                "Scrip_Name": "New Company",
                "Status": "Active",
                "GROUP": "A",
            }
        ],
        "dhan": [
            {
                "SEM_EXM_EXCH_ID": "NSE",
                "SEM_TRADING_SYMBOL": "NEWCO",
                "SEM_SEGMENT": "E",
                "SEM_SERIES": "EQ",
                "SEM_EXCH_INSTRUMENT_TYPE": "ES",
                "SEM_SMST_SECURITY_ID": "123",
            }
        ],
    }


def rows():
    return [{"nse_symbol": "NEWCO", "bse_code": "500123", "isin": "", "mcap": 700.0}]


def test_cadence():
    assert refresh.due(None, date(2026, 1, 20), "monthly")
    assert not refresh.due("2026-01-01", date(2026, 1, 31), "monthly")
    assert refresh.due("2026-12-31", date(2027, 1, 1), "monthly")
    assert not refresh.due("2026-01-01", date(2026, 1, 28), "28-days")
    assert refresh.due("2026-01-01", date(2026, 1, 29), "28-days")
    with pytest.raises(ValueError):
        refresh.due("2026-01-02", date(2026, 1, 1), "monthly")


@pytest.mark.parametrize("suffix", ["csv", "xlsx"])
def test_export_threshold_and_identity(tmp_path, suffix):
    frame = pd.DataFrame(
        {
            "NSE Code": ["NEWCO", "SMALL"],
            "BSE Code": [500123, 500124],
            "Market Capitalization": [700, 500],
        }
    )
    path = tmp_path / f"export.{suffix}"
    if suffix == "csv":
        frame.to_csv(path, index=False)
    else:
        frame.to_excel(path, index=False)
    assert refresh.read_export(path) == rows()


@pytest.mark.parametrize(
    "frame",
    [
        {"Name": ["No identifier"], "Market Capitalization": [700]},
        {"NSE Code": ["NEWCO"], "Market Capitalization": [float("nan")]},
        {"NSE Code": ["NEWCO", "NEWCO"], "Market Capitalization": [700, 800]},
    ],
)
def test_invalid_export_rejected(tmp_path, frame):
    path = tmp_path / "export.csv"
    pd.DataFrame(frame).to_csv(path, index=False)
    with pytest.raises(ValueError):
        refresh.read_export(path)


def test_resolution_uses_official_isin_and_dhan_id():
    result = refresh.plan_additions(rows(), [], sources())
    assert not result["blocked"]
    assert result["additions"][0]["security_id"] == "123"
    assert result["additions"][0]["isin"] == "INE123A01012"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s["nse"][0].update(SERIES="SM"),
        lambda s: s["nse"][0].update({"ISIN NUMBER": "INF123A01012"}),
        lambda s: s["dhan"].clear(),
        lambda s: s["bse"][0].update(SCRIP_CD="500999"),
    ],
)
def test_untrusted_identity_blocks(mutate):
    source = sources()
    mutate(source)
    result = refresh.plan_additions(rows(), [], source)
    assert result["blocked"]
    assert not result["additions"]


def test_master_isin_prevents_rename_duplicate():
    master = [
        {
            "symbol_id": "OLDNAME",
            "bse_symbol": "OLDNAME",
            "isin": "INE123A01012",
            "exchange": "NSE",
            "security_id": "123",
        }
    ]
    result = refresh.plan_additions(rows(), master, sources())
    assert result["existing"] == ["OLDNAME"]
    assert not result["additions"]
    master[0]["symbol_id"] = "NEWCO"
    master[0]["isin"] = "INE999A01012"
    assert refresh.plan_additions(rows(), master, sources())["blocked"]


def runtime(tmp_path, monkeypatch):
    root = tmp_path / "runtime"
    root.mkdir()
    path = root / "masterdata.db"
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE symbols (symbol_id TEXT PRIMARY KEY, security_id TEXT, symbol_name TEXT, exchange TEXT, instrument_type TEXT, isin TEXT, lot_size INTEGER, sector TEXT, industry TEXT, nse_symbol TEXT, bse_symbol TEXT, mcap REAL, last_updated TEXT)"
        )
    paths = SimpleNamespace(
        root_dir=root,
        master_db_path=path,
        stage_store_dir=root / "stage_store",
        feature_store_dir=root / "feature_store",
        fundamentals_dir=root / "fundamentals",
        ohlcv_db_path=root / "ohlcv.duckdb",
    )
    monkeypatch.setattr(refresh, "get_domain_paths", lambda **kw: paths)
    monkeypatch.setattr(refresh, "require_data_root_available", lambda p: None)
    export = tmp_path / "screen.csv"
    pd.DataFrame(
        {"NSE Code": ["NEWCO"], "BSE Code": [500123], "Market Capitalization": [700]}
    ).to_csv(export, index=False)
    profile = SimpleNamespace(
        fetch=lambda target: BSEClassification(
            "NEWCO",
            "BSE",
            "500123",
            "INE123A01012",
            "Industrials",
            "Machinery",
            "",
            "",
            "",
            "BSE",
            "https://example.test",
            "hash",
        )
    )
    return paths, dict(
        project_root=tmp_path,
        as_of=date.today(),
        export=export,
        source_loader=sources,
        profile_client=profile,
    )


def test_preview_does_not_mutate(tmp_path, monkeypatch):
    paths, kwargs = runtime(tmp_path, monkeypatch)
    before = paths.master_db_path.read_bytes()
    assert refresh.run_refresh(**kwargs)["status"] == "preview"
    assert paths.master_db_path.read_bytes() == before
    assert not paths.stage_store_dir.exists()
    assert not (paths.root_dir / "backups").exists()


def test_retry_after_master_insert_and_success_cadence(tmp_path, monkeypatch):
    paths, kwargs = runtime(tmp_path, monkeypatch)

    def fail(*args, **kwargs):
        # Master and source backup must exist before any backfill.
        with sqlite3.connect(paths.master_db_path) as conn:
            assert conn.execute("SELECT COUNT(*) FROM symbols").fetchone()[0] == 1
        assert list((paths.root_dir / "backups").glob("*/masterdata.db"))
        raise RuntimeError("interrupted history")

    result = refresh.run_refresh(**kwargs, apply=True, nse_runner=fail)
    assert result["status"] == "failed"
    state_path = paths.stage_store_dir / "universe_refresh/state.json"
    state = json.loads(state_path.read_text())
    assert state["last_success"] is None and "NEWCO" in state["pending"]
    calls = []

    def succeed(*args, **kwargs):
        calls.append(args[2]["symbol_id"])
        return {"status": "completed"}

    progress = []
    result = refresh.run_refresh(
        **kwargs, apply=True, nse_runner=succeed, progress_callback=progress.append
    )
    assert result["status"] == "completed"
    assert calls == ["NEWCO"]
    assert any("[1/1] NEWCO (NSE)" in event["detail"] for event in progress)
    assert progress[-1]["status"] == "ok"
    assert progress[-1]["completed"] == progress[-1]["total"]
    fractions = [event["completed"] / event["total"] for event in progress]
    assert fractions == sorted(fractions)
    state = json.loads(state_path.read_text())
    assert not state["pending"] and state["last_success"] == date.today().isoformat()
    not_due = refresh.run_refresh(**kwargs, apply=True)
    assert not_due["status"] == "not_due"
    assert not_due["updated_symbols"] == ["NEWCO"]


def test_historical_apply_rejected_before_acquisition(tmp_path):
    with pytest.raises(ValueError, match="today"):
        refresh.run_refresh(
            project_root=tmp_path, as_of=date.today() - timedelta(days=1), apply=True
        )


def test_concurrent_refresh_rejected(tmp_path):
    with refresh.refresh_lock(tmp_path):
        with pytest.raises(RuntimeError, match="Another"):
            with refresh.refresh_lock(tmp_path):
                pass


def test_quarantine_retries_before_next_cadence(tmp_path, monkeypatch):
    paths, kwargs = runtime(tmp_path, monkeypatch)
    invalid = sources()
    invalid["nse"][0]["SERIES"] = "SM"
    kwargs["source_loader"] = lambda: invalid
    result = refresh.run_refresh(**kwargs, apply=True)
    assert result["status"] == "completed_with_gaps"
    assert result["blocked"]
    state = json.loads(
        (paths.stage_store_dir / "universe_refresh/state.json").read_text()
    )
    assert state["last_success"] == date.today().isoformat()
    assert state["discovery_quarantine"]["NEWCO"]["attempts"] == 1
    assert not (paths.root_dir / "backups").exists()
    kwargs["source_loader"] = sources
    result = refresh.run_refresh(
        **kwargs, apply=True, nse_runner=lambda *a, **kw: {"status": "completed"}
    )
    assert result["status"] == "completed"
    assert result["updated_symbols"] == ["NEWCO"]
    state = json.loads(
        (paths.stage_store_dir / "universe_refresh/state.json").read_text()
    )
    assert not state["discovery_quarantine"]


def test_bse_only_and_sme_resolution():
    row = rows()[0]
    row["nse_symbol"] = ""
    source = sources()
    source["nse"] = []
    result = refresh.plan_additions([row], [], source)
    assert not result["blocked"]
    assert result["additions"][0]["exchange"] == "BSE"
    assert result["additions"][0]["security_id"] == "500123"
    source = sources()
    source["nse"] = []
    source["bse"][0]["GROUP"] = "M"
    assert refresh.plan_additions([row], [], source)["blocked"]


def test_bse_export_identifier_prefers_official_nse_listing():
    row = rows()[0]
    row["nse_symbol"] = ""
    result = refresh.plan_additions([row], [], sources())
    assert result["additions"][0]["exchange"] == "NSE"
    assert result["additions"][0]["security_id"] == "123"


def test_nse_backfill_orders_dependencies_and_bounds_ipo_history(tmp_path, monkeypatch):
    from ai_trading_system.domains.ingest import (
        repair,
        corporate_actions,
        delivery,
        new_symbol_onboarding,
    )
    from ai_trading_system.domains.features import compute_features_batch, phase1

    calls = []

    def history(**kwargs):
        assert kwargs["from_date"] == "2024-01-01"
        assert kwargs["symbols"] == ["NEWCO"]
        assert kwargs["apply_changes"] and not kwargs["recompute_features"]
        calls.append("history")
        return {"repair_status": "completed", "symbols": [{"api_rows": 1}]}

    monkeypatch.setattr(repair, "repair_window", history)
    monkeypatch.setattr(
        corporate_actions, "fetch_nse_corporate_actions", lambda **kwargs: []
    )
    monkeypatch.setattr(
        corporate_actions,
        "upsert_corporate_actions",
        lambda *args: calls.append("actions"),
    )
    monkeypatch.setattr(
        corporate_actions,
        "recompute_adjusted_prices",
        lambda *args, **kwargs: calls.append("adjust"),
    )

    def collect(*args, **kwargs):
        calls.append("delivery")
        return 1

    monkeypatch.setattr(
        delivery,
        "DeliveryCollector",
        lambda **kwargs: SimpleNamespace(
            fetch_range=collect,
            compute_delivery_features=lambda: calls.append("delivery_features"),
        ),
    )

    def technical(**kwargs):
        assert kwargs["full_rebuild"] and kwargs["symbols"] == ["NEWCO"]
        calls.append("technical")
        return {"rows_written_total": 1}

    monkeypatch.setattr(
        compute_features_batch, "run_batch_feature_computation", technical
    )
    monkeypatch.setattr(
        phase1, "refresh_phase1_features", lambda **kwargs: calls.append("phase1")
    )

    def fundamental(**kwargs):
        calls.append(kwargs["statement_basis"])
        return {"failed": 0}

    monkeypatch.setattr(new_symbol_onboarding, "_run_fundamentals", fundamental)
    monkeypatch.setattr(
        new_symbol_onboarding,
        "inspect_onboarding_coverage",
        lambda *args, **kwargs: {
            "NEWCO": {
                "fundamental_rows": 1,
                "phase1_rows": 1,
                "technical_feature_file_count": 1,
            }
        },
    )
    item = refresh.plan_additions(rows(), [], sources())["additions"][0]
    item.update(sector="Industrials", industry="Machinery")
    paths = SimpleNamespace(
        ohlcv_db_path=tmp_path / "ohlcv.duckdb",
        master_db_path=tmp_path / "master.db",
        raw_dir=tmp_path / "raw",
        feature_store_dir=tmp_path / "features",
    )
    result = refresh.onboard_nse(
        tmp_path, paths, item, "2020-01-01", "2026-01-01", str(tmp_path)
    )
    assert result["status"] == "completed"
    assert calls == [
        "history",
        "actions",
        "adjust",
        "delivery",
        "delivery_features",
        "technical",
        "phase1",
        "standalone",
        "consolidated",
    ]


@pytest.mark.parametrize("suffix", ["csv", "xlsx"])
def test_isin_only_export_rows_are_retained(tmp_path, suffix):
    frame = pd.DataFrame(
        {
            "Name": ["A R C I", "Listed"],
            "NSE Code": [None, "NEWCO"],
            "BSE Code": [None, 500123],
            "ISIN Code": ["INE148G01016", "INE123A01012"],
            "Market Capitalization": [4516.07, 700],
        }
    )
    path = tmp_path / f"export.{suffix}"
    if suffix == "csv":
        frame.to_csv(path, index=False)
    else:
        frame.to_excel(path, index=False)
    parsed = refresh.read_export(path)
    assert len(parsed) == 2
    plan = refresh.plan_additions(parsed, [], sources())
    assert len(plan["additions"]) == 1
    assert plan["excluded"] == [
        {
            "identity": "INE148G01016",
            "reason": "no_active_exchange_listing",
            "mcap": 4516.07,
        }
    ]
    assert not plan["blocked"]


def test_isin_only_resolves_exact_active_listing():
    row = {"nse_symbol": "", "bse_code": "", "isin": "INE123A01012", "mcap": 700}
    plan = refresh.plan_additions([row], [], sources())
    assert plan["additions"][0]["symbol_id"] == "NEWCO"
    assert plan["additions"][0]["exchange"] == "NSE"
    assert not plan["excluded"]
    source = sources()
    source["nse"] = []
    plan = refresh.plan_additions([row], [], source)
    assert plan["additions"][0]["exchange"] == "BSE"
    source["bse"].append(dict(source["bse"][0], SCRIP_CD="500999"))
    plan = refresh.plan_additions([row], [], source)
    assert plan["blocked"] and not plan["excluded"] and not plan["additions"]


def test_missing_all_identifiers_reports_row(tmp_path):
    path = tmp_path / "export.csv"
    pd.DataFrame(
        {"Name": ["Unknown"], "ISIN Code": [None], "Market Capitalization": [900]}
    ).to_csv(path, index=False)
    with pytest.raises(ValueError, match="row 2"):
        refresh.read_export(path)


def test_discovery_quarantine_allows_valid_additions_and_retries(tmp_path, monkeypatch):
    paths, kwargs = runtime(tmp_path, monkeypatch)
    pd.DataFrame(
        {"NSE Code": ["NEWCO", "UNRESOLVED"], "Market Capitalization": [700, 800]}
    ).to_csv(kwargs["export"], index=False)
    calls = []

    def onboard(*args, **kw):
        calls.append(args[2]["symbol_id"])
        return {"status": "completed"}

    result = refresh.run_refresh(**kwargs, apply=True, nse_runner=onboard)
    assert result["status"] == "completed_with_gaps"
    assert len(result["additions"]) == 1 and len(result["blocked"]) == 1
    assert calls == ["NEWCO"]
    assert result["updated_symbols"] == ["NEWCO"]
    with sqlite3.connect(paths.master_db_path) as conn:
        assert conn.execute("SELECT symbol_id FROM symbols").fetchall() == [("NEWCO",)]
    first = result["discovery_quarantine"]["UNRESOLVED"]
    assert first["source_row"]["nse_symbol"] == "UNRESOLVED"
    result = refresh.run_refresh(**kwargs, apply=True, nse_runner=onboard)
    assert result["status"] == "completed_with_gaps"
    assert calls == ["NEWCO"]
    queued = result["discovery_quarantine"]["UNRESOLVED"]
    assert queued["attempts"] == 2 and queued["first_seen"] == first["first_seen"]


def test_missing_nse_code_uses_identity_checked_bse_listing():
    source = sources()
    source["nse"] = []
    master = [
        {
            "symbol_id": "NEWCO",
            "isin": "INE123A01012",
            "exchange": "BSE",
            "security_id": "500123",
            "bse_symbol": "NEWCO",
        }
    ]
    row = dict(rows()[0], isin="INE123A01012")
    assert refresh.plan_additions([row], master, source)["existing"] == ["NEWCO"]
    row["isin"] = "INE999A01012"
    assert refresh.plan_additions([row], master, source)["blocked"]


def test_backfill_stops_before_discovery_day(tmp_path, monkeypatch):
    paths, kwargs = runtime(tmp_path, monkeypatch)
    ends = []

    def runner(*args, **kw):
        ends.append(args[4])
        return {"status": "completed"}

    result = refresh.run_refresh(**kwargs, apply=True, nse_runner=runner)
    assert ends == [(date.today() - timedelta(days=1)).isoformat()]
    assert result["history_cutoff"] == ends[0]


def save_negative_list(root, row):
    path = root / "configs/universe_refresh_negative_list.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"version": 1, "entries": [dict(row, reason="reviewed blocker")]})
    )


def test_saved_negative_list_skips_discovery_and_cadence_retry(tmp_path, monkeypatch):
    paths, kwargs = runtime(tmp_path, monkeypatch)
    save_negative_list(tmp_path, rows()[0])
    # No official identity exists; without the list this row would be blocked.
    kwargs["source_loader"] = lambda: {"nse": [], "bse": [], "dhan": []}
    result = refresh.run_refresh(**kwargs, apply=True)
    assert result["negative_list_count"] == 1
    assert result["blocked"] == [] and result["results"] == {}
    assert result["status"] == "completed_with_gaps"
    result = refresh.run_refresh(**kwargs, apply=True)
    assert result["status"] == "not_due"
    assert result["negative_list_count"] == 1
    assert not (paths.root_dir / "backups").exists()


def test_old_quarantine_on_negative_list_does_not_force_retry(tmp_path, monkeypatch):
    paths, kwargs = runtime(tmp_path, monkeypatch)
    save_negative_list(tmp_path, rows()[0])
    state_path = paths.stage_store_dir / "universe_refresh/state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps(
            {
                "pending": {},
                "last_success": date.today().isoformat(),
                "discovery_quarantine": {"NEWCO": {"source_row": rows()[0]}},
            }
        )
    )
    kwargs["source_loader"] = lambda: pytest.fail("Must not acquire sources")
    assert refresh.run_refresh(**kwargs, apply=True)["status"] == "not_due"


def test_negative_list_identity_change_or_removal_allows_reconsideration(
    tmp_path, monkeypatch
):
    _, kwargs = runtime(tmp_path, monkeypatch)
    row = rows()[0]
    save_negative_list(tmp_path, row)
    entries = refresh.load_negative_list(tmp_path)
    assert refresh.negative_match(dict(row, mcap=900), entries)
    assert not refresh.negative_match(dict(row, isin="INE999A01012"), entries)
    (tmp_path / "configs/universe_refresh_negative_list.json").unlink()
    result = refresh.run_refresh(
        **kwargs, apply=True, nse_runner=lambda *a, **kw: {"status": "completed"}
    )
    assert result["updated_symbols"] == ["NEWCO"]


def test_reviewed_negative_list_has_59_entries_and_keeps_spicejet():
    from pathlib import Path

    entries = refresh.load_negative_list(Path(__file__).resolve().parents[1])
    assert len(entries) == 59
    assert all(entry["identity"] != "SPICEJET" for entry in entries)


def test_saved_exclusion_defers_pending_without_deleting_checkpoint(
    tmp_path, monkeypatch
):
    paths, kwargs = runtime(tmp_path, monkeypatch)

    def interrupted(*args, **kw):
        raise RuntimeError("partial onboarding")

    assert (
        refresh.run_refresh(**kwargs, apply=True, nse_runner=interrupted)["status"]
        == "failed"
    )
    save_negative_list(tmp_path, rows()[0])

    def must_not_run(*args, **kw):
        pytest.fail("Excluded pending company must not be retried")

    result = refresh.run_refresh(**kwargs, apply=True, nse_runner=must_not_run)
    assert result["deferred_pending"] == ["NEWCO"]
    state = json.loads(
        (paths.stage_store_dir / "universe_refresh/state.json").read_text()
    )
    assert "NEWCO" in state["pending"]
    assert (
        refresh.run_refresh(**kwargs, apply=True, nse_runner=must_not_run)["status"]
        == "not_due"
    )


@pytest.mark.parametrize("transient", [True, False])
def test_company_failure_quarantines_with_bounded_retry_and_cooldown(
    tmp_path, monkeypatch, transient
):
    _, kwargs = runtime(tmp_path, monkeypatch)
    calls, sleeps = [], []
    monkeypatch.setattr(refresh.time, "sleep", sleeps.append)

    def fail(*args, **kw):
        calls.append(1)
        if transient:
            raise refresh.requests.Timeout("temporary timeout")
        raise refresh.CompanyOnboardingError("Corporate action ISIN conflict")

    result = refresh.run_refresh(**kwargs, apply=True, nse_runner=fail)
    assert result["status"] == "completed_with_gaps"
    assert result["quarantined_symbols"] == ["NEWCO"]
    assert result["results"]["NEWCO"]["status"] == "quarantined"
    assert len(calls) == (3 if transient else 1)
    assert sleeps == ([2, 4] if transient else [])
    assert (
        refresh.run_refresh(**kwargs, apply=True, nse_runner=fail)["status"]
        == "not_due"
    )
    recovered = refresh.run_refresh(
        **kwargs,
        apply=True,
        force=True,
        nse_runner=lambda *a, **kw: {"status": "completed"},
    )
    assert recovered["quarantined_symbols"] == []


def test_transient_retry_recovers_without_quarantine(tmp_path, monkeypatch):
    _, kwargs = runtime(tmp_path, monkeypatch)
    calls = []
    monkeypatch.setattr(refresh.time, "sleep", lambda seconds: None)

    def runner(*args, **kw):
        calls.append(1)
        if len(calls) == 1:
            raise refresh.requests.ConnectionError("connection reset")
        return {"status": "completed"}

    result = refresh.run_refresh(**kwargs, apply=True, nse_runner=runner)
    assert result["status"] == "completed"
    assert len(calls) == 2
    assert result["quarantined_symbols"] == []


def test_quarantined_company_does_not_stop_healthy_addition(tmp_path, monkeypatch):
    from copy import deepcopy

    _, kwargs = runtime(tmp_path, monkeypatch)
    source = sources()
    for name in ("nse", "bse", "dhan"):
        other = deepcopy(source[name][0])
        for key, value in list(other.items()):
            if value == "NEWCO":
                other[key] = "GOODCO"
            elif value == "500123":
                other[key] = "500124"
            elif value == "INE123A01012":
                other[key] = "INE124A01012"
            elif value == "123":
                other[key] = "124"
        source[name].append(other)
    kwargs["source_loader"] = lambda: source
    pd.DataFrame(
        {
            "NSE Code": ["NEWCO", "GOODCO"],
            "BSE Code": [500123, 500124],
            "Market Capitalization": [700, 800],
        }
    ).to_csv(kwargs["export"], index=False)
    called = []

    def runner(*args, **kw):
        symbol = args[2]["symbol_id"]
        called.append(symbol)
        if symbol == "NEWCO":
            raise refresh.CompanyOnboardingError("Delivery backfill returned no rows")
        return {"status": "completed"}

    result = refresh.run_refresh(**kwargs, apply=True, nse_runner=runner)
    assert set(called) == {"NEWCO", "GOODCO"}
    assert result["status"] == "completed_with_gaps"
    assert result["updated_symbols"] == ["GOODCO"]
    assert result["quarantined_symbols"] == ["NEWCO"]
