import csv
import hashlib
import json
from datetime import date
from pathlib import Path

import duckdb

from ai_trading_system.domains.research_screener.models import RunMode, ScreeningParameters
from ai_trading_system.domains.research_screener.reporting import PACK_FILES
from ai_trading_system.domains.research_screener.service import PersistentScreenerService, RULES


def test_frozen_regression_persists_all_17_and_is_idempotent(tmp_path):
    store = tmp_path / "control.duckdb"
    outputs = tmp_path / "runs"
    service = PersistentScreenerService(store_path=store, output_root=outputs)
    params = ScreeningParameters(as_of_date=date(2026, 8, 8), run_mode=RunMode.REGRESSION_REPLAY)
    first = service.run(params)
    second = service.run(params)
    assert second["idempotent_replay"] is True
    conn = duckdb.connect(str(store), read_only=True)
    try:
        assert conn.execute("select count(*) from universe_member").fetchone()[0] == 17
        assert conn.execute("select count(*) from candidate_decision").fetchone()[0] == 17
        assert conn.execute("select count(*) from security_master").fetchone()[0] == 0
        assert conn.execute("select count(*) from universe_member where identity_status = 'FROZEN_FIXTURE_ONLY'").fetchone()[0] == 17
        assert conn.execute("select count(*) from universe_member where disposition is null").fetchone()[0] == 0
        assert conn.execute("select eligible_count from screening_run").fetchone()[0] == 13
        preserved = {row[0] for row in conn.execute("select fixture_symbol from universe_member where fixture_symbol in ('SJS','KIRLPNU','HAWKINCOOK','E2E')").fetchall()}
        assert preserved == {"SJS", "KIRLPNU", "HAWKINCOOK", "E2E"}
    finally:
        conn.close()
    output = outputs / first["run_id"]
    assert all((output / name).exists() for name in PACK_FILES)


def test_technical_is_separate_and_no_archetype_quota():
    assert RULES["technical_in_fundamental_admission"] is False
    assert RULES["archetype_quota"] is None


def test_new_screen_version_reuses_source_artifacts_without_overwriting_runs(tmp_path):
    service = PersistentScreenerService(store_path=tmp_path / "control.duckdb", output_root=tmp_path / "runs")
    first = service.run(ScreeningParameters(as_of_date=date(2026, 8, 8), run_mode=RunMode.REGRESSION_REPLAY, screen_version="1.0.0"))
    second = service.run(ScreeningParameters(as_of_date=date(2026, 8, 8), run_mode=RunMode.REGRESSION_REPLAY, screen_version="1.0.1"))
    assert first["run_id"] != second["run_id"]
    conn = duckdb.connect(str(tmp_path / "control.duckdb"), read_only=True)
    try:
        assert conn.execute("select count(*) from screening_run where status='COMPLETED'").fetchone()[0] == 2
        assert conn.execute("select count(*) from source_artifact").fetchone()[0] == 5
        assert conn.execute("select count(*) from ingestion_run").fetchone()[0] == 10
        assert conn.execute("select count(*) from ingestion_artifact").fetchone()[0] == 10
        assert conn.execute(
            """select count(*) from ingestion_artifact ia
               join ingestion_run ir using (ingestion_run_id)
               where ir.screening_run_id = ?""",
            [first["run_id"]],
        ).fetchone()[0] == 5
        assert conn.execute(
            """select count(*) from ingestion_artifact ia
               join ingestion_run ir using (ingestion_run_id)
               where ir.screening_run_id = ?""",
            [second["run_id"]],
        ).fetchone()[0] == 5
        assert conn.execute("select count(*) from source_artifact where ingestion_run_id is null").fetchone()[0] == 0
        assert conn.execute("select count(*) from dataset_snapshot").fetchone()[0] == 2
        assert conn.execute("select count(*) from universe_member").fetchone()[0] == 34
    finally:
        conn.close()


def test_versioned_canary_fixtures_preserve_history_and_correct_hawkins():
    root = Path(__file__).resolve().parents[3]
    manifest = json.loads((root / "configs/research_screener/canary_fixture_versions.json").read_text())
    legacy_raw = (root / manifest["versions"]["1.0.0"]["path"]).read_bytes()
    current_raw = (root / manifest["versions"]["1.1.0"]["path"]).read_bytes()
    legacy = {row["symbol"]: row for row in csv.DictReader(legacy_raw.decode().splitlines())}
    current = {row["symbol"]: row for row in csv.DictReader(current_raw.decode().splitlines())}

    assert legacy["HAWKINCOOK"]["isin"] == "INE979A01025"
    assert legacy["HAWKINCOOK"]["fixture_version"] == "1.0.0"
    assert current["HAWKINCOOK"]["isin"] == "INE979B01015"
    assert current["HAWKINCOOK"]["fixture_version"] == "1.1.0"
    assert hashlib.sha256(legacy_raw).hexdigest() == manifest["versions"]["1.0.0"]["sha256"]
    assert hashlib.sha256(current_raw).hexdigest() == manifest["versions"]["1.1.0"]["sha256"]
    correction = manifest["versions"]["1.1.0"]["corrections"][0]
    assert correction["old_value"] == legacy["HAWKINCOOK"]["isin"]
    assert correction["new_value"] == current["HAWKINCOOK"]["isin"]
    assert len(correction["source_artifacts"]) == 3


def test_current_fixture_changes_only_e2e_business_identity_from_v1_1():
    root = Path(__file__).resolve().parents[3]
    manifest = json.loads((root / "configs/research_screener/canary_fixture_versions.json").read_text())
    prior_raw = (root / manifest["versions"]["1.1.0"]["path"]).read_bytes()
    current_raw = (root / manifest["versions"]["1.2.0"]["path"]).read_bytes()
    prior = {row["symbol"]: row for row in csv.DictReader(prior_raw.decode().splitlines())}
    current = {row["symbol"]: row for row in csv.DictReader(current_raw.decode().splitlines())}
    business_changes = [
        (symbol, field, prior[symbol][field], current[symbol][field])
        for symbol in prior
        for field in prior[symbol]
        if field != "fixture_version" and prior[symbol][field] != current[symbol][field]
    ]

    assert business_changes == [("E2E", "isin", "INE255Z01019", "INE255Z01027")]
    assert {row["fixture_version"] for row in prior.values()} == {"1.1.0"}
    assert {row["fixture_version"] for row in current.values()} == {"1.2.0"}
    assert hashlib.sha256(prior_raw).hexdigest() == manifest["versions"]["1.1.0"]["sha256"]
    assert hashlib.sha256(current_raw).hexdigest() == manifest["versions"]["1.2.0"]["sha256"]
    correction = manifest["versions"]["1.2.0"]["corrections"][0]
    assert correction["effective_date"] == "2026-06-05"
    assert correction["corporate_action_evidence"]["raw_payload_hash"] == "ffcede0741c8edcb51c3996ab5613935f12a6193e734b560a3e1bfc09f470b1b"


def test_e2e_transition_requires_exact_frozen_action_evidence():
    root = Path(__file__).resolve().parents[3]
    manifest = json.loads((root / "configs/research_screener/canary_fixture_versions.json").read_text())
    fixture = next(row for row in csv.DictReader((root / "configs/research_screener/canary_companies.csv").open()) if row["symbol"] == "E2E")
    raw_payload_json = "{\"symbol\":\"E2E\"}"
    raw_payload_hash = hashlib.sha256(raw_payload_json.encode()).hexdigest()
    manifest["versions"]["1.2.0"]["corrections"][0]["corporate_action_evidence"]["raw_payload_hash"] = raw_payload_hash
    action = {
        "stored_isin": "INE255Z01019", "action_type": "split", "ratio": "10->1",
        "ex_date": date(2026, 6, 5), "source": "nse_corporate_actions",
        "source_row_hash": raw_payload_hash,
        "raw_payload_json": raw_payload_json,
    }

    history, error = PersistentScreenerService._identifier_transition(fixture, [action], "1.2.0", manifest)
    assert error is None
    assert [(row["identifier_value"], row["valid_from"], row["valid_to"]) for row in history] == [
        ("INE255Z01019", date(2026, 6, 4), date(2026, 6, 4)),
        ("INE255Z01027", date(2026, 6, 5), None),
    ]

    action["source_row_hash"] = "wrong"
    history, error = PersistentScreenerService._identifier_transition(fixture, [action], "1.2.0", manifest)
    assert history == []
    assert "does not match" in error


def test_split_action_adds_prior_isin_for_filing_validation_only():
    raw_payload_json = '{"symbol":"CAMS","isin":"INEOLD"}'
    action = {
        "stored_isin": "INEOLD", "action_type": "split", "ex_date": date(2025, 12, 5),
        "source": "nse_corporate_actions", "source_row_hash": hashlib.sha256(raw_payload_json.encode()).hexdigest(),
        "raw_payload_json": raw_payload_json, "status": "active",
    }
    history = PersistentScreenerService._filing_identifier_history("INENEW", [action], [])
    assert history == [{
        "identifier_type": "ISIN", "identifier_value": "INEOLD", "exchange": "NSE",
        "valid_from": None, "valid_to": date(2025, 12, 4),
        "source_role": "CORPORATE_ACTION_FILING_ONLY",
    }]


def test_split_action_expands_restrictive_canonical_window_for_filing_validation():
    raw_payload_json = '{"symbol":"E2E","isin":"INEOLD"}'
    action = {
        "stored_isin": "INEOLD", "action_type": "split", "ex_date": date(2026, 6, 5),
        "source": "nse_corporate_actions", "source_row_hash": hashlib.sha256(raw_payload_json.encode()).hexdigest(),
        "raw_payload_json": raw_payload_json, "status": "active",
    }
    canonical = [
        {
            "identifier_type": "ISIN", "identifier_value": "INEOLD", "exchange": None,
            "valid_from": date(2026, 6, 4), "valid_to": date(2026, 6, 4),
            "source_role": "CORPORATE_ACTION",
        },
        {
            "identifier_type": "ISIN", "identifier_value": "INENEW", "exchange": None,
            "valid_from": date(2026, 6, 5), "valid_to": None,
            "source_role": "CURRENT_IDENTITY_MASTER",
        },
    ]

    history = PersistentScreenerService._filing_identifier_history("INENEW", [action], canonical)

    assert history[0] == {
        "identifier_type": "ISIN", "identifier_value": "INEOLD", "exchange": "NSE",
        "valid_from": None, "valid_to": date(2026, 6, 4),
        "source_role": "CORPORATE_ACTION_FILING_ONLY",
    }
    assert history[1] == canonical[1]

def test_regression_defaults_to_legacy_fixture(tmp_path):
    service = PersistentScreenerService(store_path=tmp_path / "control.duckdb", output_root=tmp_path / "runs")
    result = service.run(ScreeningParameters(as_of_date=date(2026, 8, 8), run_mode=RunMode.REGRESSION_REPLAY))
    conn = duckdb.connect(str(tmp_path / "control.duckdb"), read_only=True)
    try:
        fixture = conn.execute(
            "select source_url, metadata_json from source_artifact where source_key = 'canary_fixture'"
        ).fetchone()
    finally:
        conn.close()
    assert fixture[0] == "configs/research_screener/canary_companies_v1.0.0.csv"
    assert json.loads(fixture[1])["fixture_version"] == "1.0.0"
    hawkins = next(member for member in result["members"] if member["symbol"] == "HAWKINCOOK")
    assert hawkins["isin"] == "INE979A01025"
