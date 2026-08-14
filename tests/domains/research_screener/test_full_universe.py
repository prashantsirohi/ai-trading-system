from __future__ import annotations

from datetime import date

from ai_trading_system.domains.research_screener.models import RunMode, ScreeningParameters
from ai_trading_system.domains.research_screener.providers import _artifact
from ai_trading_system.domains.research_screener.reporting import (
    FILING_DISCOVERY_PACK_FILES,
    FULL_UNIVERSE_PACK_FILES,
)
from ai_trading_system.domains.research_screener.service import PersistentScreenerService


class _Exchange:
    def acquire_identity(self, as_of_date):
        nse_rows = [
            {
                "SYMBOL": "DUAL", "NAME OF COMPANY": "Dual Limited", "SERIES": "EQ",
                "DATE OF LISTING": "01-JAN-2020", "ISIN NUMBER": "INE000A01001", "FACE VALUE": "1",
            },
        ]
        bse_rows = [
            {
                "SCRIP_CD": "500001", "scrip_id": "DUAL", "Issuer_Name": "Dual Limited",
                "Scrip_Name": "Dual", "Status": "Active", "Segment": "Equity", "GROUP": "A",
                "ISIN_NUMBER": "INE000A01001", "FACE_VALUE": "1", "Mktcap": "5000",
            },
            {
                "SCRIP_CD": "500002", "scrip_id": "SMALL", "Issuer_Name": "Small Limited",
                "Scrip_Name": "Small", "Status": "Active", "Segment": "Equity", "GROUP": "M",
                "ISIN_NUMBER": "INE000B01009", "FACE_VALUE": "10", "Mktcap": "1500",
            },
            {
                "SCRIP_CD": "500003", "scrip_id": "FUND", "Issuer_Name": "Fund",
                "Scrip_Name": "Fund ETF", "Status": "Active", "Segment": "Equity", "GROUP": "B",
                "ISIN_NUMBER": "INF000000001", "FACE_VALUE": "10", "Mktcap": "2000",
            },
            {
                "SCRIP_CD": "500004", "scrip_id": "NOISIN", "Issuer_Name": "No Isin Limited",
                "Scrip_Name": "No Isin", "Status": "Active", "Segment": "Equity", "GROUP": "B",
                "ISIN_NUMBER": "", "FACE_VALUE": "10", "Mktcap": "2000",
            },
        ]
        return {
            "effective_date": as_of_date,
            "mii_rows": [{"ISIN": "INE000A01001"}],
            "nse_rows": nse_rows,
            "bse_rows": bse_rows,
            "artifacts": [
                _artifact("combined_security_master", "NSE", b"combined", url="nse://combined", effective_date=as_of_date, row_count=1),
                _artifact("nse_equity_master", "NSE", b"nse", url="nse://equity", effective_date=as_of_date, row_count=1),
                _artifact("bse_active_equity_master", "BSE", b"bse", url="bse://active", effective_date=as_of_date, row_count=4),
            ],
        }

    def nse_market_cap(self, symbol, as_of_date, *, expected_isin):
        artifact = _artifact(
            "nse_market_cap", "NSE", b"cap", url=f"nse://cap/{symbol}",
            effective_date=as_of_date, row_count=1,
        )
        return {
            "full_market_cap_cr": 5_000.0, "free_float_market_cap_cr": 1_000.0,
            "as_of_date": as_of_date, "artifact_id": artifact["artifact_id"],
        }, artifact

    @staticmethod
    def bse_market_cap(row, *, artifact_id):
        return {"full_market_cap_cr": float(row["Mktcap"]), "artifact_id": artifact_id}


class _FilingExchange(_Exchange):
    def __init__(self):
        self.filing_calls = 0

    def nse_fundamental_snapshot(self, symbol, expected_isin, as_of_date, *, company_type,
                                 identifier_history=None):
        self.filing_calls += 1
        artifact = _artifact(
            "filing_xbrl", "NSE", b"filing", url=f"nse://filing/{symbol}",
            effective_date=as_of_date, row_count=1,
        )
        return {
            "scope": "consolidated", "scope_reason": "test",
            "annual_completeness": 1.0, "quarterly_completeness": 1.0,
            "annual_period_count": 6, "quarterly_period_count": 12,
            "annual_statements": [], "quarterly_statements": [], "state": "PRESENT",
            "latest_disclosed_periods": {"annual": as_of_date, "quarterly": as_of_date},
            "latest_parsed_periods": {"annual": as_of_date, "quarterly": as_of_date},
            "target_periods": {"annual": [], "quarterly": []},
            "missing_target_periods": {"annual": [], "quarterly": []},
            "provenance_validation": {
                "provider": ["NSE_XBRL"], "available_at": True,
                "source_row_hash": True, "filing_source": True, "reason": "test pass",
            },
        }, [artifact]

    def nse_corporate_actions(self, symbol, expected_isin, start_date, as_of_date):
        return [], _artifact(
            "nse_corporate_actions", "NSE", b"[]", url=f"nse://actions/{symbol}",
            effective_date=as_of_date, row_count=0,
        )


class _IssuerClassifier:
    @staticmethod
    def snapshot(cohort, as_of_date):
        rows = {
            row["isin"]: {
                "state": "PRESENT", "company_type": "INDUSTRIAL",
                "sector": "Industrials", "industry": "Test Industry",
                "source": "test", "source_row_hash": "test-row-hash",
                "reason": "EXACT_ISIN_TEST_MATCH", "observed_at": str(as_of_date),
            }
            for row in cohort
        }
        return rows, b'{"schema_version":"issuer-classification-snapshot-v1","test":true}\n'


def test_full_universe_normalizes_deduplicates_and_accounts_for_every_listing(tmp_path):
    exchange = _Exchange()
    identity = exchange.acquire_identity(date(2026, 8, 12))
    rows = PersistentScreenerService._normalize_full_universe(
        identity,
        combined_artifact_id=identity["artifacts"][0]["artifact_id"],
        nse_artifact_id=identity["artifacts"][1]["artifact_id"],
        bse_artifact_id=identity["artifacts"][2]["artifact_id"],
    )

    assert len(rows) == 4
    dual = next(row for row in rows if row["symbol"] == "DUAL")
    assert dual["isin"] == "INE000A01001"
    assert {listing["exchange"] for listing in dual["listings"]} == {"NSE", "BSE"}
    assert next(row for row in rows if row["symbol"] == "SMALL")["listings"][0]["board"] == "SME"
    assert next(row for row in rows if row["symbol"] == "FUND")["identity_status"] == "NON_EQUITY_INSTRUMENT"
    assert next(row for row in rows if row["symbol"] == "NOISIN")["identity_status"] == "UNRESOLVED"


def test_full_universe_run_is_persistent_and_fail_closed_after_cap_gate(tmp_path):
    service = PersistentScreenerService(
        project_root=".", store_path=tmp_path / "control.duckdb",
        output_root=tmp_path / "runs", exchange_client=_Exchange(),
    )
    params = ScreeningParameters(as_of_date=date(2026, 8, 12), run_mode=RunMode.FULL_UNIVERSE)

    result = service.run(params)

    assert result["status"] == "COMPLETED"
    members = {member["symbol"]: member for member in result["members"]}
    assert members["DUAL"]["disposition"] == "DATA_REPAIR_REQUIRED"
    assert members["DUAL"]["reasons"][0]["code"] == "PHASE1_FILING_DISCOVERY_REQUIRED"
    assert members["SMALL"]["disposition"] == "INELIGIBLE_BOARD_OR_INSTRUMENT"
    assert members["FUND"]["disposition"] == "INELIGIBLE_BOARD_OR_INSTRUMENT"
    assert members["NOISIN"]["disposition"] == "DATA_REPAIR_REQUIRED"
    output = tmp_path / "runs" / result["run_id"]
    assert all((output / name).exists() for name in FULL_UNIVERSE_PACK_FILES)
    assert "Security-master identity coverage: `66.6667%`" in (output / "universe_summary.md").read_text()


def test_full_universe_defaults_use_a_separate_definition_and_version():
    params = ScreeningParameters(as_of_date=date(2026, 8, 12), run_mode=RunMode.FULL_UNIVERSE)
    assert params.screen_definition == "persistent_screener_phase1"
    assert params.screen_version == "1.0.0"


def test_filing_discovery_freezes_parent_and_reuses_member_checkpoint(tmp_path, monkeypatch):
    store = tmp_path / "control.duckdb"
    runs = tmp_path / "runs"
    parent_service = PersistentScreenerService(
        project_root=".", store_path=store, output_root=runs, exchange_client=_Exchange(),
    )
    parent = parent_service.run(ScreeningParameters(
        as_of_date=date(2026, 8, 12), run_mode=RunMode.FULL_UNIVERSE,
    ))
    monkeypatch.setattr(
        "ai_trading_system.domains.research_screener.providers.ExistingRepositoryProvider.get_corporate_actions",
        lambda self, isin, through_date: [],
    )
    exchange = _FilingExchange()
    params = ScreeningParameters(
        as_of_date=date(2026, 8, 12), run_mode=RunMode.FILING_DISCOVERY,
        parent_run_id=parent["run_id"], batch_size=1,
    )
    first = PersistentScreenerService(
        project_root=".", store_path=store, output_root=runs, exchange_client=exchange,
        issuer_classifier=_IssuerClassifier(),
    ).run(params)

    assert first["status"] == "COMPLETED"
    assert len(first["members"]) == 1
    assert first["members"][0]["disposition"] == "BOUNDARY_REVIEW"
    assert exchange.filing_calls == 1
    output = runs / first["run_id"]
    assert all((output / name).exists() for name in FILING_DISCOVERY_PACK_FILES)
    assert parent["run_id"] in (output / "filing_summary.md").read_text()

    replay_exchange = _FilingExchange()
    replay = PersistentScreenerService(
        project_root=".", store_path=store, output_root=runs, exchange_client=replay_exchange,
        issuer_classifier=_IssuerClassifier(),
    ).run(params)
    assert replay["idempotent_replay"] is True
    assert replay_exchange.filing_calls == 0
