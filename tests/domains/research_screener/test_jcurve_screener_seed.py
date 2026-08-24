from __future__ import annotations

import csv
import sqlite3
from datetime import date
from pathlib import Path

import duckdb

from ai_trading_system.domains.research_screener.jcurve.cohort import (
    DiscoveryRunCohort,
    SeedRunCohort,
)
from ai_trading_system.domains.research_screener.jcurve.discovery_v2 import (
    JCurveDiscoveryV2Service,
    ScreenerDiscoveryPolicyV2,
)
from ai_trading_system.domains.research_screener.jcurve.screener_seed import (
    ScreenerHistoryReader,
    ScreenerSeedPolicy,
    ScreenerSeedService,
    _canonical_query,
    parse_screen_export,
    profile_baseline,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
POLICY_PATH = PROJECT_ROOT / "configs/research_screener/jcurve/screener_seed_policy_v1.json"
DISCOVERY_POLICY_PATH = (
    PROJECT_ROOT / "configs/research_screener/jcurve/screener_discovery_policy_v2.json"
)
AS_OF = date(2026, 8, 23)
QUARTERS = [
    "2026-06-30", "2026-03-31", "2025-12-31", "2025-09-30",
    "2025-06-30", "2025-03-31", "2024-12-31", "2024-09-30",
]


def test_query_canonicalization_ignores_only_redundant_outer_parentheses() -> None:
    rendered = "(Net block + CWIP) > 1.5 * (Net block preceding year + CWIP preceding year)"
    governed = f"({rendered})"

    assert _canonical_query(rendered) == _canonical_query(governed)
    assert _canonical_query(rendered) != _canonical_query(f"({rendered}) AND ROCE > 12")


def test_profile_baseline_preserves_roles_and_reports_missing_history(tmp_path: Path) -> None:
    fundamentals = tmp_path / "fundamentals.db"
    conn = _db(fundamentals)
    _annual(conn, "FCL", [
        ("2026-03-31", 277.76, 2.18, 13.42),
        ("2025-03-31", 178.69, 21.08, 9.22),
        ("2024-03-31", 141.07, None, 6.02),
    ])
    _quarters(
        conn, "FCL", (772.0, 144.0, 62.0), (400.0, 100.0, 40.0),
        basis="consolidated",
    )
    _snapshot(conn, "FCL", 4965.44)
    conn.commit()
    conn.close()
    result = profile_baseline(
        {
            "cohort_version": "fixture",
            "members": [
                {"nse_symbol": "FCL", "case_role": "anchor", "matched_screen_ids": []},
                {"nse_symbol": "MISSING", "case_role": "challenge", "matched_screen_ids": [1]},
            ],
        },
        reader=ScreenerHistoryReader(fundamentals, _policy(), as_of_date=AS_OF),
    )

    assert result["case_count"] == 2
    assert result["history_available_count"] == 1
    assert result["missing_symbols"] == ["MISSING"]
    assert result["dispositions_by_role"] == {"anchor": {"COMMISSIONING": 1}}


def _db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE screener_financials (
            symbol TEXT, period_type TEXT, report_date DATE, statement_basis TEXT,
            metric_id TEXT, value REAL, available_at DATE, source TEXT,
            sync_batch_id TEXT, synced_at TIMESTAMP)"""
    )
    conn.execute(
        """CREATE TABLE screener_company_snapshot (
            symbol TEXT, as_of_date DATE, face_value REAL, market_cap_cr REAL,
            source TEXT, sync_batch_id TEXT, synced_at TIMESTAMP)"""
    )
    return conn


def _annual(
    conn: sqlite3.Connection,
    symbol: str,
    values: list[tuple[str, float | None, float | None, float | None]],
    *,
    basis: str = "consolidated",
) -> None:
    for report_date, net_block, cwip, depreciation in values:
        for metric, value in (
            ("net_block", net_block),
            ("capital_work_in_progress", cwip),
            ("depreciation", depreciation),
        ):
            if value is not None:
                conn.execute(
                    "INSERT INTO screener_financials VALUES (?,?,?,?,?,?,?,?,?,?)",
                    [symbol, "annual", report_date, basis, metric, value,
                     "2026-06-29", "screener", "batch", "2026-06-29"],
                )


def _quarters(
    conn: sqlite3.Connection,
    symbol: str,
    current: tuple[float, float, float],
    previous: tuple[float, float, float],
    *,
    basis: str,
    count: int = 8,
) -> None:
    for index, report_date in enumerate(QUARTERS[:count]):
        totals = current if index < 4 else previous
        for metric, total in zip(("sales", "operating_profit", "depreciation"), totals, strict=True):
            conn.execute(
                "INSERT INTO screener_financials VALUES (?,?,?,?,?,?,?,?,?,?)",
                [symbol, "quarterly", report_date, basis, metric, total / 4.0,
                 "2026-08-14", "screener", "batch", "2026-08-14"],
            )


def _snapshot(conn: sqlite3.Connection, symbol: str, market_cap: float) -> None:
    conn.execute(
        "INSERT INTO screener_company_snapshot VALUES (?,?,?,?,?,?,?)",
        [symbol, "2026-08-12", 10.0, market_cap, "screener", "batch", "2026-08-12"],
    )


def _policy() -> ScreenerSeedPolicy:
    return ScreenerSeedPolicy.load(POLICY_PATH)


def test_e2e_uses_complete_standalone_history_and_passes_commissioning_and_ramp(tmp_path: Path) -> None:
    path = tmp_path / "fundamentals.db"
    conn = _db(path)
    _annual(conn, "E2E", [("2026-03-31", None, None, 169.23)], basis="consolidated")
    _quarters(conn, "E2E", (245.58, 126.3, 169.23), (164.0, 96.7, 60.08), basis="consolidated", count=3)
    _annual(conn, "E2E", [
        ("2026-03-31", 1035.56, 533.45, 169.23),
        ("2025-03-31", 389.33, 636.18, 60.08),
        ("2024-03-31", 210.39, None, 15.75),
        ("2023-03-31", 42.0, None, 20.11),
    ], basis="standalone")
    _quarters(conn, "E2E", (366.22, 233.66, 202.44), (158.74, 79.9, 76.81), basis="standalone")
    _snapshot(conn, "E2E", 12988.68)
    conn.commit()
    conn.close()

    row = ScreenerHistoryReader(path, _policy(), as_of_date=AS_OF).classify(
        "E2E", screen_member=False,
    )

    assert row["statement_basis"] == "standalone"
    assert row["basis_resolution_reason"] == "standalone_fallback_insufficient_consolidated_history"
    assert row["accounting_visible"] is True
    assert "MISSING_CWIP_NET_BLOCK_FALLBACK" in row["reason_codes"]
    assert row["commissioning_signal"] is True
    assert row["ramp_signal"] is True
    assert row["disposition"] == "RAMP_ACTIVE"


def test_fcl_missing_cwip_uses_net_block_fallback_without_fabricating_zero(tmp_path: Path) -> None:
    path = tmp_path / "fundamentals.db"
    conn = _db(path)
    _annual(conn, "FCL", [
        ("2026-03-31", 277.76, 2.18, 13.42),
        ("2025-03-31", 178.69, 21.08, 9.22),
        ("2024-03-31", 141.07, None, 6.02),
    ])
    _quarters(conn, "FCL", (772.0, 144.0, 62.0), (400.0, 100.0, 40.0), basis="consolidated")
    _snapshot(conn, "FCL", 4965.44)
    conn.commit()
    conn.close()

    row = ScreenerHistoryReader(path, _policy(), as_of_date=AS_OF).classify(
        "FCL", screen_member=True,
    )

    assert row["metrics"]["cwip_two_year"] is None
    assert row["metrics"]["capital_base_2y_growth"] is None
    assert "MISSING_CWIP_NET_BLOCK_FALLBACK" in row["reason_codes"]
    assert row["commissioning_signal"] is True
    assert row["ramp_signal"] is False
    assert row["disposition"] == "COMMISSIONING"


def test_revised_accounting_union_recovers_four_threshold_misses(tmp_path: Path) -> None:
    path = tmp_path / "fundamentals.db"
    conn = _db(path)
    cases = {
        "APLAPOLLO": ([
            ("2026-03-31", 4060.47, 328.24, 230.92),
            ("2025-03-31", 3667.92, 335.52, 201.32),
            ("2024-03-31", 3280.96, 202.99, 175.93),
            ("2023-03-31", 2580.45, 373.98, 138.33),
            ("2022-03-31", 1837.36, 503.68, 108.97),
        ], 55610.81, True),
        "JINDALSAW": ([
            ("2026-03-31", 10408.53, 452.88, 630.49),
            ("2025-03-31", 9341.01, 640.89, 602.06),
            ("2024-03-31", 8852.9, 632.2, 567.99),
        ], 17250.96, True),
        "NTPC": ([
            ("2026-03-31", 318820.81, 84832.76, 19629.33),
            ("2025-03-31", 271436.58, 100859.28, 17401.19),
            ("2024-03-31", 258933.63, 87664.45, 16203.63),
        ], 327747.34, True),
        "TATAPOWER": ([
            ("2026-03-31", 87292.69, 14595.13, 4811.09),
            ("2025-03-31", 78374.43, 12678.87, 4116.86),
            ("2024-03-31", 67209.56, 11561.31, 3786.37),
            ("2023-03-31", 61746.66, 5376.36, 3439.2),
            ("2022-03-31", 57389.44, 4635.1, 3122.2),
        ], 119825.2, True),
        "KPRMILL": ([
            ("2026-03-31", 2402.7, 63.14, 215.6),
            ("2025-03-31", 2461.08, 40.35, 207.87),
            ("2024-03-31", 2429.31, 117.51, 189.19),
            ("2023-03-31", 2306.41, 86.65, 173.69),
            ("2022-03-31", 1940.34, 115.32, 141.12),
        ], 37153.49, False),
        "LT": ([
            ("2026-03-31", 30866.91, 3310.95, 4364.75),
            ("2025-03-31", 44055.45, 2588.68, 4121.18),
            ("2024-03-31", 42963.82, 3045.01, 3682.33),
            ("2023-03-31", 42641.0, 3065.57, 3502.25),
            ("2022-03-31", 42945.01, 1249.55, 2947.95),
        ], 550621.64, False),
    }
    for symbol, (annual, market_cap, _) in cases.items():
        _annual(conn, symbol, annual)
        _snapshot(conn, symbol, market_cap)
    conn.commit()
    conn.close()

    reader = ScreenerHistoryReader(path, _policy(), as_of_date=AS_OF)
    observed = {
        symbol: reader.classify(symbol, screen_member=False)["accounting_visible"]
        for symbol in cases
    }
    assert observed == {symbol: expected for symbol, (_, _, expected) in cases.items()}


def test_screen_export_parser_accepts_nse_code_and_rejects_duplicates(tmp_path: Path) -> None:
    export = tmp_path / "screen.csv"
    export.write_text(
        "Name,BSE Code,NSE Code,ISIN Code,Market Cap\n"
        "Fineotex Chemical,533333,FCL,INE045J01026,4965\n"
        "E2E Networks,535080,E2E,INE255Z01019,12988\n"
        "ACE Alpha Tech,544431,,INE0S9X01011,232\n"
        "Gaja Alternative Asset,,,INE18UN01038,500\n"
    )
    assert parse_screen_export(export) == [
        {
            "symbol": "FCL", "company_name": "Fineotex Chemical",
            "listing_exchange": "NSE", "listing_code": "FCL",
            "isin": "INE045J01026", "nse_symbol": "FCL", "bse_code": "533333",
        },
        {
            "symbol": "E2E", "company_name": "E2E Networks",
            "listing_exchange": "NSE", "listing_code": "E2E",
            "isin": "INE255Z01019", "nse_symbol": "E2E", "bse_code": "535080",
        },
        {
            "symbol": "544431", "company_name": "ACE Alpha Tech",
            "listing_exchange": "BSE", "listing_code": "544431",
            "isin": "INE0S9X01011", "nse_symbol": None, "bse_code": "544431",
        },
        {
            "symbol": "ISIN:INE18UN01038", "company_name": "Gaja Alternative Asset",
            "listing_exchange": None, "listing_code": None,
            "isin": "INE18UN01038", "nse_symbol": None, "bse_code": None,
        },
    ]

    export.write_text("Name,NSE Code\nFineotex,FCL\nFineotex,FCL\n")
    try:
        parse_screen_export(export)
    except ValueError as exc:
        assert "duplicate symbols" in str(exc)
    else:
        raise AssertionError("duplicate screen symbols should fail")


def test_seed_service_persists_immutable_candidates_and_exposes_bootstrap_cohort(tmp_path: Path) -> None:
    fundamentals = tmp_path / "fundamentals.db"
    conn = _db(fundamentals)
    _annual(conn, "FCL", [
        ("2026-03-31", 277.76, 2.18, 13.42),
        ("2025-03-31", 178.69, 21.08, 9.22),
        ("2024-03-31", 141.07, None, 6.02),
    ])
    _quarters(conn, "FCL", (772.0, 144.0, 62.0), (400.0, 100.0, 40.0), basis="consolidated")
    _snapshot(conn, "FCL", 4965.44)
    conn.commit()
    conn.close()

    store_path = tmp_path / "research.duckdb"
    from ai_trading_system.domains.research_screener.jcurve.store import JCurveStore

    JCurveStore(store_path)
    identity = duckdb.connect(str(store_path))
    identity.execute(
        "INSERT INTO source_artifact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ["identity-source", None, "fixture", "TEST", None, None, AS_OF, None, AS_OF,
         "a" * 64, 1, 1, "v1", "v1", "VALID", None, "{}"],
    )
    identity.execute(
        "INSERT INTO company_master VALUES ('company:fcl','Fineotex Chemical Limited','LISTED',DATE '2020-01-01',NULL,'identity-source')"
    )
    identity.execute(
        "INSERT INTO security_master VALUES ('security:fcl','company:fcl','INE045J01026','Fineotex Chemical Limited','EQUITY',1,'INR',DATE '2020-01-01',NULL,'identity-source')"
    )
    identity.execute(
        "INSERT INTO listing_master VALUES ('listing:fcl','security:fcl','NSE','FCL','FCL',NULL,'EQ','MAIN',TRUE,DATE '2011-01-01',NULL,DATE '2020-01-01',NULL,'identity-source')"
    )
    identity.execute(
        "INSERT INTO company_master VALUES ('company:ace','ACE Alpha Tech Limited','LISTED',DATE '2020-01-01',NULL,'identity-source')"
    )
    identity.execute(
        "INSERT INTO security_master VALUES ('security:ace','company:ace','INE0S9X01011','ACE Alpha Tech Limited','EQUITY',1,'INR',DATE '2020-01-01',NULL,'identity-source')"
    )
    identity.execute(
        "INSERT INTO listing_master VALUES ('listing:ace','security:ace','BSE','544431','ACEALPHA','544431','X','SME',TRUE,DATE '2020-01-01',NULL,DATE '2020-01-01',NULL,'identity-source')"
    )
    identity.close()

    screen = tmp_path / "screen.csv"
    with screen.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Name", "BSE Code", "NSE Code", "ISIN Code"])
        writer.writerow(["Fineotex Chemical Limited", "533333", "FCL", "INE045J01026"])
        writer.writerow(["Uncovered Screen Company", "", "UNCOVERED", ""])
        writer.writerow(["ACE Alpha Tech Limited", "544431", "", "INE0S9X01011"])

    result = ScreenerSeedService(
        store_path=store_path,
        output_root=tmp_path / "runs",
        fundamentals_db=fundamentals,
        policy=_policy(),
    ).run(as_of_date=AS_OF, screen_id=317873, screen_export=screen)

    assert result["candidate_count"] == 1
    assert result["evaluated_count"] == 3
    assert result["history_available_count"] == 1
    assert result["history_unavailable_count"] == 2
    assert result["history_coverage_rate"] == 1 / 3
    assert result["resolved_count"] == 2
    conn = duckdb.connect(str(store_path), read_only=True)
    try:
        persisted = conn.execute(
            "SELECT symbol, disposition, accepted FROM jcurve_seed_candidate ORDER BY symbol"
        ).fetchall()
    finally:
        conn.close()
    assert persisted == [
        ("544431", "HISTORY_UNAVAILABLE", False),
        ("FCL", "COMMISSIONING", True),
        ("UNCOVERED", "HISTORY_UNAVAILABLE", False),
    ]
    conn = duckdb.connect(str(store_path), read_only=True)
    try:
        bse_row = conn.execute(
            "SELECT screen_exchange, screen_listing_code, screen_isin, listing_id "
            "FROM jcurve_seed_candidate WHERE symbol = '544431'"
        ).fetchone()
    finally:
        conn.close()
    assert bse_row == ("BSE", "544431", "INE0S9X01011", "listing:ace")
    cohort = SeedRunCohort(run_id=result["run_id"]).resolve(
        store_path=store_path, as_of_date=AS_OF,
    )
    assert cohort.company_ids == ("company:fcl",)
    assert cohort.members[0]["nse_symbol"] == "FCL"


def test_discovery_v2_intersects_universe_classifies_and_bounds_queue(tmp_path: Path) -> None:
    fundamentals = tmp_path / "fundamentals.db"
    conn = _db(fundamentals)
    _annual(conn, "RAMP", [
        ("2026-03-31", 220.0, 10.0, 30.0),
        ("2025-03-31", 150.0, 20.0, 20.0),
        ("2024-03-31", 100.0, 10.0, 15.0),
    ])
    _quarters(conn, "RAMP", (200.0, 50.0, 30.0), (100.0, 20.0, 15.0), basis="consolidated")
    _snapshot(conn, "RAMP", 5000.0)
    _annual(conn, "BUILD", [
        ("2026-03-31", 100.0, 100.0, 10.0),
        ("2025-03-31", 100.0, 50.0, 10.0),
        ("2024-03-31", 50.0, 10.0, 8.0),
    ])
    _quarters(conn, "BUILD", (100.0, 20.0, 10.0), (100.0, 20.0, 10.0), basis="consolidated")
    _snapshot(conn, "BUILD", 2000.0)
    conn.commit()
    conn.close()

    from ai_trading_system.domains.research_screener.jcurve.store import JCurveStore

    store_path = tmp_path / "research.duckdb"
    JCurveStore(store_path)
    _discovery_identities_and_universe(store_path)
    exports: dict[int, Path] = {}
    membership = {
        3901581: ["RAMP", "BUILD", "OUTSIDE"],
        3901588: ["RAMP"],
        3901589: ["RAMP", "BUILD", "OUTSIDE"],
        3901592: ["RAMP", "BUILD", "OUTSIDE"],
    }
    identifiers = {
        "RAMP": ("INE000A01001", "500001"),
        "BUILD": ("INE000A01002", "500002"),
        "OUTSIDE": ("INE000A01003", "500003"),
    }
    for screen_id, symbols in membership.items():
        path = tmp_path / f"screen-{screen_id}.csv"
        with path.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["Name", "BSE Code", "NSE Code", "ISIN Code"])
            for symbol in symbols:
                isin, bse = identifiers[symbol]
                writer.writerow([f"{symbol} Limited", bse, symbol, isin])
        exports[screen_id] = path

    result = JCurveDiscoveryV2Service(
        store_path=store_path,
        output_root=tmp_path / "runs",
        fundamentals_db=fundamentals,
        policy=ScreenerDiscoveryPolicyV2.load(DISCOVERY_POLICY_PATH),
        accounting_policy=_policy(),
        cohort={
            "cohort_version": "fixture-v2",
            "members": [{
                "isin": "INE000A01001", "nse_symbol": "RAMP",
                "bse_code": "500001", "legal_name": "RAMP Limited",
                "case_role": "known_anchor",
            }],
        },
    ).run(as_of_date=AS_OF, screen_exports=exports)

    assert result["union_count"] == 3
    assert result["universe_eligible_count"] == 2
    assert result["focus_count"] == 2
    assert result["primary_queue_count"] == 1
    assert result["watchlist_count"] == 1
    persisted = duckdb.connect(str(store_path), read_only=True)
    try:
        rows = persisted.execute(
            """SELECT nse_symbol, accounting_disposition, queue_disposition, queue_rank
               FROM jcurve_discovery_candidate ORDER BY nse_symbol"""
        ).fetchall()
    finally:
        persisted.close()
    assert rows == [
        ("BUILD", "BUILD", "WATCHLIST", None),
        ("OUTSIDE", None, "UNIVERSE_EXCLUDED", None),
        ("RAMP", "RAMP_ACTIVE", "PRIMARY_RESEARCH", 1),
    ]
    cohort = DiscoveryRunCohort(run_id=result["run_id"]).resolve(
        store_path=store_path, as_of_date=AS_OF,
    )
    assert cohort.company_ids == ("company:ramp",)
    assert cohort.members[0]["queue_rank"] == 1


def _discovery_identities_and_universe(path: Path) -> None:
    conn = duckdb.connect(str(path))
    conn.execute(
        "INSERT INTO source_artifact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ["identity-v2", None, "fixture", "TEST", None, None, AS_OF, None, AS_OF,
         "b" * 64, 1, 3, "v1", "v1", "VALID", None, "{}"],
    )
    for index, symbol in enumerate(("RAMP", "BUILD", "OUTSIDE"), start=1):
        company_id = f"company:{symbol.lower()}"
        security_id = f"security:{symbol.lower()}"
        isin = f"INE000A0100{index}"
        conn.execute(
            "INSERT INTO company_master VALUES (?,?, 'LISTED', DATE '2020-01-01', NULL, 'identity-v2')",
            [company_id, f"{symbol} Limited"],
        )
        conn.execute(
            """INSERT INTO security_master VALUES (?, ?, ?, ?, 'EQUITY', 1, 'INR',
               DATE '2020-01-01', NULL, 'identity-v2')""",
            [security_id, company_id, isin, f"{symbol} Limited"],
        )
        conn.execute(
            """INSERT INTO listing_master VALUES (?, ?, 'NSE', ?, ?, NULL, 'EQ', 'MAIN', TRUE,
               DATE '2020-01-01', NULL, DATE '2020-01-01', NULL, 'identity-v2')""",
            [f"listing:{symbol.lower()}", security_id, symbol, symbol],
        )
        conn.execute(
            """INSERT INTO listing_master VALUES (?, ?, 'BSE', ?, ?, ?, 'A', 'MAIN', TRUE,
               DATE '2020-01-01', NULL, DATE '2020-01-01', NULL, 'identity-v2')""",
            [f"listing:bse:{symbol.lower()}", security_id, f"50000{index}", symbol, f"50000{index}"],
        )
    conn.execute(
        """INSERT INTO screen_definition VALUES
           ('screen:full-universe-fixture', 'full-universe-fixture', CURRENT_TIMESTAMP)"""
    )
    conn.execute(
        """INSERT INTO screen_definition_version VALUES
           ('screen:full-universe-fixture:v1', 'screen:full-universe-fixture', 'v1', '{}', 'hash', CURRENT_TIMESTAMP)"""
    )
    conn.execute(
        """INSERT INTO screening_run VALUES (
           'full-universe-fixture', 'screen:full-universe-fixture:v1', 'full_universe', ?, ?, ?,
           1000, 100000, 'snapshot', 'test', 'COMPLETED', 2, 2,
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, NULL, NULL)""",
        [AS_OF, AS_OF, AS_OF],
    )
    conn.execute(
        """INSERT INTO universe_snapshot VALUES
           ('universe:fixture', 'full-universe-fixture', 'snapshot', 2, CURRENT_TIMESTAMP)"""
    )
    for symbol in ("RAMP", "BUILD"):
        conn.execute(
            """INSERT INTO universe_member VALUES (
               ?, 'universe:fixture', ?, ?, ?, 'RESOLVED', 5000, ?, 'ELIGIBLE',
               'consolidated', 1, 1, 'CLEAR', 1, 'NOT_REQUIRED', 'ELIGIBLE', '{}')""",
            [f"member:{symbol.lower()}", f"company:{symbol.lower()}",
             f"security:{symbol.lower()}", symbol, AS_OF],
        )
    conn.close()
