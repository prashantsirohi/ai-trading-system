import json
from datetime import date

import duckdb

from ai_trading_system.domains.ingest.demerger_repair import (
    build_action,
    inspect_market_evidence,
    load_evidence,
    repair_demerger,
)


def _fixture(tmp_path):
    evidence = {
        "contract_version": "test-v1", "symbol": "TEST", "isin": "INETEST",
        "ex_date": "2025-04-24", "entitlement_ratio": "1:1", "last_cum_date": "2025-04-23",
        "expected_last_cum_close": 100.0, "expected_special_preopen_price": 70.0,
        "official_action": {"symbol": "TEST", "isin": "INETEST", "subject": "Demerger", "exDate": "24-Apr-2025"},
        "official_action_url": "https://nse.test/action", "special_preopen_circular_url": "https://nse.test/circular",
        "special_preopen_method_url": "https://nse.test/method", "entitlement_source_url": "https://issuer.test/ratio",
    }
    raw = json.dumps(evidence["official_action"], sort_keys=True, separators=(",", ":"))
    import hashlib
    evidence["official_action_sha256"] = hashlib.sha256(raw.encode()).hexdigest()
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps(evidence))
    db_path = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""CREATE TABLE _catalog(symbol_id VARCHAR, isin VARCHAR, exchange VARCHAR, timestamp TIMESTAMP,
                 open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adjusted_open DOUBLE, adjusted_high DOUBLE,
                 adjusted_low DOUBLE, adjusted_close DOUBLE, adjustment_factor DOUBLE, adjustment_source VARCHAR,
                 adjusted_at TIMESTAMP, adjustment_version BIGINT, provider VARCHAR, ingestion_ts TIMESTAMP,
                 validation_status VARCHAR, is_benchmark BOOLEAN, instrument_type VARCHAR)""")
    conn.execute("""INSERT INTO _catalog VALUES
                 ('TEST','INETEST','NSE','2025-04-23',100,101,99,100,100,101,99,100,1,NULL,NULL,1,'nse_bhavcopy',NULL,'trusted_primary',false,'equity'),
                 ('TEST','INETEST','NSE','2025-04-24',70,72,68,71,70,72,68,71,1,NULL,NULL,1,'nse_bhavcopy',NULL,'trusted_primary',false,'equity')""")
    conn.close()
    return db_path, evidence_path


def test_build_demerger_action_uses_special_preopen_equilibrium(tmp_path):
    db_path, evidence_path = _fixture(tmp_path)
    evidence = load_evidence(evidence_path)
    market = inspect_market_evidence(db_path, evidence)
    action = build_action(evidence, market)
    assert action.action_type == "demerger"
    assert action.parsed_ratio == "1:1"
    assert action.price_factor == 0.7
    assert action.share_factor == 1.0


def test_repair_is_preview_by_default_and_backup_gated_on_apply(tmp_path):
    db_path, evidence_path = _fixture(tmp_path)
    preview = repair_demerger(db_path, evidence_path, apply=False)
    assert preview["status"] == "preview" and preview["applied"] is False
    conn = duckdb.connect(str(db_path), read_only=True)
    assert conn.execute("select count(*) from information_schema.tables where table_name='_corporate_actions'").fetchone()[0] == 0
    conn.close()

    applied = repair_demerger(db_path, evidence_path, apply=True, backup_root=tmp_path / "backups")
    assert applied["status"] == "completed" and applied["applied"] is True
    assert applied["backup"]["sha256"]
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        action = conn.execute("select action_type, parsed_ratio, price_factor from _corporate_actions").fetchone()
        bars = conn.execute("select cast(timestamp as date),adjusted_close,adjustment_factor from _catalog order by timestamp").fetchall()
    finally:
        conn.close()
    assert action == ("demerger", "1:1", 0.7)
    assert bars == [(date(2025, 4, 23), 70.0, 0.7), (date(2025, 4, 24), 71.0, 1.0)]
