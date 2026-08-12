"""Backup-gated repair for an official NSE special-pre-open demerger.

Preview is the default. ``--apply`` backs up the operational OHLCV store,
persists one evidence-bound action, and recomputes only the explicit symbol.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.domains.ingest.corporate_actions import (
    ParsedCorporateAction,
    ensure_corporate_action_schema,
    make_corporate_action_key,
    recompute_adjusted_prices,
    reconcile_corporate_actions,
)
from ai_trading_system.platform.db.paths import get_domain_paths, require_data_root_available
from ai_trading_system.platform.utils.env import load_project_env

SOURCE = "nse_demerger_special_preopen"
NORMALIZER_VERSION = 3


@dataclass(frozen=True)
class DemergerEvidence:
    contract_version: str
    symbol: str
    isin: str
    ex_date: date
    entitlement_ratio: str
    last_cum_date: date
    expected_last_cum_close: float
    expected_special_preopen_price: float
    official_action: dict[str, Any]
    official_action_sha256: str
    official_action_url: str
    special_preopen_circular_url: str
    special_preopen_method_url: str
    entitlement_source_url: str


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_evidence(path: str | Path) -> DemergerEvidence:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    evidence = DemergerEvidence(
        contract_version=str(payload["contract_version"]),
        symbol=str(payload["symbol"]).upper(),
        isin=str(payload["isin"]).upper(),
        ex_date=date.fromisoformat(payload["ex_date"]),
        entitlement_ratio=str(payload["entitlement_ratio"]),
        last_cum_date=date.fromisoformat(payload["last_cum_date"]),
        expected_last_cum_close=float(payload["expected_last_cum_close"]),
        expected_special_preopen_price=float(payload["expected_special_preopen_price"]),
        official_action=dict(payload["official_action"]),
        official_action_sha256=str(payload["official_action_sha256"]),
        official_action_url=str(payload["official_action_url"]),
        special_preopen_circular_url=str(payload["special_preopen_circular_url"]),
        special_preopen_method_url=str(payload["special_preopen_method_url"]),
        entitlement_source_url=str(payload["entitlement_source_url"]),
    )
    observed_hash = hashlib.sha256(_canonical_json(evidence.official_action).encode("utf-8")).hexdigest()
    if observed_hash != evidence.official_action_sha256:
        raise ValueError("official action payload checksum does not match the repair contract")
    raw = evidence.official_action
    if str(raw.get("symbol", "")).upper() != evidence.symbol:
        raise ValueError("official action symbol does not match the repair target")
    if str(raw.get("isin", "")).upper() != evidence.isin:
        raise ValueError("official action ISIN does not match the repair target")
    if str(raw.get("subject", "")).strip().lower() != "demerger":
        raise ValueError("official action is not an exact demerger event")
    if datetime.strptime(str(raw.get("exDate")), "%d-%b-%Y").date() != evidence.ex_date:
        raise ValueError("official action ex-date does not match the repair target")
    if evidence.last_cum_date >= evidence.ex_date:
        raise ValueError("last cum date must precede the demerger ex-date")
    return evidence


def inspect_market_evidence(db_path: str | Path, evidence: DemergerEvidence) -> dict[str, Any]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """SELECT cast(timestamp AS DATE), open, close, provider, validation_status
               FROM _catalog
               WHERE upper(symbol_id) = ? AND exchange = 'NSE'
                 AND cast(timestamp AS DATE) IN (?, ?)
               ORDER BY timestamp""",
            [evidence.symbol, evidence.last_cum_date, evidence.ex_date],
        ).fetchall()
    finally:
        conn.close()
    by_date = {row[0]: row for row in rows}
    if set(by_date) != {evidence.last_cum_date, evidence.ex_date}:
        raise RuntimeError("trusted catalog does not contain both repair boundary sessions")
    last_cum = by_date[evidence.last_cum_date]
    ex_day = by_date[evidence.ex_date]
    last_close = float(last_cum[2])
    special_open = float(ex_day[1])
    if abs(last_close - evidence.expected_last_cum_close) > 1e-9:
        raise RuntimeError(f"last cum close drifted: expected {evidence.expected_last_cum_close}, observed {last_close}")
    if abs(special_open - evidence.expected_special_preopen_price) > 1e-9:
        raise RuntimeError(f"special-pre-open price drifted: expected {evidence.expected_special_preopen_price}, observed {special_open}")
    for row in (last_cum, ex_day):
        if row[3] != "nse_bhavcopy" or not str(row[4]).startswith("trusted"):
            raise RuntimeError(f"repair boundary is not trusted NSE bhavcopy evidence: {row}")
    factor = special_open / last_close
    if not 0 < factor < 1:
        raise RuntimeError(f"demerger price factor must be inside (0, 1), observed {factor}")
    return {
        "last_cum_date": evidence.last_cum_date,
        "last_cum_close": last_close,
        "ex_date": evidence.ex_date,
        "special_preopen_price": special_open,
        "price_factor": factor,
        "providers": [last_cum[3], ex_day[3]],
        "validation_statuses": [last_cum[4], ex_day[4]],
    }


def build_action(evidence: DemergerEvidence, market: dict[str, Any]) -> ParsedCorporateAction:
    lineage = {
        "contract_version": evidence.contract_version,
        "official_action": evidence.official_action,
        "official_action_sha256": evidence.official_action_sha256,
        "official_action_url": evidence.official_action_url,
        "special_preopen_circular_url": evidence.special_preopen_circular_url,
        "special_preopen_method_url": evidence.special_preopen_method_url,
        "entitlement_source_url": evidence.entitlement_source_url,
        "entitlement_ratio": evidence.entitlement_ratio,
        "market_evidence": market,
        "formula": "special_preopen_price / last_cum_close",
    }
    raw_payload_json = _canonical_json(lineage)
    return ParsedCorporateAction(
        symbol=evidence.symbol,
        isin=evidence.isin,
        ex_date=evidence.ex_date,
        action_type="demerger",
        parsed_ratio=evidence.entitlement_ratio,
        price_factor=float(market["price_factor"]),
        share_factor=1.0,
        source=SOURCE,
        raw_subject="Demerger; NSE-mandated special pre-open equilibrium adjustment",
        raw_payload_hash=hashlib.sha256(raw_payload_json.encode("utf-8")).hexdigest(),
        raw_payload_json=raw_payload_json,
    )


def repair_demerger(db_path: str | Path, evidence_path: str | Path, *, apply: bool,
                    backup_root: str | Path | None = None) -> dict[str, Any]:
    db_path = Path(db_path)
    evidence = load_evidence(evidence_path)
    market = inspect_market_evidence(db_path, evidence)
    action = build_action(evidence, market)
    report: dict[str, Any] = {
        "status": "preview" if not apply else "started",
        "symbol": evidence.symbol,
        "isin": evidence.isin,
        "ex_date": evidence.ex_date,
        "entitlement_ratio": evidence.entitlement_ratio,
        "price_factor": action.price_factor,
        "action_payload_hash": action.raw_payload_hash,
        "market_evidence": market,
        "applied": False,
    }
    if not apply:
        return report

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = Path(backup_root or db_path.parent / "backups") / f"demerger-repair-{evidence.symbol}-{timestamp}"
    backup_dir.mkdir(parents=True, exist_ok=False)
    backup_path = backup_dir / db_path.name
    shutil.copy2(db_path, backup_path)
    report["backup"] = {"path": str(backup_path), "sha256": _sha256_file(backup_path)}

    ensure_corporate_action_schema(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("BEGIN TRANSACTION")
        reconcile = reconcile_corporate_actions(
            db_path, [action], fetch_from=evidence.ex_date, fetch_to=evidence.ex_date,
            normalizer_version=NORMALIZER_VERSION, _deactivate_missing=False, _conn=conn,
        )
        adjusted = recompute_adjusted_prices(
            db_path, symbols=[evidence.symbol], force=False, _conn=conn,
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    verified = inspect_market_evidence(db_path, evidence)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        row = conn.execute(
            """SELECT action_type, parsed_ratio, price_factor, share_factor, source, status
               FROM _corporate_actions WHERE action_key = ?""",
            [make_corporate_action_key(action)],
        ).fetchone()
    finally:
        conn.close()
    if row != ("demerger", evidence.entitlement_ratio, action.price_factor, 1.0, SOURCE, "active"):
        raise RuntimeError(f"persisted demerger action failed verification: {row}")
    report.update({"status": "completed", "applied": True, "reconcile": reconcile,
                   "adjustment": adjusted, "post_apply_market_evidence": verified})
    report_path = backup_dir / "repair_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    report["report_path"] = str(report_path)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preview or apply an evidence-bound NSE demerger repair.")
    parser.add_argument("--evidence-file", type=Path, required=True)
    parser.add_argument("--data-domain", default="operational")
    parser.add_argument("--apply", action="store_true", help="Back up and mutate the operational OHLCV store")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    load_project_env()
    require_data_root_available()
    paths = get_domain_paths(data_domain=args.data_domain)
    result = repair_demerger(paths.ohlcv_db_path, args.evidence_file, apply=args.apply)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
