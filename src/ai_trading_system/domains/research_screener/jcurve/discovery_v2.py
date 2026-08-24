from __future__ import annotations

import csv
import hashlib
import json
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.domains.fundamentals.screener_client import ScreenerClient
from ai_trading_system.domains.research_screener.store import content_hash

from .screener_seed import (
    ScreenerHistoryReader,
    ScreenerSeedPolicy,
    _canonical_query,
    detect_screen_export_suffix,
    parse_screen_export,
)
from .store import JCurveStore


@dataclass(frozen=True)
class ScreenerDiscoveryPolicyV2:
    raw: dict[str, Any]
    version: str
    policy_hash: str

    @classmethod
    def load(cls, path: Path) -> "ScreenerDiscoveryPolicyV2":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        required = {
            "policy_version", "screens", "identity_key_order", "calibration_cohort",
            "universe_gate", "focus_gate", "routing", "primary_queue", "authority",
        }
        if set(raw) != required:
            raise ValueError("Screener discovery policy V2 has an invalid top-level shape")
        screen_ids = [int(row["screen_id"]) for row in raw["screens"]]
        if len(screen_ids) != 4 or len(set(screen_ids)) != 4:
            raise ValueError("Screener discovery V2 requires four unique screens")
        for screen in raw["screens"]:
            if set(screen) != {
                "screen_id", "url", "lane", "expected_query", "required_query_terms",
            }:
                raise ValueError("Screener discovery screen has an invalid shape")
            if not str(screen["expected_query"]).strip() or not screen["required_query_terms"]:
                raise ValueError("Screener discovery screens require an exact query contract")
        if raw["identity_key_order"] != ["ISIN", "NSE", "BSE"]:
            raise ValueError("Screener discovery identity order must be ISIN, NSE, BSE")
        if int(raw["primary_queue"]["limit"]) > 250:
            raise ValueError("Screener discovery primary queue cannot exceed 250")
        if set(raw["primary_queue"]["eligible_dispositions"]) != {
            "RAMP_ACTIVE", "COMMISSIONING",
        }:
            raise ValueError("V2 primary queue must contain only ramp and commissioning cases")
        return cls(raw=raw, version=str(raw["policy_version"]), policy_hash=content_hash(raw))


@dataclass(frozen=True)
class FrozenScreen:
    screen_id: int
    lane: str
    url: str
    query_text: str
    path: Path
    raw: bytes
    retrieved_at: datetime
    acquisition_mode: str
    temporary: bool


class JCurveDiscoveryV2Service:
    """Build an immutable, bounded four-screen accounting-discovery queue."""

    def __init__(
        self,
        *,
        store_path: Path,
        output_root: Path,
        fundamentals_db: Path,
        policy: ScreenerDiscoveryPolicyV2,
        accounting_policy: ScreenerSeedPolicy,
        cohort: dict[str, Any],
    ):
        self.store = JCurveStore(store_path)
        self.store_path = Path(store_path)
        self.output_root = Path(output_root)
        self.fundamentals_db = Path(fundamentals_db)
        self.policy = policy
        self.accounting_policy = accounting_policy
        self.cohort = cohort

    def run(
        self,
        *,
        as_of_date: date,
        screen_exports: dict[int, Path] | None = None,
        universe_run_id: str | None = None,
        screener_client: ScreenerClient | None = None,
    ) -> dict[str, Any]:
        started_at = datetime.now(UTC)
        supplied = screen_exports or {}
        governed_ids = {int(row["screen_id"]) for row in self.policy.raw["screens"]}
        if supplied and set(supplied) != governed_ids:
            raise ValueError("provided exports must cover all four governed V2 screen IDs")
        frozen = [
            self._freeze_screen(row, supplied.get(int(row["screen_id"])), screener_client)
            for row in self.policy.raw["screens"]
        ]
        try:
            parsed = {item.screen_id: parse_screen_export(item.path) for item in frozen}
            union = _merge_screen_rows(parsed, self.policy.raw["screens"])
            official_run, official = self._official_context(
                as_of_date=as_of_date, universe_run_id=universe_run_id,
            )
            baseline = _baseline_by_isin(self.cohort)
            _add_baseline_members(union, baseline)
            history = ScreenerHistoryReader(
                self.fundamentals_db, self.accounting_policy, as_of_date=as_of_date,
            )
            history_symbols = set(history.symbols())
            candidates = self._evaluate(
                union, baseline=baseline, official=official, history=history,
                history_symbols=history_symbols,
            )
            candidates.sort(key=lambda row: row["candidate_key"])
            _assign_primary_queue(candidates, self.policy.raw)
            artifacts = self._artifacts(frozen, parsed, as_of_date=as_of_date)
            snapshot_hash = content_hash({
                "as_of_date": as_of_date,
                "universe_run_id": official_run,
                "policy_hash": self.policy.policy_hash,
                "cohort_hash": content_hash(self.cohort),
                "screens": [artifact["content_hash"] for artifact in artifacts],
                "decisions": [row["decision_hash"] for row in candidates],
            })
            run_id = f"jcurve-discovery-{as_of_date}-{snapshot_hash[:16]}"
            output_dir = self.output_root / run_id
            if self.store.completed_discovery_run(run_id) and output_dir.is_dir():
                return {
                    "run_id": run_id, "status": "COMPLETED", "reused": True,
                    "output_dir": str(output_dir),
                }
            if output_dir.exists():
                raise FileExistsError(f"incomplete J-curve discovery output exists: {output_dir}")
            output_dir.mkdir(parents=True)
            try:
                for item, artifact in zip(frozen, artifacts, strict=True):
                    name = f"screener-screen-{item.screen_id}{detect_screen_export_suffix(item.raw)}"
                    (output_dir / name).write_bytes(item.raw)
                    artifact["metadata"]["local_path"] = name
                result = self._result(
                    run_id=run_id, output_dir=output_dir, universe_run_id=official_run,
                    snapshot_hash=snapshot_hash, candidates=candidates,
                )
                payload = {
                    **result,
                    "as_of_date": as_of_date,
                    "policy_hash": self.policy.policy_hash,
                    "cohort_hash": content_hash(self.cohort),
                    "started_at": started_at,
                    "artifacts": artifacts,
                    "screens": [
                        {
                            "screen_id": item.screen_id, "lane": item.lane,
                            "screen_url": item.url, "query_text": item.query_text,
                            "query_hash": content_hash(item.query_text),
                            "source_artifact_id": artifact["artifact_id"],
                            "source_row_count": len(parsed[item.screen_id]),
                            "acquisition_mode": item.acquisition_mode,
                        }
                        for item, artifact in zip(frozen, artifacts, strict=True)
                    ],
                    "candidates": candidates,
                }
                self._write_outputs(output_dir, payload)
                self.store.persist_discovery(payload)
                return result
            except Exception:
                shutil.rmtree(output_dir)
                raise
        finally:
            for item in frozen:
                if item.temporary:
                    item.path.unlink(missing_ok=True)

    def _freeze_screen(
        self,
        config: dict[str, Any],
        supplied: Path | None,
        client: ScreenerClient | None,
    ) -> FrozenScreen:
        screen_id = int(config["screen_id"])
        if supplied is not None:
            path = Path(supplied)
            if not path.is_file() or path.stat().st_size == 0:
                raise FileNotFoundError(f"screen export not found or empty: {path}")
            return FrozenScreen(
                screen_id, str(config["lane"]), str(config["url"]),
                str(config["expected_query"]), path, path.read_bytes(),
                datetime.fromtimestamp(path.stat().st_mtime, tz=UTC), "PROVIDED_EXPORT", False,
            )
        screener = client or ScreenerClient()
        with tempfile.NamedTemporaryFile(
            prefix=f"jcurve-v2-{screen_id}-", suffix=".download", delete=False,
        ) as handle:
            path = Path(handle.name)
        try:
            result = screener.download_screen_export(
                screen_id, destination=path, screen_url=str(config["url"]),
                expected_query_terms=tuple(config["required_query_terms"]),
            )
            if _canonical_query(result.query_text) != _canonical_query(config["expected_query"]):
                raise RuntimeError(f"Screener screen {screen_id} query drifted from V2 policy")
            return FrozenScreen(
                screen_id, str(config["lane"]), str(config["url"]), result.query_text,
                path, path.read_bytes(), datetime.now(UTC),
                "PLAYWRIGHT_AUTHENTICATED_EXPORT", True,
            )
        except Exception:
            path.unlink(missing_ok=True)
            raise

    def _official_context(
        self, *, as_of_date: date, universe_run_id: str | None,
    ) -> tuple[str, dict[str, Any]]:
        conn = duckdb.connect(str(self.store_path), read_only=True)
        try:
            if universe_run_id is None:
                row = conn.execute(
                    """SELECT run_id FROM screening_run
                       WHERE run_mode = 'full_universe' AND status = 'COMPLETED'
                         AND as_of_date <= ?
                       ORDER BY as_of_date DESC, ended_at DESC, run_id DESC LIMIT 1""",
                    [as_of_date],
                ).fetchone()
                if not row:
                    raise ValueError(f"no completed full_universe run exists at or before {as_of_date}")
                universe_run_id = str(row[0])
            run = conn.execute(
                """SELECT as_of_date, min_market_cap_cr, max_market_cap_cr FROM screening_run
                   WHERE run_id = ? AND run_mode = 'full_universe' AND status = 'COMPLETED'""",
                [universe_run_id],
            ).fetchone()
            if not run or run[0] > as_of_date:
                raise ValueError("universe run must be completed and point-in-time valid")
            gate = self.policy.raw["universe_gate"]
            if (
                float(run[1]) != float(gate["market_cap_min_cr"])
                or float(run[2]) != float(gate["market_cap_max_cr"])
            ):
                raise ValueError("full_universe parent market-cap bounds do not match V2 policy")
            universe_rows = conn.execute(
                """SELECT u.company_id, u.security_id, u.market_cap_status, u.identity_status
                   FROM universe_member u JOIN universe_snapshot s
                     ON s.universe_snapshot_id = u.universe_snapshot_id
                   WHERE s.run_id = ?""",
                [universe_run_id],
            ).fetchall()
            identity_rows = conn.execute(
                """SELECT c.company_id, c.legal_name, s.security_id, s.isin,
                          max(CASE WHEN l.exchange = 'NSE' THEN l.symbol END) AS nse_symbol,
                          max(CASE WHEN l.exchange = 'BSE' THEN l.bse_code END) AS bse_code,
                          max(CASE WHEN l.exchange = 'NSE' THEN l.listing_id END) AS nse_listing_id,
                          max(CASE WHEN l.exchange = 'BSE' THEN l.listing_id END) AS bse_listing_id
                   FROM security_master s JOIN company_master c ON c.company_id = s.company_id
                   LEFT JOIN listing_master l ON l.security_id = s.security_id
                     AND l.valid_from <= ? AND (l.valid_to IS NULL OR l.valid_to >= ?)
                   WHERE s.valid_from <= ? AND (s.valid_to IS NULL OR s.valid_to >= ?)
                     AND c.valid_from <= ? AND (c.valid_to IS NULL OR c.valid_to >= ?)
                   GROUP BY c.company_id, c.legal_name, s.security_id, s.isin""",
                [as_of_date, as_of_date, as_of_date, as_of_date, as_of_date, as_of_date],
            ).fetchall()
        finally:
            conn.close()
        universe = {
            str(row[0]): {
                "security_id": str(row[1]), "market_cap_status": str(row[2]),
                "identity_status": str(row[3]),
            }
            for row in universe_rows
        }
        identities = [
            {
                "company_id": str(row[0]), "legal_name": str(row[1]),
                "security_id": str(row[2]), "isin": str(row[3]) if row[3] else None,
                "nse_symbol": str(row[4]) if row[4] else None,
                "bse_code": str(row[5]) if row[5] else None,
                "listing_id": str(row[6] or row[7]) if (row[6] or row[7]) else None,
            }
            for row in identity_rows
        ]
        maps: dict[str, dict[str, list[dict[str, Any]]]] = {
            key: {} for key in ("isin", "nse_symbol", "bse_code")
        }
        for identity in identities:
            for key in maps:
                value = identity.get(key)
                if value:
                    maps[key].setdefault(str(value).upper(), []).append(identity)
        return universe_run_id, {"universe": universe, "maps": maps}

    def _evaluate(
        self,
        union: dict[str, dict[str, Any]],
        *,
        baseline: dict[str, dict[str, Any]],
        official: dict[str, Any],
        history: ScreenerHistoryReader,
        history_symbols: set[str],
    ) -> list[dict[str, Any]]:
        required_lanes = set(self.policy.raw["focus_gate"]["required_any_lanes"])
        minimum_matches = int(self.policy.raw["focus_gate"]["minimum_screen_matches"])
        rows: list[dict[str, Any]] = []
        for candidate in union.values():
            identity, identity_status = _resolve_identity(candidate, official["maps"])
            if identity:
                candidate.update(identity)
            company_id = candidate.get("company_id")
            universe_member = official["universe"].get(company_id)
            if universe_member and universe_member["identity_status"] == "RESOLVED":
                universe_status = str(universe_member["market_cap_status"])
            elif identity_status == "RESOLVED":
                universe_status = "NOT_IN_UNIVERSE"
            else:
                universe_status = "UNRESOLVED"
            isin = candidate.get("isin")
            baseline_member = baseline.get(str(isin)) if isin else None
            lanes = sorted(candidate["matched_lanes"])
            screens = sorted(candidate["matched_screen_ids"])
            focus = (
                identity_status == "RESOLVED"
                and universe_status == self.policy.raw["universe_gate"]["required_market_cap_status"]
                and len(screens) >= minimum_matches
                and bool(set(lanes) & required_lanes)
            )
            classification_symbol = (
                candidate.get("nse_symbol") or candidate.get("screen_nse_symbol")
                or candidate.get("bse_code") or candidate.get("screen_bse_code")
            )
            should_evaluate = focus or bool(baseline_member)
            if should_evaluate and classification_symbol in history_symbols:
                decision = history.classify(
                    classification_symbol, screen_member=bool(screens),
                    supplemental_member=bool(baseline_member),
                )
            elif should_evaluate:
                decision = {
                    "statement_basis": None,
                    "basis_resolution_reason": "screener_fundamentals_history_unavailable",
                    "disposition": "HISTORY_UNAVAILABLE",
                    "reason_codes": ["SCREENER_FUNDAMENTALS_HISTORY_UNAVAILABLE"],
                    "metrics": {"history_available": False},
                }
            else:
                decision = {
                    "statement_basis": None, "basis_resolution_reason": None,
                    "disposition": None, "reason_codes": [], "metrics": {},
                }
            row = {
                "candidate_key": _candidate_key(candidate),
                "company_name": candidate.get("legal_name") or candidate.get("company_name"),
                "isin": candidate.get("isin"),
                "nse_symbol": candidate.get("nse_symbol") or candidate.get("screen_nse_symbol"),
                "bse_code": candidate.get("bse_code") or candidate.get("screen_bse_code"),
                "company_id": company_id,
                "security_id": candidate.get("security_id"),
                "listing_id": candidate.get("listing_id"),
                "identity_status": identity_status,
                "matched_screen_ids": screens,
                "matched_lanes": lanes,
                "screen_match_count": len(screens),
                "baseline_member": bool(baseline_member),
                "baseline_case_role": baseline_member.get("case_role") if baseline_member else None,
                "universe_status": universe_status,
                "focus_eligible": focus,
                "statement_basis": decision.get("statement_basis"),
                "basis_resolution_reason": decision.get("basis_resolution_reason"),
                "accounting_disposition": decision.get("disposition"),
                "reason_codes": list(decision.get("reason_codes", [])),
                "metrics": dict(decision.get("metrics", {})),
                "queue_disposition": _routing_disposition(
                    identity_status=identity_status, universe_status=universe_status,
                    focus=focus, accounting=decision.get("disposition"),
                    routing=self.policy.raw["routing"],
                ),
                "queue_rank": None,
            }
            row["decision_hash"] = content_hash(row)
            rows.append(row)
        return rows

    def _artifacts(
        self, frozen: list[FrozenScreen], parsed: dict[int, list[dict[str, Any]]], *, as_of_date: date,
    ) -> list[dict[str, Any]]:
        artifacts = []
        for item in frozen:
            digest = hashlib.sha256(item.raw).hexdigest()
            artifact_id = f"artifact:jcurve-discovery-screen:{item.screen_id}:{digest[:20]}"
            artifacts.append({
                "artifact_id": artifact_id,
                "ingestion_run_id": f"ingest:jcurve-discovery:{artifact_id}",
                "source_key": "screener_screen_export", "provider": "SCREENER",
                "source_url": item.url,
                "local_dataset_id": f"screener.screen:{item.screen_id}:{as_of_date}",
                "effective_date": as_of_date, "published_at": None,
                "retrieved_at": item.retrieved_at, "content_hash": digest,
                "byte_count": len(item.raw), "row_count": len(parsed[item.screen_id]),
                "parser_version": self.policy.version,
                "schema_version": "jcurve-screener-discovery-v2",
                "validation_status": "VALID", "parent_artifact_id": None,
                "metadata": {
                    "screen_id": item.screen_id, "lane": item.lane,
                    "query_hash": content_hash(item.query_text),
                    "acquisition_mode": item.acquisition_mode,
                },
            })
        return artifacts

    def _result(
        self, *, run_id: str, output_dir: Path, universe_run_id: str,
        snapshot_hash: str, candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        union_rows = [row for row in candidates if row["matched_screen_ids"]]
        return {
            "run_id": run_id, "status": "COMPLETED", "reused": False,
            "output_dir": str(output_dir), "policy_version": self.policy.version,
            "cohort_version": self.cohort["cohort_version"],
            "universe_run_id": universe_run_id, "snapshot_hash": snapshot_hash,
            "union_count": len(union_rows),
            "universe_eligible_count": sum(
                row["universe_status"] == "ELIGIBLE" for row in union_rows
            ),
            "focus_count": sum(row["focus_eligible"] for row in candidates),
            "evaluated_count": sum(row["accounting_disposition"] is not None for row in candidates),
            "primary_queue_count": sum(row["queue_disposition"] == "PRIMARY_RESEARCH" for row in candidates),
            "watchlist_count": sum(row["queue_disposition"] == "WATCHLIST" for row in candidates),
            "manual_triage_count": sum(row["queue_disposition"] == "MANUAL_TRIAGE" for row in candidates),
        }

    @staticmethod
    def _write_outputs(output_dir: Path, payload: dict[str, Any]) -> None:
        result_keys = (
            "run_id", "status", "policy_version", "cohort_version", "universe_run_id",
            "snapshot_hash", "union_count", "universe_eligible_count", "focus_count",
            "evaluated_count", "primary_queue_count", "watchlist_count", "manual_triage_count",
        )
        result = {key: payload[key] for key in result_keys}
        (output_dir / "result.json").write_text(
            json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8",
        )
        (output_dir / "manifest.json").write_text(
            json.dumps({
                **result, "policy_hash": payload["policy_hash"],
                "cohort_hash": payload["cohort_hash"], "screens": payload["screens"],
                "decision_hashes": [row["decision_hash"] for row in payload["candidates"]],
            }, indent=2, default=str) + "\n", encoding="utf-8",
        )
        fields = [
            "candidate_key", "company_name", "isin", "nse_symbol", "bse_code",
            "company_id", "identity_status", "matched_screen_ids", "matched_lanes",
            "screen_match_count", "baseline_member", "baseline_case_role", "universe_status",
            "focus_eligible", "statement_basis", "accounting_disposition",
            "queue_disposition", "queue_rank", "reason_codes", "metrics", "decision_hash",
        ]
        _write_csv(output_dir / "candidates.csv", payload["candidates"], fields)
        _write_csv(
            output_dir / "primary_queue.csv",
            sorted(
                (row for row in payload["candidates"] if row["queue_disposition"] == "PRIMARY_RESEARCH"),
                key=lambda row: int(row["queue_rank"]),
            ),
            fields,
        )


def _merge_screen_rows(
    parsed: dict[int, list[dict[str, Any]]], screen_config: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    lanes = {int(row["screen_id"]): str(row["lane"]) for row in screen_config}
    groups: dict[str, dict[str, Any]] = {}
    token_to_key: dict[str, str] = {}
    for screen_id in sorted(parsed):
        for row in parsed[screen_id]:
            tokens = _identity_tokens(row)
            existing = sorted({token_to_key[token] for token in tokens if token in token_to_key})
            key = existing[0] if existing else tokens[0]
            group = groups.setdefault(key, _empty_group(row))
            for duplicate in existing[1:]:
                other = groups.pop(duplicate)
                _combine_groups(group, other)
                for token, mapped in list(token_to_key.items()):
                    if mapped == duplicate:
                        token_to_key[token] = key
            _combine_row(group, row)
            group["matched_screen_ids"].add(screen_id)
            group["matched_lanes"].add(lanes[screen_id])
            for token in tokens:
                token_to_key[token] = key
    return groups


def _identity_tokens(row: dict[str, Any]) -> list[str]:
    tokens = []
    for prefix, field in (("ISIN", "isin"), ("NSE", "nse_symbol"), ("BSE", "bse_code")):
        if row.get(field):
            tokens.append(f"{prefix}:{str(row[field]).upper()}")
    if not tokens:
        tokens.append(f"SYMBOL:{row['symbol']}")
    return tokens


def _empty_group(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "company_name": row.get("company_name"), "isin": row.get("isin"),
        "screen_nse_symbol": row.get("nse_symbol"), "screen_bse_code": row.get("bse_code"),
        "matched_screen_ids": set(), "matched_lanes": set(),
    }


def _combine_row(group: dict[str, Any], row: dict[str, Any]) -> None:
    for target, source in (
        ("company_name", "company_name"), ("isin", "isin"),
        ("screen_nse_symbol", "nse_symbol"), ("screen_bse_code", "bse_code"),
    ):
        group[target] = group.get(target) or row.get(source)


def _combine_groups(target: dict[str, Any], source: dict[str, Any]) -> None:
    _combine_row(target, {
        "company_name": source.get("company_name"), "isin": source.get("isin"),
        "nse_symbol": source.get("screen_nse_symbol"), "bse_code": source.get("screen_bse_code"),
    })
    target["matched_screen_ids"].update(source["matched_screen_ids"])
    target["matched_lanes"].update(source["matched_lanes"])


def _baseline_by_isin(cohort: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row["isin"]): row for row in cohort["members"]}


def _add_baseline_members(
    union: dict[str, dict[str, Any]], baseline: dict[str, dict[str, Any]],
) -> None:
    by_isin = {str(row.get("isin")): row for row in union.values() if row.get("isin")}
    for isin, member in baseline.items():
        if isin in by_isin:
            continue
        union[f"ISIN:{isin}"] = {
            "company_name": member["legal_name"], "isin": isin,
            "screen_nse_symbol": member.get("nse_symbol"),
            "screen_bse_code": member.get("bse_code"),
            "matched_screen_ids": set(), "matched_lanes": set(),
        }


def _resolve_identity(
    candidate: dict[str, Any], maps: dict[str, dict[str, list[dict[str, Any]]]],
) -> tuple[dict[str, Any] | None, str]:
    for field, value in (
        ("isin", candidate.get("isin")),
        ("nse_symbol", candidate.get("screen_nse_symbol")),
        ("bse_code", candidate.get("screen_bse_code")),
    ):
        if not value:
            continue
        matches = maps[field].get(str(value).upper(), [])
        unique = {row["company_id"]: row for row in matches}
        if len(unique) == 1:
            return next(iter(unique.values())), "RESOLVED"
        if len(unique) > 1:
            return None, "AMBIGUOUS"
    return None, "UNRESOLVED"


def _candidate_key(candidate: dict[str, Any]) -> str:
    for prefix, field in (
        ("ISIN", "isin"), ("COMPANY", "company_id"),
        ("NSE", "screen_nse_symbol"), ("BSE", "screen_bse_code"),
    ):
        if candidate.get(field):
            return f"{prefix}:{candidate[field]}"
    return f"UNKNOWN:{content_hash(candidate)[:24]}"


def _routing_disposition(
    *, identity_status: str, universe_status: str, focus: bool,
    accounting: str | None, routing: dict[str, str],
) -> str:
    if identity_status != "RESOLVED":
        return "IDENTITY_UNRESOLVED"
    if universe_status != "ELIGIBLE":
        return "UNIVERSE_EXCLUDED"
    if not focus:
        return "SCREEN_FILTERED"
    return routing.get(str(accounting), "DROP")


def _assign_primary_queue(candidates: list[dict[str, Any]], policy: dict[str, Any]) -> None:
    eligible = set(policy["primary_queue"]["eligible_dispositions"])
    disposition_order = {
        value: index for index, value in enumerate(policy["primary_queue"]["disposition_order"])
    }
    screen_by_lane = {str(row["lane"]): int(row["screen_id"]) for row in policy["screens"]}
    ramp_id = screen_by_lane["RAMP"]
    commissioning_id = screen_by_lane["COMMISSIONING"]
    ranked = sorted(
        (row for row in candidates if row["accounting_disposition"] in eligible and row["focus_eligible"]),
        key=lambda row: (
            disposition_order[row["accounting_disposition"]],
            -row["screen_match_count"],
            -(ramp_id in row["matched_screen_ids"]),
            -(commissioning_id in row["matched_screen_ids"]),
            row.get("isin") or row["candidate_key"],
        ),
    )
    limit = int(policy["primary_queue"]["limit"])
    for rank, row in enumerate(ranked, start=1):
        row["queue_rank"] = rank
        if rank <= limit:
            row["queue_disposition"] = "PRIMARY_RESEARCH"
        else:
            row["queue_disposition"] = str(policy["primary_queue"]["overflow_disposition"])
        row["decision_hash"] = content_hash({
            key: value for key, value in row.items() if key != "decision_hash"
        })


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: json.dumps(row[key], sort_keys=True, default=str)
                if key in {"matched_screen_ids", "matched_lanes", "reason_codes", "metrics"}
                else row.get(key)
                for key in fields
            })
