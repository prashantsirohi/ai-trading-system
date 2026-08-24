from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.domains.research_screener.store import content_hash


@dataclass(frozen=True)
class ResolvedCohort:
    version: str
    policy_hash: str
    raw: dict[str, Any]
    company_ids: tuple[str, ...]
    members: tuple[dict[str, Any], ...]


class BaselineCohort:
    """Versioned capex cohort resolved through exact point-in-time identifiers."""

    REQUIRED_MEMBER_FIELDS = {
        "legal_name", "isin", "nse_symbol", "bse_code", "aliases", "selection_tags",
    }
    V2_MEMBER_FIELDS = {
        "case_role", "label_state", "selection_source", "matched_screen_ids",
    }

    def __init__(self, raw: dict[str, Any]):
        self.raw = raw
        self._validate()
        self.version = str(raw["cohort_version"])
        self.policy_hash = content_hash(raw)

    @classmethod
    def load(cls, path: Path) -> "BaselineCohort":
        return cls(json.loads(Path(path).read_text(encoding="utf-8")))

    def resolve(self, *, store_path: Path, as_of_date: date) -> ResolvedCohort:
        identity_checked_as_of = date.fromisoformat(str(self.raw["identity_checked_as_of"]))
        if as_of_date < identity_checked_as_of:
            raise ValueError(
                "baseline identity snapshot is later than the requested research cutoff"
            )
        conn = duckdb.connect(str(store_path), read_only=True)
        resolved: list[dict[str, Any]] = []
        try:
            for member in self.raw["members"]:
                rows = conn.execute(
                    """SELECT DISTINCT c.company_id, s.security_id, c.legal_name,
                                      n.listing_id, n.symbol, b.listing_id, b.bse_code
                       FROM company_master c
                       JOIN security_master s ON s.company_id = c.company_id
                       LEFT JOIN listing_master n ON n.security_id = s.security_id
                            AND n.exchange = 'NSE' AND upper(coalesce(n.symbol, '')) = ?
                            AND n.valid_from <= ? AND (n.valid_to IS NULL OR n.valid_to >= ?)
                       LEFT JOIN listing_master b ON b.security_id = s.security_id
                            AND b.exchange = 'BSE' AND coalesce(b.bse_code, '') = ?
                            AND b.valid_from <= ? AND (b.valid_to IS NULL OR b.valid_to >= ?)
                       WHERE s.isin = ?
                         AND c.valid_from <= ? AND (c.valid_to IS NULL OR c.valid_to >= ?)
                         AND s.valid_from <= ? AND (s.valid_to IS NULL OR s.valid_to >= ?)""",
                    [
                        member["nse_symbol"], as_of_date, as_of_date,
                        member["bse_code"], as_of_date, as_of_date,
                        member["isin"], as_of_date, as_of_date, as_of_date, as_of_date,
                    ],
                ).fetchall()
                valid = [row for row in rows if row[3] is not None and row[5] is not None]
                if len(valid) != 1:
                    raise ValueError(
                        f"baseline member {member['nse_symbol']} did not resolve to one exact dual listing"
                    )
                row = valid[0]
                if str(row[2]).strip().casefold() != str(member["legal_name"]).strip().casefold():
                    raise ValueError(f"baseline legal-name mismatch for {member['nse_symbol']}")
                resolved.append(member | {
                    "company_id": str(row[0]), "security_id": str(row[1]),
                    "nse_listing_id": str(row[3]), "bse_listing_id": str(row[5]),
                })
        finally:
            conn.close()
        company_ids = tuple(sorted({str(row["company_id"]) for row in resolved}))
        if len(company_ids) != int(self.raw["required_company_count"]):
            raise ValueError("baseline members do not resolve to the required number of unique companies")
        return ResolvedCohort(
            version=self.version, policy_hash=self.policy_hash, raw=self.raw,
            company_ids=company_ids, members=tuple(resolved),
        )

    def _validate(self) -> None:
        required = {
            "cohort_version", "identity_checked_as_of", "purpose",
            "required_company_count", "members",
        }
        allowed = required | {"selection_policy"}
        if not required <= set(self.raw) or not set(self.raw) <= allowed:
            raise ValueError("baseline cohort has an invalid top-level shape")
        is_v2 = str(self.raw["cohort_version"]) == "jcurve-capex-baseline-v2"
        if is_v2 and "selection_policy" not in self.raw:
            raise ValueError("V2 baseline must include its selection policy")
        try:
            date.fromisoformat(str(self.raw["identity_checked_as_of"]))
        except ValueError as exc:
            raise ValueError("baseline identity_checked_as_of must be an ISO date") from exc
        members = self.raw["members"]
        required_count = int(self.raw["required_company_count"])
        if not isinstance(members, list) or len(members) != required_count:
            raise ValueError("baseline cohort member count does not match required_company_count")
        for member in members:
            expected_fields = (
                self.REQUIRED_MEMBER_FIELDS | self.V2_MEMBER_FIELDS
                if is_v2 else self.REQUIRED_MEMBER_FIELDS
            )
            if set(member) != expected_fields:
                raise ValueError("baseline cohort member has an invalid shape")
            if not str(member["isin"]).startswith("INE") or len(str(member["isin"])) != 12:
                raise ValueError(f"invalid baseline ISIN: {member['isin']}")
            if not str(member["nse_symbol"]).strip() or not str(member["bse_code"]).isdigit():
                raise ValueError(f"invalid baseline listing identifiers: {member['legal_name']}")
            if not isinstance(member["aliases"], list) or not isinstance(member["selection_tags"], list):
                raise ValueError(f"invalid baseline metadata: {member['legal_name']}")
            if is_v2:
                if member["label_state"] != "UNLABELED":
                    raise ValueError("V2 cohort membership must not embed an unaudited label")
                if not isinstance(member["matched_screen_ids"], list) or not all(
                    isinstance(value, int) for value in member["matched_screen_ids"]
                ):
                    raise ValueError("V2 matched_screen_ids must be an integer list")
        for field in ("isin", "nse_symbol", "bse_code"):
            values = [str(member[field]).upper() for member in members]
            if len(values) != len(set(values)):
                raise ValueError(f"baseline cohort contains duplicate {field} values")


class SeedRunCohort:
    """Completed Screener seed candidates exposed through the import cohort contract."""

    def __init__(self, *, run_id: str):
        self.run_id = str(run_id)

    def resolve(self, *, store_path: Path, as_of_date: date) -> ResolvedCohort:
        from .store import JCurveStore

        seed = JCurveStore(store_path).load_seed_cohort(self.run_id)
        if as_of_date < seed["as_of_date"]:
            raise ValueError("seed run is later than the requested research cutoff")
        raw = {
            "cohort_version": seed["version"],
            "identity_checked_as_of": str(seed["as_of_date"]),
            "purpose": "Screener-derived J-curve discovery seed; official evidence required",
            "required_company_count": len(seed["company_ids"]),
            "seed_run_id": self.run_id,
            "seed_snapshot_hash": seed["snapshot_hash"],
            "members": list(seed["members"]),
        }
        return ResolvedCohort(
            version=seed["version"],
            policy_hash=seed["policy_hash"],
            raw=raw,
            company_ids=seed["company_ids"],
            members=seed["members"],
        )


class DiscoveryRunCohort:
    """The bounded primary queue from a completed V2 discovery run."""

    def __init__(self, *, run_id: str):
        self.run_id = str(run_id)

    def resolve(self, *, store_path: Path, as_of_date: date) -> ResolvedCohort:
        from .store import JCurveStore

        discovery = JCurveStore(store_path).load_discovery_cohort(self.run_id)
        if as_of_date < discovery["as_of_date"]:
            raise ValueError("discovery run is later than the requested research cutoff")
        raw = {
            "cohort_version": discovery["version"],
            "identity_checked_as_of": str(discovery["as_of_date"]),
            "purpose": "Bounded J-curve V2 primary queue; official evidence required",
            "required_company_count": len(discovery["company_ids"]),
            "discovery_run_id": self.run_id,
            "discovery_snapshot_hash": discovery["snapshot_hash"],
            "members": list(discovery["members"]),
        }
        return ResolvedCohort(
            version=discovery["version"], policy_hash=discovery["policy_hash"],
            raw=raw, company_ids=discovery["company_ids"], members=discovery["members"],
        )


__all__ = ["BaselineCohort", "DiscoveryRunCohort", "ResolvedCohort", "SeedRunCohort"]
