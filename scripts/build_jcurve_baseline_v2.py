#!/usr/bin/env python3
"""Build the explicit J-curve V2 calibration cohort from frozen screen exports."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.domains.research_screener.jcurve.screener_seed import (
    parse_screen_export,
)


SCREEN_ROLES = {
    3901581: "screen1_only_build",
    3901588: "screen2_only_older_capex",
}
STRATA = (
    "screen1_only_build",
    "screen2_only_older_capex",
    "screen3_led_commissioning",
    "screen4_led_ramp",
    "multi_screen_commissioning_ramp",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-v1", required=True, type=Path)
    parser.add_argument("--screen-export", action="append", required=True, type=_screen_arg)
    parser.add_argument("--research-store", required=True, type=Path)
    parser.add_argument("--fundamentals-db", required=True, type=Path)
    parser.add_argument("--universe-run-id", required=True)
    parser.add_argument("--identity-checked-as-of", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    payload = build(args)
    print(json.dumps(payload, indent=2) + "\n", end="")
    return 0


def build(args: argparse.Namespace) -> dict[str, Any]:
    screens = dict(args.screen_export)
    if set(screens) != {3901581, 3901588, 3901589, 3901592}:
        raise ValueError("all four governed V2 screen exports are required")
    baseline_v1 = json.loads(args.baseline_v1.read_text(encoding="utf-8"))
    membership: dict[str, set[int]] = defaultdict(set)
    metadata: dict[str, dict[str, Any]] = {}
    for screen_id, path in screens.items():
        frame = pd.read_csv(path, dtype={"ISIN Code": "string"})
        industry_by_isin = {
            str(row["ISIN Code"]).strip(): str(row.get("Industry Group") or "Unknown").strip()
            for _, row in frame.iterrows()
            if pd.notna(row.get("ISIN Code"))
        }
        for row in parse_screen_export(path):
            if not row["isin"]:
                continue
            membership[row["isin"]].add(screen_id)
            metadata[row["isin"]] = row | {
                "industry": industry_by_isin.get(row["isin"], "Unknown")
            }
    available = _history_symbols(args.fundamentals_db)
    eligible, identities = _eligible_identities(
        args.research_store,
        universe_run_id=args.universe_run_id,
        as_of_date=args.identity_checked_as_of,
    )
    existing_isins = {str(row["isin"]) for row in baseline_v1["members"]}
    pools: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for isin, matched_screens in membership.items():
        role = _role(matched_screens)
        row = metadata[isin]
        identity = identities.get(isin)
        if (
            not role
            or isin in existing_isins
            or isin not in eligible
            or identity is None
            or not identity["nse_symbol"]
            or row["symbol"] not in available
        ):
            continue
        pools[role].append({
            "legal_name": identity["legal_name"],
            "isin": isin,
            "nse_symbol": identity["nse_symbol"],
            "bse_code": identity["bse_code"],
            "aliases": [],
            "selection_tags": [
                _tag(row["industry"]),
                role.replace("_", "-"),
            ],
            "case_role": role,
            "label_state": "UNLABELED",
            "selection_source": "SCREENER_STRATIFIED_CHALLENGE",
            "matched_screen_ids": sorted(matched_screens),
        })
    challenge_members: list[dict[str, Any]] = []
    for role in STRATA:
        selected = _diverse_sample(pools[role], count=15)
        if len(selected) != 15:
            raise ValueError(f"stratum {role} has only {len(selected)} eligible cases")
        challenge_members.extend(selected)
    anchors = [
        row | {
            "case_role": "known_anchor",
            "label_state": "UNLABELED",
            "selection_source": "CURATED_CAPEX_BASELINE_V1",
            "matched_screen_ids": sorted(membership.get(str(row["isin"]), set())),
        }
        for row in baseline_v1["members"]
    ]
    members = anchors + challenge_members
    return {
        "cohort_version": "jcurve-capex-baseline-v2",
        "identity_checked_as_of": str(args.identity_checked_as_of),
        "purpose": (
            "100-company J-curve calibration cohort: 25 known anchors and 75 "
            "unlabeled stratified challenges. Membership is not a positive label."
        ),
        "required_company_count": 100,
        "selection_policy": {
            "policy_version": "jcurve-baseline-selection-v2",
            "source_universe_run_id": args.universe_run_id,
            "required_market_cap_status": "ELIGIBLE",
            "required_local_history": True,
            "anchor_count": 25,
            "challenge_count_per_stratum": 15,
            "strata": list(STRATA),
            "sampling": "DETERMINISTIC_INDUSTRY_ROUND_ROBIN_SHA256",
        },
        "members": members,
    }


def _role(screens: set[int]) -> str | None:
    if len(screens) == 1:
        return SCREEN_ROLES.get(next(iter(screens)))
    if 3901589 in screens and 3901592 not in screens:
        return "screen3_led_commissioning"
    if 3901592 in screens and 3901589 not in screens:
        return "screen4_led_ramp"
    if 3901589 in screens and 3901592 in screens and len(screens) >= 3:
        return "multi_screen_commissioning_ramp"
    return None


def _diverse_sample(rows: list[dict[str, Any]], *, count: int) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["selection_tags"][0]].append(row)
    for group in grouped.values():
        group.sort(key=lambda row: _hash(f"jcurve-baseline-v2|{row['isin']}"))
    industries = sorted(grouped, key=lambda value: _hash(f"industry|{value}"))
    selected: list[dict[str, Any]] = []
    while len(selected) < count:
        progressed = False
        for industry in industries:
            if grouped[industry] and len(selected) < count:
                selected.append(grouped[industry].pop(0))
                progressed = True
        if not progressed:
            break
    return selected


def _history_symbols(path: Path) -> set[str]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return {
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT upper(trim(symbol)) FROM screener_financials"
            ).fetchall()
        }
    finally:
        conn.close()


def _eligible_identities(
    path: Path, *, universe_run_id: str, as_of_date: date,
) -> tuple[set[str], dict[str, dict[str, str | None]]]:
    conn = duckdb.connect(str(path), read_only=True)
    try:
        eligible = {
            str(row[0])
            for row in conn.execute(
                """
                SELECT sec.isin
                FROM universe_member u
                JOIN universe_snapshot us
                  ON us.universe_snapshot_id = u.universe_snapshot_id
                JOIN security_master sec ON sec.security_id = u.security_id
                WHERE us.run_id = ? AND u.market_cap_status = 'ELIGIBLE'
                  AND u.identity_status = 'RESOLVED'
                """,
                [universe_run_id],
            ).fetchall()
        }
        rows = conn.execute(
            """
            SELECT c.legal_name, s.isin,
                   max(CASE WHEN l.exchange = 'NSE' THEN l.symbol END) AS nse_symbol,
                   max(CASE WHEN l.exchange = 'BSE' THEN l.bse_code END) AS bse_code
            FROM security_master s
            JOIN company_master c ON c.company_id = s.company_id
            LEFT JOIN listing_master l ON l.security_id = s.security_id
              AND l.valid_from <= ? AND (l.valid_to IS NULL OR l.valid_to >= ?)
            WHERE s.isin IN (SELECT unnest(?))
            GROUP BY c.legal_name, s.isin
            """,
            [as_of_date, as_of_date, sorted(eligible)],
        ).fetchall()
    finally:
        conn.close()
    identities = {
        str(row[1]): {
            "legal_name": str(row[0]),
            "nse_symbol": str(row[2]) if row[2] else None,
            "bse_code": str(row[3]) if row[3] else None,
        }
        for row in rows
    }
    return eligible, identities


def _screen_arg(value: str) -> tuple[int, Path]:
    screen_id, separator, path = value.partition("=")
    if not separator:
        raise argparse.ArgumentTypeError("screen export must be SCREEN_ID=PATH")
    return int(screen_id), Path(path)


def _tag(value: str) -> str:
    return "-".join(str(value).casefold().replace("&", " and ").split())


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
