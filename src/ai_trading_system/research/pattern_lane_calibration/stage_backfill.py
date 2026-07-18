"""Point-in-time historical weekly-stage backfill.

Reconstructs governed weekly-stage observations for completed weeks by
running the production stage-coverage builder
(``domains/opportunities/coverage.build_stage_coverage``, classifier policy
``weekly-stage-v2``) over daily bars truncated to each week end, with
``lock_current_week=True`` so only completed (locked) weeks are emitted.

The workflow is a two-step protocol:

- ``compute``: runs the full pipeline against a **frozen snapshot** of the
  OHLCV store and writes report artifacts only (observations CSV, coverage
  report with raw/eligible/decision denominators, conflicts vs the live
  governed table, manifest). No database is modified. The manifest binds the
  snapshot hash, source identity, code commit, policy version/hash, and a
  canonical content hash over the deterministic projection (grain + label +
  transition + score + input hash + source bar date) so re-running against
  the same snapshot must reproduce the same content hash.
- ``write``: operator-authorized append into
  ``control_plane.duckdb::weekly_stage_backfill_observation``. Idempotent by
  content: an existing grain row with identical content is a no-op; an
  existing grain row with different content is a conflict and the whole
  write aborts before any insert. Never a silent skip.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.domains.opportunities.coverage import (
    build_stage_coverage,
    load_sector_mapping,
)
from ai_trading_system.domains.opportunities.routing import StageCoverageConfig
from ai_trading_system.research.pattern_lane_calibration.stage_source import (
    BACKFILL_TABLE,
    normalize_stage_label,
    normalize_stage_transition,
)

RECOGNIZED_EXCLUSION_REASONS = ("invalid_ohlcv", "illiquid", "insufficient_weekly_history")

CANONICAL_COLUMNS = [
    "symbol_id", "exchange", "week_end", "stage_policy_version",
    "stage_label", "stage_transition", "stage_score",
    "input_hash", "source_bar_max_date",
]

BACKFILL_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {BACKFILL_TABLE} (
    observation_id VARCHAR NOT NULL,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    week_end DATE NOT NULL,
    stage_policy_version VARCHAR NOT NULL,
    stage_label VARCHAR NOT NULL,
    stage_transition VARCHAR,
    stage_score DOUBLE,
    reason_codes VARCHAR,
    backfill_run_id VARCHAR NOT NULL,
    source_bar_max_date DATE,
    input_hash VARCHAR,
    classifier_version VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (symbol_id, exchange, week_end, stage_policy_version)
)
"""


def stage_policy_hash(config: StageCoverageConfig | None = None) -> str:
    active = config or StageCoverageConfig()
    return hashlib.sha256(
        json.dumps({
            "classifier_version": active.stage_classifier_version,
            "confidence_formula_version": active.confidence_formula_version,
            "minimum_price": active.minimum_price,
            "minimum_liquidity_score": active.minimum_liquidity_score,
            "lock_current_week": True,
            "lookback_days": 800,
        }, sort_keys=True).encode()
    ).hexdigest()


def _json_scalar(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_commit(project_root: Path) -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=project_root, capture_output=True, text=True, check=True,
        ).stdout.strip()
    except Exception:
        return None


def _assert_no_active_writer(db_path: Path) -> None:
    """Fail when another process holds the database file open for writing."""
    result = subprocess.run(
        ["lsof", "-F", "an", str(db_path)], capture_output=True, text=True
    )
    writers = []
    for line in result.stdout.splitlines():
        if line.startswith("a") and ("w" in line[1:] or "u" in line[1:]):
            writers.append(line)
    if writers:
        raise RuntimeError(
            f"active writer detected on {db_path}; refusing to snapshot/read a moving database"
        )
    probe = duckdb.connect(str(db_path), read_only=True)
    probe.close()


def create_snapshot(source_db: Path, snapshot_dir: Path) -> Path:
    _assert_no_active_writer(source_db)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    destination = snapshot_dir / f"ohlcv-{stamp}.duckdb"
    shutil.copy2(source_db, destination)
    return destination


def canonical_content_hash(observations: pd.DataFrame) -> str:
    projection = observations[CANONICAL_COLUMNS].copy()
    projection["week_end"] = projection["week_end"].astype(str)
    projection["source_bar_max_date"] = projection["source_bar_max_date"].astype(str)
    projection["stage_score"] = projection["stage_score"].map(lambda v: f"{float(v):.6f}")
    projection = projection.sort_values(
        ["symbol_id", "exchange", "week_end", "stage_policy_version"], kind="mergesort"
    )
    payload = projection.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _fridays(from_week: str, to_week: str) -> list[pd.Timestamp]:
    fridays = pd.date_range(pd.Timestamp(from_week), pd.Timestamp(to_week), freq="W-FRI")
    if len(fridays) == 0:
        raise ValueError(f"no Fridays between {from_week} and {to_week}")
    return list(fridays)


def _load_daily(ohlcv_db: Path, *, exchange: str, from_date: str, to_date: str) -> pd.DataFrame:
    conn = duckdb.connect(str(ohlcv_db), read_only=True)
    try:
        frame = conn.execute(
            """
            SELECT symbol_id, exchange, timestamp, open, high, low, close, volume
            FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            ORDER BY symbol_id, timestamp
            """,
            [exchange, from_date, to_date],
        ).fetchdf()
    finally:
        conn.close()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    return frame


def _input_fingerprint(symbol: str, group: pd.DataFrame) -> str:
    last = group.iloc[-1]
    token = f"{symbol}|{len(group)}|{pd.Timestamp(last['timestamp']).date()}|{float(last['close'])}"
    return hashlib.sha256(token.encode()).hexdigest()


def _classify_week(
    daily: pd.DataFrame,
    *,
    week_end: pd.Timestamp,
    sector_mapping: dict[str, tuple[str, str]],
    config: StageCoverageConfig,
    market_regime: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    lower = week_end - pd.Timedelta(days=800)
    scoped = daily.loc[(daily["timestamp"] <= week_end) & (daily["timestamp"] >= lower)]
    if scoped.empty:
        return pd.DataFrame(), pd.DataFrame(columns=["symbol_id", "reason", "scope"])
    return build_stage_coverage(
        scoped,
        as_of=week_end.date().isoformat(),
        sector_mapping=sector_mapping,
        config=config,
        lock_current_week=True,
        market_regime=market_regime,
    )


def _observation_rows(
    stock: pd.DataFrame,
    daily: pd.DataFrame,
    *,
    week_end: pd.Timestamp,
    run_id: str,
    config: StageCoverageConfig,
    created_at: datetime,
) -> list[dict[str, Any]]:
    if stock.empty:
        return []
    scoped = daily.loc[daily["timestamp"] <= week_end]
    per_symbol = {
        str(symbol): group for symbol, group in scoped.groupby("symbol_id", sort=False)
    }
    week_start = (week_end - pd.Timedelta(days=6)).date()
    rows: list[dict[str, Any]] = []
    for record in stock.to_dict(orient="records"):
        symbol = str(record["symbol_id"])
        group = per_symbol.get(symbol)
        if group is None or group.empty:
            continue
        source_week_end = pd.Timestamp(record["source_week_end"]).date()
        if source_week_end < week_start:
            # The symbol did not trade in the classified week; its observation
            # is keyed to an earlier week already covered by that week's run.
            continue
        reason_codes = json.dumps({
            "provisional_stage": record.get("provisional_stage"),
            "locked_stage": record.get("locked_stage"),
            "previous_locked_stage": record.get("previous_locked_stage"),
            "stage_transition": record.get("stage_transition"),
            "stage_confidence_band": record.get("stage_confidence_band"),
            "market_regime": record.get("market_regime"),
        }, sort_keys=True)
        observation_id = hashlib.sha256(
            f"backfill|{record['exchange']}|{symbol}|{source_week_end}|{config.stage_classifier_version}".encode()
        ).hexdigest()
        rows.append({
            "observation_id": observation_id,
            "symbol_id": symbol,
            "exchange": str(record["exchange"]),
            "week_end": source_week_end,
            "stage_policy_version": config.stage_classifier_version,
            "stage_label": str(record["effective_stage"]),
            "stage_transition": str(record.get("stage_transition") or "none"),
            "stage_score": float(record.get("stage_confidence_score") or 0.0),
            "reason_codes": reason_codes,
            "backfill_run_id": run_id,
            "source_bar_max_date": pd.Timestamp(group["timestamp"].max()).date(),
            "input_hash": _input_fingerprint(symbol, group),
            "classifier_version": config.stage_classifier_version,
            "created_at": created_at,
        })
    return rows


def detect_conflicts(observations: pd.DataFrame, control_plane_db: Path) -> pd.DataFrame:
    conn = duckdb.connect(str(control_plane_db), read_only=True)
    try:
        live = conn.execute(
            """
            SELECT symbol_id, exchange, CAST(source_week_end AS DATE) AS week_end,
                   effective_stage AS live_stage, stage_status
            FROM weekly_stock_stage_history
            """
        ).fetchdf()
    finally:
        conn.close()
    if live.empty or observations.empty:
        return pd.DataFrame(columns=[
            "symbol_id", "exchange", "week_end", "backfill_stage", "live_stage", "live_status",
        ])
    live["week_end"] = pd.to_datetime(live["week_end"]).dt.date
    live = (
        live.assign(_locked=(live["stage_status"].astype(str) == "locked").astype(int))
        .sort_values(["symbol_id", "week_end", "_locked"], kind="mergesort")
        .drop_duplicates(subset=["symbol_id", "exchange", "week_end"], keep="last")
    )
    merged = observations.merge(
        live, on=["symbol_id", "exchange", "week_end"], how="inner", suffixes=("", "_live")
    )
    conflicts = merged.loc[merged["stage_label"] != merged["live_stage"]]
    return conflicts[[
        "symbol_id", "exchange", "week_end", "stage_label", "live_stage", "stage_status",
    ]].rename(columns={"stage_label": "backfill_stage", "stage_status": "live_status"})


def build_coverage_report(
    weekly_results: list[dict[str, Any]], conflicts: pd.DataFrame
) -> pd.DataFrame:
    conflict_counts = (
        conflicts.groupby("week_end").size().to_dict() if not conflicts.empty else {}
    )
    rows = []
    for result in weekly_results:
        week_end = result["week_end"]
        observations: pd.DataFrame = result["observations"]
        traded: set[str] = result["traded_symbols"]
        exclusions: pd.DataFrame = result["exclusions"]

        classified_symbols = set(observations["symbol_id"]) if not observations.empty else set()
        excluded_traded = (
            exclusions.loc[exclusions["symbol_id"].isin(traded)]
            if not exclusions.empty else pd.DataFrame(columns=["symbol_id", "reason"])
        )
        legit_excluded = set(
            excluded_traded.loc[
                excluded_traded["reason"].isin(RECOGNIZED_EXCLUSION_REASONS), "symbol_id"
            ]
        )
        unexplained = traded - classified_symbols - set(excluded_traded["symbol_id"])
        eligible = len(traded) - len(legit_excluded)

        labels = pd.Series(
            [normalize_stage_label(value) for value in observations["stage_label"]]
        ) if not observations.empty else pd.Series(dtype=str)
        transitions = pd.Series(
            [
                normalize_stage_transition(transition, raw_label=None)
                for transition in observations["stage_transition"]
            ]
        ) if not observations.empty else pd.Series(dtype=str)
        decided = int((labels != "UNDEFINED").sum())
        reason_breakdown = (
            excluded_traded["reason"].value_counts().to_dict() if not excluded_traded.empty else {}
        )
        rows.append({
            "week_end": week_end,
            "traded_universe": len(traded),
            "classified_symbols": len(classified_symbols),
            "raw_coverage_pct": round(100.0 * len(classified_symbols) / len(traded), 2) if traded else 0.0,
            "eligible_rows": eligible,
            "eligible_coverage_pct": round(100.0 * len(classified_symbols) / eligible, 2) if eligible else 0.0,
            "decision_coverage_pct": round(100.0 * decided / eligible, 2) if eligible else 0.0,
            "unexplained_exclusions": len(unexplained),
            "stage1_count": int((labels == "S1").sum()),
            "transition_count": int((transitions != "NONE").sum()),
            "s1_to_s2_count": int((transitions == "S1_TO_S2").sum()),
            "stage2_count": int((labels == "S2").sum()),
            "stage3_count": int((labels == "S3").sum()),
            "stage4_count": int((labels == "S4").sum()),
            "undefined_count": int((labels == "UNDEFINED").sum()),
            "excluded_count": int(len(excluded_traded)),
            "exclusion_breakdown": json.dumps(reason_breakdown, sort_keys=True),
            "conflict_count": int(conflict_counts.get(week_end, 0)),
        })
    return pd.DataFrame(rows)


def validate_backfill(
    observations: pd.DataFrame,
    coverage: pd.DataFrame,
    *,
    eligible_coverage_gate_pct: float = 95.0,
    unexplained_exclusion_gate_pct: float = 0.5,
) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    future_bars = observations.loc[
        pd.to_datetime(observations["source_bar_max_date"]) > pd.to_datetime(observations["week_end"])
    ]
    checks["no_future_weekly_bars"] = {"passed": future_bars.empty, "violations": int(len(future_bars))}
    grain = ["symbol_id", "exchange", "week_end", "stage_policy_version"]
    duplicates = int(observations.duplicated(subset=grain).sum())
    checks["no_duplicate_grain_rows"] = {"passed": duplicates == 0, "violations": duplicates}
    min_eligible = float(coverage["eligible_coverage_pct"].min()) if not coverage.empty else 0.0
    checks["eligible_coverage_gate"] = {
        "passed": min_eligible >= eligible_coverage_gate_pct,
        "gate_pct": eligible_coverage_gate_pct,
        "min_eligible_coverage_pct": min_eligible,
        "median_eligible_coverage_pct": float(coverage["eligible_coverage_pct"].median()) if not coverage.empty else 0.0,
        "median_raw_coverage_pct": float(coverage["raw_coverage_pct"].median()) if not coverage.empty else 0.0,
    }
    unexplained_pct = (
        100.0 * coverage["unexplained_exclusions"].sum() / max(1, coverage["traded_universe"].sum())
        if not coverage.empty else 0.0
    )
    checks["unexplained_exclusions_gate"] = {
        "passed": unexplained_pct <= unexplained_exclusion_gate_pct,
        "gate_pct": unexplained_exclusion_gate_pct,
        "unexplained_pct": round(unexplained_pct, 4),
    }
    weeks_with_s1 = int((coverage["stage1_count"] > 0).sum())
    checks["s1_nonzero_most_weeks"] = {
        "passed": weeks_with_s1 >= int(0.9 * len(coverage)),
        "weeks_with_s1": weeks_with_s1,
        "total_weeks": int(len(coverage)),
    }
    weeks_with_transitions = int((coverage["transition_count"] > 0).sum())
    checks["transitions_nonzero_most_weeks"] = {
        "passed": weeks_with_transitions >= int(0.9 * len(coverage)),
        "weeks_with_transitions": weeks_with_transitions,
    }
    label_share = (
        pd.Series([normalize_stage_label(v) for v in observations["stage_label"]])
        .value_counts(normalize=True).to_dict()
        if not observations.empty else {}
    )
    dominant = max(label_share.values()) if label_share else 1.0
    checks["stage_distribution_plausible"] = {
        "passed": bool(label_share) and dominant < 0.90,
        "label_share": {k: round(v, 4) for k, v in label_share.items()},
    }
    checks["all_passed"] = all(
        entry["passed"] for name, entry in checks.items() if isinstance(entry, dict)
    )
    return checks


def compute_backfill(
    *,
    source_ohlcv_db: Path,
    snapshot_db: Path | None,
    snapshot_dir: Path | None,
    control_plane_db: Path,
    master_db: Path,
    from_week: str,
    to_week: str,
    report_dir: Path,
    project_root: Path,
    regime_csv: Path | None = None,
    progress_every: int = 5,
) -> dict[str, Any]:
    started = perf_counter()
    if snapshot_db is None:
        if snapshot_dir is None:
            raise ValueError("compute requires --snapshot-db or --snapshot-dir")
        snapshot_db = create_snapshot(source_ohlcv_db, snapshot_dir)
    snapshot_hash = _sha256_file(snapshot_db)
    source_stat = source_ohlcv_db.stat()

    config = StageCoverageConfig()
    fridays = _fridays(from_week, to_week)
    daily = _load_daily(
        snapshot_db,
        exchange="NSE",
        from_date=(fridays[0] - pd.Timedelta(days=800)).date().isoformat(),
        to_date=fridays[-1].date().isoformat(),
    )
    if daily.empty:
        raise RuntimeError("no daily bars loaded from snapshot for backfill window")
    max_session = daily["timestamp"].max().normalize()
    completed = [friday for friday in fridays if friday <= max_session]
    if not completed:
        raise RuntimeError(f"no completed weeks: last session {max_session.date()} precedes {fridays[0].date()}")

    sector_mapping, _limitations = load_sector_mapping(master_db)
    regime_by_date: dict[pd.Timestamp, str] = {}
    if regime_csv is not None:
        regime = pd.read_csv(regime_csv)
        regime["date"] = pd.to_datetime(regime["date"]).dt.normalize()
        regime = regime.sort_values("date")
        for friday in completed:
            scoped = regime.loc[regime["date"] <= friday]
            if not scoped.empty:
                regime_by_date[friday] = str(scoped.iloc[-1]["regime"])

    run_id = f"stage-backfill-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    weekly_results: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    for index, friday in enumerate(completed, start=1):
        stock, exclusions = _classify_week(
            daily,
            week_end=friday,
            sector_mapping=sector_mapping,
            config=config,
            market_regime=regime_by_date.get(friday, "unknown"),
        )
        rows = _observation_rows(
            stock, daily, week_end=friday, run_id=run_id, config=config, created_at=created_at
        )
        all_rows.extend(rows)
        week_scope = daily.loc[
            (daily["timestamp"] > friday - pd.Timedelta(days=7)) & (daily["timestamp"] <= friday)
        ]
        symbol_exclusions = (
            exclusions.loc[exclusions.get("scope", "symbol") != "sector"] if not exclusions.empty else exclusions
        )
        weekly_results.append({
            "week_end": friday.date(),
            "observations": pd.DataFrame(rows),
            "traded_symbols": set(week_scope["symbol_id"].astype(str)),
            "exclusions": symbol_exclusions,
        })
        if index % progress_every == 0 or index == len(completed):
            elapsed = perf_counter() - started
            print(json.dumps({
                "event": "backfill_progress", "weeks_done": index, "weeks_total": len(completed),
                "week_end": friday.date().isoformat(), "observation_rows": len(rows),
                "elapsed_seconds": round(elapsed, 1),
            }), flush=True)

    observations = pd.DataFrame(all_rows)
    if observations.empty:
        raise RuntimeError("backfill produced no observations")
    conflicts = detect_conflicts(observations, control_plane_db)
    coverage = build_coverage_report(weekly_results, conflicts)
    checks = validate_backfill(observations, coverage)

    report_dir.mkdir(parents=True, exist_ok=True)
    observations.to_csv(report_dir / "backfill_observations.csv", index=False)
    coverage.to_csv(report_dir / "backfill_coverage_report.csv", index=False)
    conflicts.to_csv(report_dir / "backfill_conflicts.csv", index=False)

    policy_hash = stage_policy_hash(config)
    manifest = {
        "schema_version": "weekly-stage-backfill-manifest-v1",
        "backfill_run_id": run_id,
        "stage_policy_version": config.stage_classifier_version,
        "policy_hash": policy_hash,
        "analysis_code_commit": _git_commit(project_root),
        "snapshot": {
            "path": str(snapshot_db),
            "sha256": snapshot_hash,
            "source_path": str(source_ohlcv_db),
            "source_mtime": datetime.fromtimestamp(source_stat.st_mtime, tz=timezone.utc).isoformat(),
            "source_size_bytes": source_stat.st_size,
        },
        "regime_source": str(regime_csv) if regime_csv else None,
        "from_week": completed[0].date().isoformat(),
        "to_week": completed[-1].date().isoformat(),
        "weeks": len(completed),
        "observation_rows": int(len(observations)),
        "conflict_rows": int(len(conflicts)),
        "canonical_content_hash": canonical_content_hash(observations),
        "dataset_hashes": {
            name: _sha256_file(report_dir / name)
            for name in ("backfill_observations.csv", "backfill_coverage_report.csv", "backfill_conflicts.csv")
        },
        "validation": checks,
        "database_writes": 0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(perf_counter() - started, 1),
    }
    (report_dir / "backfill_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True, default=_json_scalar)
    )
    return manifest


def write_backfill(
    *, report_dir: Path, control_plane_db: Path, project_root: Path | None = None
) -> dict[str, Any]:
    manifest_path = report_dir / "backfill_manifest.json"
    if not manifest_path.exists():
        raise RuntimeError(f"no backfill manifest at {manifest_path}; run compute first")
    manifest = json.loads(manifest_path.read_text())
    for name, expected in manifest["dataset_hashes"].items():
        actual = _sha256_file(report_dir / name)
        if actual != expected:
            raise RuntimeError(f"report artifact {name} hash mismatch; recompute before writing")
    snapshot_info = manifest.get("snapshot") or {}
    snapshot_path = Path(snapshot_info.get("path", ""))
    if not snapshot_path.exists():
        raise RuntimeError(f"frozen snapshot missing: {snapshot_path}")
    if _sha256_file(snapshot_path) != snapshot_info.get("sha256"):
        raise RuntimeError(f"frozen snapshot {snapshot_path} no longer matches the Step A hash")
    if project_root is not None and manifest.get("analysis_code_commit"):
        current_commit = _git_commit(project_root)
        if current_commit != manifest["analysis_code_commit"]:
            raise RuntimeError(
                f"code commit {current_commit} differs from Step A commit {manifest['analysis_code_commit']}"
            )
    if stage_policy_hash() != manifest.get("policy_hash"):
        raise RuntimeError("stage policy hash differs from the Step A manifest; recompute before writing")
    _assert_no_active_writer(control_plane_db)
    observations = pd.read_csv(report_dir / "backfill_observations.csv")
    observations["week_end"] = pd.to_datetime(observations["week_end"]).dt.date
    observations["source_bar_max_date"] = pd.to_datetime(observations["source_bar_max_date"]).dt.date
    if canonical_content_hash(observations) != manifest["canonical_content_hash"]:
        raise RuntimeError("observations content hash does not match manifest; recompute before writing")

    conn = duckdb.connect(str(control_plane_db))
    try:
        conn.execute(BACKFILL_SCHEMA)
        existing = conn.execute(
            f"""SELECT symbol_id, exchange, CAST(week_end AS DATE) AS week_end,
                       stage_policy_version, stage_label, stage_transition,
                       stage_score, input_hash
                FROM {BACKFILL_TABLE}"""
        ).fetchdf()
        if existing.empty:
            to_insert = observations
            noop = 0
        else:
            existing["week_end"] = pd.to_datetime(existing["week_end"]).dt.date
            merged = observations.merge(
                existing,
                on=["symbol_id", "exchange", "week_end", "stage_policy_version"],
                how="left",
                suffixes=("", "_existing"),
                indicator=True,
            )
            overlapping = merged.loc[merged["_merge"] == "both"]
            same_content = (
                (overlapping["stage_label"] == overlapping["stage_label_existing"])
                & (overlapping["stage_transition"].fillna("") == overlapping["stage_transition_existing"].fillna(""))
                & (overlapping["input_hash"] == overlapping["input_hash_existing"])
                & ((overlapping["stage_score"] - overlapping["stage_score_existing"]).abs() < 1e-9)
            )
            content_conflicts = overlapping.loc[~same_content]
            if not content_conflicts.empty:
                conflict_path = report_dir / "backfill_write_conflicts.csv"
                content_conflicts[[
                    "symbol_id", "exchange", "week_end", "stage_policy_version",
                    "stage_label", "stage_label_existing", "input_hash", "input_hash_existing",
                ]].to_csv(conflict_path, index=False)
                raise RuntimeError(
                    f"{int(len(content_conflicts))} existing grain rows differ in content; "
                    f"aborting without inserting anything (see {conflict_path})"
                )
            noop = int(len(overlapping))
            to_insert = merged.loc[merged["_merge"] == "left_only", observations.columns.tolist()]
        conn.execute("BEGIN TRANSACTION")
        if not to_insert.empty:
            conn.register("backfill_rows", to_insert)
            conn.execute(f"INSERT INTO {BACKFILL_TABLE} SELECT * FROM backfill_rows")
            conn.unregister("backfill_rows")
        conn.execute("COMMIT")
        total = conn.execute(f"SELECT COUNT(*) FROM {BACKFILL_TABLE}").fetchone()[0]
    finally:
        conn.close()
    result = {
        "inserted": int(len(to_insert)),
        "noop_identical": noop,
        "table_rows_after": int(total),
        "backfill_run_id": manifest["backfill_run_id"],
    }
    (report_dir / "backfill_write_result.json").write_text(json.dumps(result, indent=2, sort_keys=True))
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Point-in-time weekly-stage backfill (weekly-stage-v2).")
    sub = parser.add_subparsers(dest="command", required=True)

    compute = sub.add_parser("compute", help="Run the full pipeline against a frozen snapshot; report artifacts only.")
    compute.add_argument("--ohlcv-db", required=True, help="Live OHLCV store (snapshot source).")
    compute.add_argument("--snapshot-dir", help="Directory to create a fresh frozen snapshot in.")
    compute.add_argument("--snapshot-db", help="Existing frozen snapshot to use instead of copying.")
    compute.add_argument("--control-plane-db", required=True)
    compute.add_argument("--master-db", required=True)
    compute.add_argument("--from-week", required=True)
    compute.add_argument("--to-week", required=True)
    compute.add_argument("--report-dir", required=True)
    compute.add_argument("--regime-csv")
    compute.add_argument("--project-root", default=".")

    write = sub.add_parser("write", help="Operator-authorized append of a computed report into the governed table.")
    write.add_argument("--from-report", required=True)
    write.add_argument("--control-plane-db", required=True)
    write.add_argument("--project-root", default=".")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "compute":
        manifest = compute_backfill(
            source_ohlcv_db=Path(args.ohlcv_db).resolve(),
            snapshot_db=Path(args.snapshot_db).resolve() if args.snapshot_db else None,
            snapshot_dir=Path(args.snapshot_dir).resolve() if args.snapshot_dir else None,
            control_plane_db=Path(args.control_plane_db).resolve(),
            master_db=Path(args.master_db).resolve(),
            from_week=args.from_week,
            to_week=args.to_week,
            report_dir=Path(args.report_dir).resolve(),
            project_root=Path(args.project_root).resolve(),
            regime_csv=Path(args.regime_csv).resolve() if args.regime_csv else None,
        )
        print(json.dumps({
            "status": "computed",
            "backfill_run_id": manifest["backfill_run_id"],
            "weeks": manifest["weeks"],
            "observation_rows": manifest["observation_rows"],
            "canonical_content_hash": manifest["canonical_content_hash"],
            "validation_all_passed": manifest["validation"]["all_passed"],
        }, sort_keys=True, default=_json_scalar), flush=True)
        return 0
    result = write_backfill(
        report_dir=Path(args.from_report).resolve(),
        control_plane_db=Path(args.control_plane_db).resolve(),
        project_root=Path(args.project_root).resolve(),
    )
    print(json.dumps({"status": "written", **result}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
