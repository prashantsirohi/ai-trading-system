"""Refresh and export canonical fundamental insight readmodels."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.domains.features.company_growth_features import refresh_company_growth_features
from ai_trading_system.domains.features.company_insight_tags import refresh_company_insight_tags
from ai_trading_system.domains.features.sector_earnings_leadership import refresh_sector_earnings_leadership_analytical
from ai_trading_system.domains.features.universe_valuation import refresh_universe_valuation_daily
from ai_trading_system.domains.features.valuation_cycle_features import refresh_fundamental_valuation_cycle_features
from ai_trading_system.domains.fundamentals.analytical_store import (
    default_fundamentals_duckdb_path,
    mirror_screener_financials,
)
from ai_trading_system.platform.db.paths import get_domain_paths


GREAT_RESULT_PRIORITY = {
    "blowout_result": 1,
    "great_result": 2,
    "profit_acceleration_result": 3,
    "margin_expansion_result": 4,
    "revenue_acceleration_result": 5,
}
TURNAROUND_PRIORITY = {
    "turnaround_confirmed": 1,
    "turnaround_candidate": 2,
    "loss_to_profit": 3,
    "margin_recovery": 4,
    "sales_recovery": 5,
}
COMPOUNDER_PRIORITY = {
    "quality_growth": 1,
    "high_growth_compounder": 2,
    "consistent_compounder": 3,
    "emerging_compounder": 4,
}


def refresh_fundamental_insight_readmodels(
    *,
    screener_db_path: str | Path,
    fundamentals_db_path: str | Path | None = None,
    ohlcv_db_path: str | Path | None = None,
    master_db_path: str | Path | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    output_dir: str | Path | None = None,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    resolved_fundamentals = Path(fundamentals_db_path) if fundamentals_db_path is not None else default_fundamentals_duckdb_path(project_root)
    resolved_ohlcv = Path(ohlcv_db_path) if ohlcv_db_path is not None else paths.ohlcv_db_path
    resolved_master = Path(master_db_path) if master_db_path is not None else paths.master_db_path
    resolved_output = Path(output_dir) if output_dir is not None else None
    if resolved_output is not None:
        resolved_output.mkdir(parents=True, exist_ok=True)

    mirrored_rows = mirror_screener_financials(
        screener_db_path=screener_db_path,
        fundamentals_db_path=resolved_fundamentals,
        project_root=project_root,
    )
    growth = refresh_company_growth_features(
        fundamentals_db_path=resolved_fundamentals,
        from_date=from_date,
        to_date=to_date,
    )
    tags = refresh_company_insight_tags(
        fundamentals_db_path=resolved_fundamentals,
        from_date=from_date,
        to_date=to_date,
    )
    sector = refresh_sector_earnings_leadership_analytical(
        fundamentals_db_path=resolved_fundamentals,
        master_db_path=resolved_master,
        from_date=from_date,
        to_date=to_date,
        output_csv=resolved_output / "sector_earnings_leadership.csv" if resolved_output is not None else None,
    )
    valuation_status: dict[str, Any]
    if _ohlcv_has_valuation_inputs(resolved_ohlcv):
        universe = refresh_universe_valuation_daily(
            ohlcv_db_path=resolved_ohlcv,
            fundamentals_db_path=resolved_fundamentals,
            from_date=from_date,
            to_date=to_date,
        )
        cycle = refresh_fundamental_valuation_cycle_features(
            fundamentals_db_path=resolved_fundamentals,
            from_date=from_date,
            to_date=to_date,
        )
        valuation_status = {"universe": asdict(universe), "cycle": asdict(cycle)}
    else:
        valuation_status = {"status": "skipped_missing_ohlcv_valuation_inputs"}

    artifacts: dict[str, str] = {}
    if resolved_output is not None:
        exports = {
            "company_growth_features": ("company_growth_features", "report_date", "company_growth_features.csv"),
            "company_insight_tags": ("company_insight_tags", "report_date", "company_insight_tags.csv"),
            "universe_valuation_daily": ("universe_valuation_daily", "date", "universe_valuation_daily.csv"),
            "valuation_cycle_features": ("valuation_cycle_features", "date", "valuation_cycle_features.csv"),
        }
        for artifact_type, (table, date_col, filename) in exports.items():
            path = resolved_output / filename
            _export_table(resolved_fundamentals, table, date_col, path, from_date=from_date, to_date=to_date)
            artifacts[artifact_type] = str(path)
        sector_path = resolved_output / "sector_earnings_leadership.csv"
        if sector_path.exists():
            artifacts["sector_earnings_leadership"] = str(sector_path)
        tags_path = resolved_output / "company_insight_tags.csv"
        growth_path = resolved_output / "company_growth_features.csv"
        sector_valuation_path = resolved_output / "sector_valuation_daily.csv"
        _export_optional_ohlcv_table(
            resolved_ohlcv,
            "sector_valuation_daily",
            "date",
            sector_valuation_path,
            from_date=from_date,
            to_date=to_date,
        )
        artifacts["sector_valuation_daily"] = str(sector_valuation_path)
        candidate_paths = _export_candidate_artifacts(tags_path, resolved_output)
        artifacts.update({artifact_type: str(path) for artifact_type, path in candidate_paths.items()})
        curated_paths = _export_curated_artifacts(
            tags_path=tags_path,
            sector_path=sector_path,
            sector_valuation_path=sector_valuation_path,
            universe_path=resolved_output / "universe_valuation_daily.csv",
            cycle_path=resolved_output / "valuation_cycle_features.csv",
            output_dir=resolved_output,
        )
        artifacts.update({artifact_type: str(path) for artifact_type, path in curated_paths.items()})
        payload_path = resolved_output / "fundamental_dashboard_payload.json"
        payload = _build_dashboard_payload(
            run_date=to_date,
            tags_path=tags_path,
            sector_path=sector_path,
            universe_path=resolved_output / "universe_valuation_daily.csv",
            cycle_path=resolved_output / "valuation_cycle_features.csv",
            growth_path=growth_path,
            great_latest_path=curated_paths.get("great_results_latest"),
            turnaround_latest_path=curated_paths.get("turnaround_candidates_latest"),
            compounder_latest_path=curated_paths.get("compounder_candidates_latest"),
            sector_latest_path=curated_paths.get("sector_earnings_latest"),
            valuation_cycle_latest_path=curated_paths.get("valuation_cycle_latest"),
        )
        payload_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        artifacts["fundamental_dashboard_payload"] = str(payload_path)

    return {
        "status": "completed",
        "fundamentals_db_path": str(resolved_fundamentals),
        "screener_rows_mirrored": int(mirrored_rows),
        "company_growth_features": asdict(growth),
        "company_insight_tags": asdict(tags),
        "sector_earnings_leadership": sector,
        "valuation": valuation_status,
        "artifacts": artifacts,
    }


def _ohlcv_has_valuation_inputs(path: Path) -> bool:
    if not path.exists():
        return False
    conn = duckdb.connect(str(path), read_only=True)
    try:
        tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name IN ('stock_valuation_daily', 'universe_index_daily')
                """
            ).fetchall()
        }
        return {"stock_valuation_daily", "universe_index_daily"}.issubset(tables)
    finally:
        conn.close()


def _export_table(
    db_path: Path,
    table: str,
    date_col: str,
    output: Path,
    *,
    from_date: str | None,
    to_date: str | None,
) -> None:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        filters = []
        params: list[str] = []
        if from_date:
            filters.append(f"{date_col} >= CAST(? AS DATE)")
            params.append(str(from_date)[:10])
        if to_date:
            filters.append(f"{date_col} <= CAST(? AS DATE)")
            params.append(str(to_date)[:10])
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        frame = conn.execute(f"SELECT * FROM {table} {where} ORDER BY {date_col}", params).df()
    finally:
        conn.close()
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)


def _export_optional_ohlcv_table(
    db_path: Path,
    table: str,
    date_col: str,
    output: Path,
    *,
    from_date: str | None,
    to_date: str | None,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if not db_path.exists():
        pd.DataFrame().to_csv(output, index=False)
        return
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        exists = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table],
        ).fetchone()[0]
        if not exists:
            frame = pd.DataFrame()
        else:
            filters = []
            params: list[str] = []
            if from_date:
                filters.append(f"{date_col} >= CAST(? AS DATE)")
                params.append(str(from_date)[:10])
            if to_date:
                filters.append(f"{date_col} <= CAST(? AS DATE)")
                params.append(str(to_date)[:10])
            where = f"WHERE {' AND '.join(filters)}" if filters else ""
            frame = conn.execute(f"SELECT * FROM {table} {where} ORDER BY {date_col}", params).df()
    finally:
        conn.close()
    frame.to_csv(output, index=False)


def _export_candidate_artifacts(tags_path: Path, output_dir: Path) -> dict[str, Path]:
    if tags_path.exists():
        try:
            tags = pd.read_csv(tags_path)
        except pd.errors.EmptyDataError:
            tags = pd.DataFrame()
    else:
        tags = pd.DataFrame()
    groups = {
        "great_results": {
            "great_result",
            "blowout_result",
            "margin_expansion_result",
            "revenue_acceleration_result",
            "profit_acceleration_result",
        },
        "turnaround_candidates": {
            "turnaround_candidate",
            "turnaround_confirmed",
            "loss_to_profit",
            "margin_recovery",
            "sales_recovery",
        },
        "compounder_candidates": {
            "consistent_compounder",
            "emerging_compounder",
            "high_growth_compounder",
            "quality_growth",
            "expensive_compounder",
        },
    }
    paths: dict[str, Path] = {}
    for artifact_type, insight_types in groups.items():
        path = output_dir / f"{artifact_type}.csv"
        if tags.empty or "insight_type" not in tags.columns:
            frame = pd.DataFrame(columns=list(tags.columns) if not tags.empty else ["symbol", "report_date", "insight_type", "insight_score", "evidence_json"])
        else:
            frame = tags[tags["insight_type"].astype(str).isin(insight_types)].copy()
            sort_cols = [col for col in ["report_date", "insight_score", "symbol"] if col in frame.columns]
            if sort_cols:
                ascending = [False if col in {"report_date", "insight_score"} else True for col in sort_cols]
                frame = frame.sort_values(sort_cols, ascending=ascending, na_position="last")
        frame.to_csv(path, index=False)
        paths[artifact_type] = path
    return paths


def _export_curated_artifacts(
    *,
    tags_path: Path,
    sector_path: Path,
    sector_valuation_path: Path,
    universe_path: Path,
    cycle_path: Path,
    output_dir: Path,
) -> dict[str, Path]:
    tags = _read_csv(tags_path)
    paths: dict[str, Path] = {}
    stock_specs = {
        "great_results_latest": GREAT_RESULT_PRIORITY,
        "turnaround_candidates_latest": TURNAROUND_PRIORITY,
        "compounder_candidates_latest": COMPOUNDER_PRIORITY,
    }
    for artifact_type, priority in stock_specs.items():
        path = output_dir / f"{artifact_type}.csv"
        _curated_stock_tags(tags, priority, limit=100).to_csv(path, index=False)
        paths[artifact_type] = path

    sector_latest = _latest_by_date(_read_csv(sector_path), "report_date")
    if not sector_latest.empty and "sector_fundamental_score" in sector_latest.columns:
        sector_latest = sector_latest.sort_values("sector_fundamental_score", ascending=False, na_position="last")
    path = output_dir / "sector_earnings_latest.csv"
    sector_latest.to_csv(path, index=False)
    paths["sector_earnings_latest"] = path

    sector_valuation_latest = _latest_by_date(_read_csv(sector_valuation_path), "date")
    path = output_dir / "sector_valuation_latest.csv"
    sector_valuation_latest.to_csv(path, index=False)
    paths["sector_valuation_latest"] = path

    universe_latest = _recent_by_date(_read_csv(universe_path), "date", limit=500)
    path = output_dir / "universe_valuation_latest.csv"
    universe_latest.to_csv(path, index=False)
    paths["universe_valuation_latest"] = path

    cycle_latest = _recent_by_date(_read_csv(cycle_path), "date", limit=500)
    path = output_dir / "valuation_cycle_latest.csv"
    cycle_latest.to_csv(path, index=False)
    paths["valuation_cycle_latest"] = path
    return paths


def _curated_stock_tags(tags: pd.DataFrame, priority: dict[str, int], *, limit: int) -> pd.DataFrame:
    columns = [
        "symbol",
        "report_date",
        "insight_type",
        "insight_score",
        "evidence",
        "sales_yoy_growth",
        "profit_yoy_growth",
        "profit_qoq_growth",
        "opm_yoy_change",
        "net_profit_cr",
    ]
    if tags.empty or "insight_type" not in tags.columns:
        return pd.DataFrame(columns=columns)
    frame = tags[tags["insight_type"].astype(str).isin(priority)].copy()
    frame = _latest_by_date(frame, "report_date")
    if frame.empty:
        return pd.DataFrame(columns=columns)
    frame.loc[:, "_priority"] = frame["insight_type"].astype(str).map(priority).fillna(99).astype(int)
    frame.loc[:, "insight_score"] = pd.to_numeric(frame.get("insight_score"), errors="coerce")
    evidence = frame.get("evidence_json", pd.Series("", index=frame.index)).map(_parse_evidence)
    frame.loc[:, "evidence"] = evidence.map(lambda item: item.get("note") or "")
    for key in ("sales_yoy_growth", "profit_yoy_growth", "profit_qoq_growth", "opm_yoy_change", "net_profit_cr"):
        if key not in frame.columns:
            frame.loc[:, key] = evidence.map(lambda item, metric=key: item.get(metric))
    frame = frame.sort_values(["_priority", "insight_score", "symbol"], ascending=[True, False, True], na_position="last")
    frame = frame.drop_duplicates("symbol", keep="first").head(limit)
    if "report_date" in frame.columns:
        frame.loc[:, "report_date"] = pd.to_datetime(frame["report_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    selected = [column for column in columns if column in frame.columns]
    return frame[selected].reset_index(drop=True)


def _build_dashboard_payload(
    *,
    run_date: str | None,
    tags_path: Path,
    sector_path: Path,
    universe_path: Path,
    cycle_path: Path,
    growth_path: Path,
    great_latest_path: Path | None = None,
    turnaround_latest_path: Path | None = None,
    compounder_latest_path: Path | None = None,
    sector_latest_path: Path | None = None,
    valuation_cycle_latest_path: Path | None = None,
) -> dict[str, Any]:
    tags = _read_csv(tags_path)
    sectors = _read_csv(sector_path)
    universe = _read_csv(universe_path)
    cycle = _read_csv(cycle_path)
    growth = _read_csv(growth_path)
    latest_universe = _latest_by_date(universe, "date")
    great_latest = _read_csv(great_latest_path) if great_latest_path else _curated_stock_tags(tags, GREAT_RESULT_PRIORITY, limit=100)
    turnaround_latest = _read_csv(turnaround_latest_path) if turnaround_latest_path else _curated_stock_tags(tags, TURNAROUND_PRIORITY, limit=100)
    compounder_latest = _read_csv(compounder_latest_path) if compounder_latest_path else _curated_stock_tags(tags, COMPOUNDER_PRIORITY, limit=100)
    sector_latest = _read_csv(sector_latest_path) if sector_latest_path else _latest_by_date(sectors, "report_date")
    cycle_latest = _read_csv(valuation_cycle_latest_path) if valuation_cycle_latest_path else _recent_by_date(cycle, "date", limit=500)
    summary = {
        "company_growth_rows": int(len(growth)),
        "insight_tag_rows": int(len(tags)),
        "great_results_count": int(len(great_latest)),
        "turnaround_count": int(len(turnaround_latest)),
        "compounder_count": int(len(compounder_latest)),
        "sector_count": int(sector_latest["sector_name"].nunique()) if "sector_name" in sector_latest.columns else int(len(sector_latest)),
    }
    if not sector_latest.empty:
        if "sector_fundamental_score" in sector_latest.columns:
            top = sector_latest.sort_values("sector_fundamental_score", ascending=False, na_position="last").head(1)
            if not top.empty:
                summary["top_earnings_sector"] = str(top.iloc[0].get("sector_name") or top.iloc[0].get("sector") or "")
    universe_payload = {}
    if not latest_universe.empty:
        row = latest_universe.sort_values("date").tail(1).iloc[0].to_dict()
        universe_payload = {key: _json_scalar(value) for key, value in row.items()}
    return {
        "run_date": str(run_date or _max_date([universe, cycle, sectors, tags]) or ""),
        "summary": summary,
        "universe": universe_payload,
        "top_great_results": _records(great_latest.head(20)),
        "top_turnarounds": _records(turnaround_latest.head(20)),
        "top_compounders": _records(compounder_latest.head(20)),
        "sector_earnings_leadership": _records(sector_latest.head(20)),
        "valuation_chart": _records(cycle_latest),
    }


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _latest_by_date(frame: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if frame.empty or date_col not in frame.columns:
        return frame
    df = frame.copy()
    df.loc[:, date_col] = pd.to_datetime(df[date_col], errors="coerce")
    latest = df[date_col].max()
    if pd.isna(latest):
        return df
    return df[df[date_col].eq(latest)]


def _recent_by_date(frame: pd.DataFrame, date_col: str, *, limit: int) -> pd.DataFrame:
    if frame.empty or date_col not in frame.columns:
        return frame
    df = frame.copy()
    df.loc[:, date_col] = pd.to_datetime(df[date_col], errors="coerce")
    return df.sort_values(date_col).tail(limit).reset_index(drop=True)


def _count_tags(tags: pd.DataFrame, insight_types: set[str]) -> int:
    if tags.empty or "insight_type" not in tags.columns:
        return 0
    return int(tags["insight_type"].astype(str).isin(insight_types).sum())


def _parse_evidence(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {"note": value}
    return parsed if isinstance(parsed, dict) else {}


def _top_tags(tags: pd.DataFrame, insight_types: set[str], limit: int = 20) -> list[dict[str, Any]]:
    if tags.empty or "insight_type" not in tags.columns:
        return []
    frame = tags[tags["insight_type"].astype(str).isin(insight_types)].copy()
    if frame.empty:
        return []
    sort_cols = [col for col in ["report_date", "insight_score", "symbol"] if col in frame.columns]
    if sort_cols:
        frame = frame.sort_values(
            sort_cols,
            ascending=[False if col in {"report_date", "insight_score"} else True for col in sort_cols],
            na_position="last",
        )
    return _records(frame.head(limit))


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    clean = frame.where(frame.notna(), None)
    return [{str(k): _json_scalar(v) for k, v in row.items()} for row in clean.to_dict(orient="records")]


def _json_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    return str(value)


def _max_date(frames: list[pd.DataFrame]) -> str | None:
    values = []
    for frame in frames:
        for column in ("date", "report_date"):
            if frame is not None and not frame.empty and column in frame.columns:
                series = pd.to_datetime(frame[column], errors="coerce").dropna()
                if not series.empty:
                    values.append(series.max())
    if not values:
        return None
    return str(max(values).date())


__all__ = ["refresh_fundamental_insight_readmodels"]
