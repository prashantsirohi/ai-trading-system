"""Presentation payloads for fundamentals Sheets and React surfaces."""

from __future__ import annotations

from typing import Any

import pandas as pd


DEFAULT_PUBLISH_UNIVERSE_ID = "UNIV_TOP1000_MCAP"


def build_fundamental_sheet_payload(
    *,
    universe_valuation: pd.DataFrame | None,
    valuation_cycle: pd.DataFrame | None,
    sector_dashboard: pd.DataFrame | None = None,
    sector_valuation: pd.DataFrame | None = None,
    universe_id: str = DEFAULT_PUBLISH_UNIVERSE_ID,
    years: int = 5,
) -> dict[str, Any]:
    """Build the single-tab valuation dashboard payload for Google Sheets."""

    chart_rows = _valuation_chart_rows(
        universe_valuation=_as_frame(universe_valuation),
        valuation_cycle=_as_frame(valuation_cycle),
        universe_id=universe_id,
        years=years,
    )
    latest = chart_rows.tail(1).iloc[0].to_dict() if not chart_rows.empty else {}
    summary = {
        "Universe": universe_id,
        "Period": f"Last {years} years",
        "Current PE": _json_scalar(latest.get("pe_ttm")),
        "PE 200DMA": _json_scalar(latest.get("pe_200dma")),
        "PE 5Y Median": _json_scalar(latest.get("pe_5y_median")),
        "PE 5Y Percentile": _json_scalar(latest.get("pe_percentile_5y")),
        "Valuation Zone": _json_scalar(latest.get("valuation_zone")),
        "PE distance from 200DMA": _json_scalar(latest.get("pe_distance_from_200dma")),
        "Loss-making mcap %": _json_scalar(latest.get("loss_mcap_pct")),
    }
    sector_context = build_sector_context_rows(
        sector_dashboard=_as_frame(sector_dashboard),
        sector_valuation=_as_frame(sector_valuation),
        universe_id=universe_id,
    )
    return {
        "summary": summary,
        "chart_rows": _records(chart_rows),
        "sector_context_rows": _records(sector_context),
    }


def build_fundamental_dashboard_payload(
    *,
    base_payload: dict[str, Any] | None = None,
    great_results: pd.DataFrame | None = None,
    turnarounds: pd.DataFrame | None = None,
    compounders: pd.DataFrame | None = None,
    sector_earnings: pd.DataFrame | None = None,
    universe_valuation: pd.DataFrame | None = None,
    valuation_cycle: pd.DataFrame | None = None,
    universe_id: str = DEFAULT_PUBLISH_UNIVERSE_ID,
    years: int = 5,
) -> dict[str, Any]:
    """Build compact JSON for the React fundamentals page."""

    payload = dict(base_payload or {})
    chart = _valuation_chart_rows(
        universe_valuation=_as_frame(universe_valuation),
        valuation_cycle=_as_frame(valuation_cycle),
        universe_id=universe_id,
        years=years,
    )
    latest = chart.tail(1).iloc[0].to_dict() if not chart.empty else dict(payload.get("universe") or {})
    summary = dict(payload.get("summary") or {})
    summary.update(
        {
            "universe_id": universe_id,
            "pe_ttm": _json_scalar(latest.get("pe_ttm")),
            "pe_200dma": _json_scalar(latest.get("pe_200dma")),
            "pe_5y_median": _json_scalar(latest.get("pe_5y_median")),
            "pe_percentile_5y": _json_scalar(latest.get("pe_percentile_5y")),
            "valuation_zone": _json_scalar(latest.get("valuation_zone")),
            "pe_distance_from_200dma": _json_scalar(latest.get("pe_distance_from_200dma")),
            "loss_mcap_pct": _json_scalar(latest.get("loss_mcap_pct")),
        }
    )
    return {
        "summary": summary,
        "valuation_chart": _records(chart),
        "great_results_top": _records(_top_rows(_as_frame(great_results), 10)),
        "turnarounds_top": _records(_top_rows(_as_frame(turnarounds), 10)),
        "compounders_top": _records(_top_rows(_as_frame(compounders), 20)),
        "sector_earnings_top": _records(_top_rows(_as_frame(sector_earnings), 20, score_col="sector_fundamental_score")),
    }


def build_sector_context_rows(
    *,
    sector_dashboard: pd.DataFrame,
    sector_valuation: pd.DataFrame,
    universe_id: str = DEFAULT_PUBLISH_UNIVERSE_ID,
) -> pd.DataFrame:
    columns = ["Rank", "Sector", "RS", "Momentum", "Quadrant", "Valuation vs 5Y Avg PE"]
    if sector_dashboard.empty:
        return pd.DataFrame(columns=columns)
    dashboard = sector_dashboard.copy()
    sector_col = _first_col(dashboard, ["Sector", "sector", "sector_name"])
    quadrant_col = _first_col(dashboard, ["Quadrant", "quadrant"])
    rank_col = _first_col(dashboard, ["RS_rank", "rs_rank", "rank"])
    rs_col = _first_col(dashboard, ["RS", "rs", "rel_strength"])
    momentum_col = _first_col(dashboard, ["Momentum", "momentum"])
    if sector_col is None or quadrant_col is None:
        return pd.DataFrame(columns=columns)
    dashboard.loc[:, "_quadrant"] = dashboard[quadrant_col].astype(str).str.strip()
    filtered = dashboard.loc[dashboard["_quadrant"].isin(["Leading", "Improving"])].copy()
    if filtered.empty:
        return pd.DataFrame(columns=columns)
    valuation = _latest_sector_valuation(sector_valuation, universe_id=universe_id)
    rows = []
    for _, row in filtered.iterrows():
        sector = str(row.get(sector_col) or "")
        valuation_row = valuation.get(_norm(sector), {})
        rows.append(
            {
                "Rank": _num(row.get(rank_col)) if rank_col else None,
                "Sector": sector,
                "RS": _num(row.get(rs_col)) if rs_col else None,
                "Momentum": _num(row.get(momentum_col)) if momentum_col else None,
                "Quadrant": row.get(quadrant_col),
                "Valuation vs 5Y Avg PE": valuation_label(
                    _num(valuation_row.get("pe_ttm")),
                    _num(valuation_row.get("pe_avg_5y") or valuation_row.get("sector_pe_5y_avg")),
                ),
            }
        )
    out = pd.DataFrame(rows, columns=columns)
    if "Rank" in out.columns:
        out = out.sort_values("Rank", na_position="last", kind="stable")
    return out.reset_index(drop=True)


def valuation_label(current_pe: float | None, avg_5y_pe: float | None) -> str:
    if current_pe is None or avg_5y_pe is None or avg_5y_pe == 0:
        return "5Y avg unavailable"
    premium = (current_pe - avg_5y_pe) / avg_5y_pe * 100.0
    if premium <= -10:
        label = "Below 5Y avg"
    elif premium < 10:
        label = "Near 5Y avg"
    elif premium < 25:
        label = "Above 5Y avg"
    else:
        label = "High premium"
    return f"{label} ({premium:+.1f}%)"


def _valuation_chart_rows(
    *,
    universe_valuation: pd.DataFrame,
    valuation_cycle: pd.DataFrame,
    universe_id: str,
    years: int,
) -> pd.DataFrame:
    valuation = universe_valuation.copy()
    cycle = valuation_cycle.copy()
    if not valuation.empty and "universe_id" in valuation.columns:
        valuation = valuation.loc[valuation["universe_id"].astype(str).eq(universe_id)].copy()
    if not cycle.empty:
        if "entity_id" in cycle.columns:
            cycle = cycle.loc[cycle["entity_id"].astype(str).eq(universe_id)].copy()
        elif "universe_id" in cycle.columns:
            cycle = cycle.loc[cycle["universe_id"].astype(str).eq(universe_id)].copy()
    for frame in (valuation, cycle):
        if not frame.empty and "date" in frame.columns:
            frame.loc[:, "date"] = pd.to_datetime(frame["date"], errors="coerce")
    if valuation.empty and cycle.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "index_level",
                "index_200dma",
                "pe_ttm",
                "pe_200dma",
                "pe_5y_median",
                "pe_percentile_5y",
                "valuation_zone",
                "pe_distance_from_200dma",
                "loss_mcap_pct",
            ]
        )
    columns_from_valuation = [
        column
        for column in ["date", "index_level_mcap_weight", "index_level", "loss_mcap_pct"]
        if column in valuation.columns
    ]
    columns_from_cycle = [
        column
        for column in [
            "date",
            "pe_ttm",
            "pe_200dma",
            "pe_5y_median",
            "pe_percentile_5y",
            "valuation_zone",
            "pe_distance_from_200dma",
            "index_level",
            "index_200dma",
        ]
        if column in cycle.columns
    ]
    base = valuation[columns_from_valuation].copy() if columns_from_valuation else pd.DataFrame()
    extra = cycle[columns_from_cycle].copy() if columns_from_cycle else pd.DataFrame()
    if base.empty:
        merged = extra
    elif extra.empty:
        merged = base
    else:
        merged = base.merge(extra, on="date", how="outer", suffixes=("", "_cycle"))
    if "index_level" not in merged.columns and "index_level_mcap_weight" in merged.columns:
        merged.loc[:, "index_level"] = merged["index_level_mcap_weight"]
    if "index_200dma" not in merged.columns and "index_level" in merged.columns:
        merged.loc[:, "index_200dma"] = pd.to_numeric(merged["index_level"], errors="coerce").rolling(200, min_periods=20).mean()
    merged.loc[:, "date"] = pd.to_datetime(merged["date"], errors="coerce")
    merged = merged.sort_values("date").dropna(subset=["date"]).reset_index(drop=True)
    if not merged.empty:
        latest = merged["date"].max()
        merged = merged.loc[merged["date"].ge(latest - pd.DateOffset(years=years))].copy()
        merged.loc[:, "date"] = pd.to_datetime(merged["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    ordered = [
        "date",
        "index_level",
        "index_200dma",
        "pe_ttm",
        "pe_200dma",
        "pe_5y_median",
        "pe_percentile_5y",
        "valuation_zone",
        "pe_distance_from_200dma",
        "loss_mcap_pct",
    ]
    return merged[[column for column in ordered if column in merged.columns]].reset_index(drop=True)


def _latest_sector_valuation(frame: pd.DataFrame, *, universe_id: str) -> dict[str, dict[str, Any]]:
    if frame.empty:
        return {}
    out = frame.copy()
    if "universe_id" in out.columns:
        out = out.loc[out["universe_id"].astype(str).eq(universe_id)].copy()
    if out.empty:
        return {}
    if "date" in out.columns:
        out.loc[:, "date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.sort_values("date")
        pieces = []
        for _, group in out.groupby(_first_col(out, ["sector_name", "Sector", "sector"]) or "sector_name", sort=False):
            group = group.copy()
            if "pe_avg_5y" not in group.columns and "pe_ttm" in group.columns:
                group.loc[:, "pe_avg_5y"] = pd.to_numeric(group["pe_ttm"], errors="coerce").rolling(1260, min_periods=60).mean()
            pieces.append(group.tail(1))
        out = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
    sector_col = _first_col(out, ["sector_name", "Sector", "sector"])
    if sector_col is None:
        return {}
    return {_norm(row.get(sector_col)): row.to_dict() for _, row in out.iterrows()}


def _top_rows(frame: pd.DataFrame, limit: int, *, score_col: str = "insight_score") -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    if score_col in out.columns:
        out = out.sort_values(score_col, ascending=False, na_position="last", kind="stable")
    elif "report_date" in out.columns:
        out = out.sort_values("report_date", ascending=False, na_position="last", kind="stable")
    if "symbol" in out.columns:
        out = out.drop_duplicates("symbol", keep="first")
    return out.head(limit).reset_index(drop=True)


def _first_col(frame: pd.DataFrame, names: list[str]) -> str | None:
    for name in names:
        if name in frame.columns:
            return name
    return None


def _as_frame(value: pd.DataFrame | None) -> pd.DataFrame:
    return value.copy() if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    clean = frame.where(frame.notna(), None)
    return [{str(k): _json_scalar(v) for k, v in row.items()} for row in clean.to_dict(orient="records")]


def _json_scalar(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _num(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(parsed) else parsed


def _norm(value: Any) -> str:
    return str(value or "").strip().casefold()


__all__ = [
    "DEFAULT_PUBLISH_UNIVERSE_ID",
    "build_fundamental_dashboard_payload",
    "build_fundamental_sheet_payload",
    "build_sector_context_rows",
    "valuation_label",
]
