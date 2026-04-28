"""Read models for sector detail endpoints.

Two public functions:
  - ``get_sectors_with_stage(root)`` — sector list enriched with S1–S4 breadth.
  - ``get_sector_constituents(root, sector)`` — ALL stocks in a sector with
    latest price, technicals, and weekly Weinstein stage label.

Data sources (all read-only):
  - ``masterdata.db``       → stock_details (sector mapping, fundamentals)
  - ``data/ohlcv.duckdb``  → _catalog (latest price, volume, SMA, ADX)
  - ``data/ohlcv.duckdb``  → weekly_stage_snapshot (S1–S4 labels)
  - pipeline ranked_signals CSV → composite scores for ranked stocks
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import ensure_domain_layout

LOG = logging.getLogger(__name__)

# ── helpers ───────────────────────────────────────────────────────────────────

def _ohlcv_path(root: Path) -> str:
    paths = ensure_domain_layout(project_root=str(root), data_domain="operational")
    return str(paths.ohlcv_db_path)


def _master_path(root: Path) -> str:
    paths = ensure_domain_layout(project_root=str(root), data_domain="operational")
    return str(paths.master_db_path)


def _safe_float(v) -> float | None:
    try:
        f = float(v)
        return None if pd.isna(f) else round(f, 4)
    except Exception:
        return None


def _pct(num: int, denom: int) -> float:
    return round(num / denom * 100, 1) if denom else 0.0


# ── stage snapshot (latest per symbol) ───────────────────────────────────────

def _load_stage_snapshot(ohlcv_db: str) -> pd.DataFrame:
    """Latest stage label + confidence per symbol from weekly_stage_snapshot."""
    try:
        conn = duckdb.connect(ohlcv_db, read_only=True)
        try:
            tables = {r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name = 'weekly_stage_snapshot'"
            ).fetchall()}
            if "weekly_stage_snapshot" not in tables:
                return pd.DataFrame(columns=["symbol", "stage_label", "stage_confidence", "week_end_date"])
            df = conn.execute("""
                SELECT symbol, stage_label, stage_confidence, week_end_date
                FROM weekly_stage_snapshot
                WHERE stage_label != 'UNDEFINED'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY week_end_date DESC) = 1
            """).fetchdf()
        finally:
            conn.close()
        return df
    except Exception as exc:
        LOG.warning("_load_stage_snapshot failed: %s", exc)
        return pd.DataFrame(columns=["symbol", "stage_label", "stage_confidence", "week_end_date"])


# ── latest technicals from _catalog ──────────────────────────────────────────

def _load_latest_technicals(ohlcv_db: str, symbols: list[str]) -> pd.DataFrame:
    """Latest close, volume, sma_20/50/150, adx_14, high_52w for given symbols."""
    if not symbols:
        return pd.DataFrame()
    try:
        conn = duckdb.connect(ohlcv_db, read_only=True)
        try:
            sym_list = ", ".join(f"'{s}'" for s in symbols)
            df = conn.execute(f"""
                SELECT
                    symbol_id,
                    CAST(timestamp AS DATE) AS date,
                    close,
                    volume
                FROM _catalog
                WHERE symbol_id IN ({sym_list})
                  AND exchange = 'NSE'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id ORDER BY timestamp DESC
                ) = 1
            """).fetchdf()
        finally:
            conn.close()
        return df
    except Exception as exc:
        LOG.warning("_load_latest_technicals failed: %s", exc)
        return pd.DataFrame()


def _load_feature_technicals(root: Path, symbols: list[str]) -> pd.DataFrame:
    """Latest sma_20, sma_50, sma_150, adx_14, high_52w from feature store."""
    if not symbols:
        return pd.DataFrame()
    try:
        paths = ensure_domain_layout(project_root=str(root), data_domain="operational")
        feature_dir = paths.feature_store_dir
        # Try the ranked_signals CSV from the latest run for richer technicals
        import glob, os
        pattern = str(feature_dir.parent / "pipeline_runs" / "*" / "rank" / "attempt_*" / "ranked_signals.csv")
        files = sorted(glob.glob(pattern))
        if not files:
            return pd.DataFrame()
        latest = files[-1]
        df = pd.read_csv(latest)
        cols = ["symbol_id"]
        for c in ["sma_20", "sma_50", "sma_150", "adx_14", "high_52w",
                  "vol_20_avg", "return_20", "composite_score",
                  "rel_strength_score", "is_stage2_structural",
                  "rank_confidence"]:
            if c in df.columns:
                cols.append(c)
        sym_set = set(symbols)
        return df[df["symbol_id"].isin(sym_set)][cols].drop_duplicates("symbol_id")
    except Exception as exc:
        LOG.warning("_load_feature_technicals failed: %s", exc)
        return pd.DataFrame()


# ── sector list with stage breadth ───────────────────────────────────────────

def get_sectors_with_stage(root: Path) -> dict[str, Any]:
    """Return sector RS/momentum/quadrant + S1–S4 stage distribution per sector."""
    ohlcv_db = _ohlcv_path(root)
    master_db = _master_path(root)

    # 1. Sector RS dashboard (from latest pipeline run)
    sector_rs: list[dict] = []
    try:
        paths = ensure_domain_layout(project_root=str(root), data_domain="operational")
        import glob
        pattern = str(paths.ohlcv_db_path.parent / "pipeline_runs" / "*" / "rank" / "attempt_*" / "sector_dashboard.csv")
        files = sorted(glob.glob(pattern))
        if files:
            df = pd.read_csv(files[-1])
            sector_rs = df.to_dict(orient="records")
    except Exception as exc:
        LOG.warning("sector_dashboard CSV load failed: %s", exc)

    # 2. Stage snapshot
    snap = _load_stage_snapshot(ohlcv_db)

    # 3. Stock→sector mapping from master DB (SQLite)
    sector_map: dict[str, str] = {}
    try:
        import sqlite3
        con = sqlite3.connect(master_db)
        rows = pd.read_sql_query(
            "SELECT Symbol, Sector FROM stock_details WHERE exchange = 'NSE'", con
        )
        con.close()
        sector_map = dict(zip(rows["Symbol"], rows["Sector"]))
    except Exception as exc:
        LOG.warning("sector_map load failed: %s", exc)

    # 4. Join stage labels onto sector map
    if not snap.empty and sector_map:
        snap["sector"] = snap["symbol"].map(sector_map)
        stage_by_sector: dict[str, dict[str, int]] = {}
        for _, row in snap.dropna(subset=["sector"]).iterrows():
            sec = row["sector"]
            lbl = row["stage_label"]
            if sec not in stage_by_sector:
                stage_by_sector[sec] = {"S1": 0, "S2": 0, "S3": 0, "S4": 0, "total": 0}
            if lbl in stage_by_sector[sec]:
                stage_by_sector[sec][lbl] += 1
            stage_by_sector[sec]["total"] += 1
    else:
        stage_by_sector = {}

    # 5. Merge stage distribution into each sector row
    enriched = []
    for s in sector_rs:
        sec_name = s.get("Sector") or s.get("sector") or ""
        dist = stage_by_sector.get(sec_name, {})
        total = dist.get("total", 0)
        enriched.append({
            **s,
            "stage_s1_pct": _pct(dist.get("S1", 0), total),
            "stage_s2_pct": _pct(dist.get("S2", 0), total),
            "stage_s3_pct": _pct(dist.get("S3", 0), total),
            "stage_s4_pct": _pct(dist.get("S4", 0), total),
            "stage_s2_count": dist.get("S2", 0),
            "stage_total": total,
        })

    return {"sectors": enriched}


# ── sector constituents (ALL stocks) ─────────────────────────────────────────

def get_sector_constituents(root: Path, sector: str) -> dict[str, Any]:
    """Return ALL NSE stocks in *sector* with price, technicals, and stage label."""
    ohlcv_db = _ohlcv_path(root)
    master_db = _master_path(root)

    # 1. Get all symbols in this sector from masterdata (SQLite DB)
    import sqlite3
    try:
        con = sqlite3.connect(master_db)
        rows = pd.read_sql_query(
            "SELECT Symbol, Name, [Industry Group], Industry, MCAP "
            "FROM stock_details "
            "WHERE exchange = 'NSE' AND LOWER(Sector) = LOWER(?)",
            con,
            params=[sector],
        )
        con.close()
    except Exception as exc:
        LOG.warning("sector constituent lookup failed: %s", exc)
        return {"sector": sector, "constituents": [], "stage_summary": {}}

    if rows.empty:
        return {"sector": sector, "constituents": [], "stage_summary": {}}

    symbols = rows["Symbol"].tolist()

    # 2. Latest price + volume from _catalog
    price_df = _load_latest_technicals(ohlcv_db, symbols)

    # 3. Feature-store technicals (sma, adx, rs, etc.) from ranked_signals CSV
    feat_df = _load_feature_technicals(root, symbols)

    # 4. Stage labels from snapshot
    snap = _load_stage_snapshot(ohlcv_db)
    snap_map: dict[str, dict] = {}
    if not snap.empty:
        for _, r in snap.iterrows():
            snap_map[r["symbol"]] = {
                "stage_label": r["stage_label"],
                "stage_confidence": _safe_float(r["stage_confidence"]),
                "stage_week": str(r["week_end_date"]),
            }

    # 5. Build per-symbol response rows
    price_map: dict[str, dict] = {}
    if not price_df.empty:
        for _, r in price_df.iterrows():
            price_map[r["symbol_id"]] = {
                "close": _safe_float(r.get("close")),
                "volume": _safe_float(r.get("volume")),
                "date": str(r.get("date", "")),
            }

    feat_map: dict[str, dict] = {}
    if not feat_df.empty:
        for _, r in feat_df.iterrows():
            feat_map[r["symbol_id"]] = r.to_dict()

    constituents = []
    for _, stock in rows.iterrows():
        sym = stock["Symbol"]
        price_info = price_map.get(sym, {})
        feat_info = feat_map.get(sym, {})
        stage_info = snap_map.get(sym, {"stage_label": None, "stage_confidence": None, "stage_week": None})

        close = price_info.get("close") or _safe_float(feat_info.get("close"))
        sma50 = _safe_float(feat_info.get("sma_50"))
        sma20 = _safe_float(feat_info.get("sma_20"))
        sma150 = _safe_float(feat_info.get("sma_150"))
        high52w = _safe_float(feat_info.get("high_52w"))
        adx = _safe_float(feat_info.get("adx_14"))
        vol = price_info.get("volume")
        vol_avg = _safe_float(feat_info.get("vol_20_avg"))
        rs_score = _safe_float(feat_info.get("rel_strength_score"))
        ret20 = _safe_float(feat_info.get("return_20"))
        composite = _safe_float(feat_info.get("composite_score"))

        constituents.append({
            "symbol": sym,
            "name": stock.get("Name", ""),
            "industry": stock.get("Industry", ""),
            "mcap": _safe_float(stock.get("MCAP")),
            # Price / returns
            "close": close,
            "return_20": round(ret20 * 100, 2) if ret20 is not None else None,
            "date": price_info.get("date"),
            # Moving averages
            "sma_20": sma20,
            "sma_50": sma50,
            "sma_150": sma150,
            "high_52w": high52w,
            "adx_14": adx,
            "vol_mult": round(vol / vol_avg, 2) if (vol and vol_avg and vol_avg > 0) else None,
            # Derived booleans
            "above_ma50": bool(close and sma50 and close > sma50),
            "above_ma200": bool(close and sma150 and close > sma150),
            "golden_cross": bool(sma20 and sma50 and sma20 > sma50),
            "near_52w_high": bool(close and high52w and high52w > 0 and (high52w - close) / high52w <= 0.05),
            "adx_above_20": bool(adx and adx > 20),
            "vol_expand": bool(vol and vol_avg and vol_avg > 0 and vol / vol_avg > 1.5),
            # Ranking score (None if not in ranked list)
            "composite_score": composite,
            "rs_score": rs_score,
            # Stage
            **stage_info,
        })

    # 6. Stage summary
    labeled = [c for c in constituents if c.get("stage_label")]
    total_labeled = len(labeled)
    stage_summary = {
        "total": len(constituents),
        "labeled": total_labeled,
        "S1": sum(1 for c in labeled if c["stage_label"] == "S1"),
        "S2": sum(1 for c in labeled if c["stage_label"] == "S2"),
        "S3": sum(1 for c in labeled if c["stage_label"] == "S3"),
        "S4": sum(1 for c in labeled if c["stage_label"] == "S4"),
    }
    for s in ("S1", "S2", "S3", "S4"):
        stage_summary[f"{s}_pct"] = _pct(stage_summary[s], total_labeled)

    # Sort: S2 first (most interesting), then by composite score desc
    stage_order = {"S2": 0, "S1": 1, "S3": 2, "S4": 3, None: 4}
    constituents.sort(
        key=lambda c: (
            stage_order.get(c.get("stage_label"), 4),
            -(c.get("composite_score") or 0),
        )
    )

    return {
        "sector": sector,
        "stage_summary": stage_summary,
        "constituents": constituents,
    }
