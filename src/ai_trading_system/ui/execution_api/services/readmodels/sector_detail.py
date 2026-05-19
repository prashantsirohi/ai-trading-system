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
from ai_trading_system.ui.execution_api.services.readmodels.latest_operational_snapshot import (
    load_latest_operational_snapshot,
)

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
    """Latest close, 20-day return, volume context, and 52-week high per symbol.

    Returns columns: symbol_id, date, close, close_20d_ago, volume,
    vol_20_avg, high_52w.
    Covers ALL requested symbols (no ranked-list dependency).
    """
    if not symbols:
        return pd.DataFrame()
    try:
        conn = duckdb.connect(ohlcv_db, read_only=True)
        try:
            sym_list = ", ".join(f"'{s}'" for s in symbols)
            # One scan, window functions for both the latest snapshot and
            # the trailing aggregates. rn=1 row carries the per-symbol totals.
            df = conn.execute(f"""
                WITH ranked AS (
                    SELECT
                        symbol_id,
                        timestamp,
                        close,
                        volume,
                        high,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol_id ORDER BY timestamp DESC
                        ) AS rn
                    FROM _catalog
                    WHERE symbol_id IN ({sym_list})
                      AND exchange = 'NSE'
                ),
                agg AS (
                    SELECT
                        symbol_id,
                        MAX(close) FILTER (WHERE rn = 21) AS close_20d_ago,
                        AVG(volume) FILTER (WHERE rn <= 20) AS vol_20_avg,
                        MAX(high)   FILTER (WHERE rn <= 252) AS high_52w
                    FROM ranked
                    GROUP BY symbol_id
                )
                SELECT
                    r.symbol_id,
                    CAST(r.timestamp AS DATE) AS date,
                    r.close,
                    a.close_20d_ago,
                    r.volume,
                    a.vol_20_avg,
                    a.high_52w
                FROM ranked r
                JOIN agg a USING (symbol_id)
                WHERE r.rn = 1
            """).fetchdf()
        finally:
            conn.close()
        return df
    except Exception as exc:
        LOG.warning("_load_latest_technicals failed: %s", exc)
        return pd.DataFrame()


def _load_indicators_for_symbols(root: Path, symbols: list[str]) -> pd.DataFrame:
    """Latest technical indicators per symbol from the feature store.

    Reads the per-symbol parquet shards via DuckDB glob (three reads instead
    of N×3). Returns one row per symbol with the indicator columns.
    """
    if not symbols:
        return pd.DataFrame()
    paths = ensure_domain_layout(project_root=str(root), data_domain="operational")
    fs_root = paths.feature_store_dir
    sym_list = ", ".join(f"'{s}'" for s in symbols)
    try:
        conn = duckdb.connect(":memory:")
        try:
            def _latest(table_glob: str, cols: list[str]) -> pd.DataFrame:
                col_select = ", ".join(cols)
                return conn.execute(f"""
                    SELECT symbol_id, {col_select}
                    FROM read_parquet('{table_glob}', union_by_name = true)
                    WHERE symbol_id IN ({sym_list})
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY symbol_id ORDER BY timestamp DESC
                    ) = 1
                """).fetchdf()

            sma = _latest(f"{fs_root}/sma/NSE/*.parquet", ["sma_20", "sma_50", "sma_200"])
            rsi = _latest(f"{fs_root}/rsi/NSE/*.parquet", ["rsi_14"])
            adx = _latest(f"{fs_root}/adx/NSE/*.parquet", ["adx_14"])
            macd = _latest(
                f"{fs_root}/macd/NSE/*.parquet",
                ["macd_line", "macd_signal_9", "macd_histogram"],
            )
            bb = _latest(
                f"{fs_root}/bb/NSE/*.parquet",
                ["bb_middle_20", "bb_upper_20_2sd", "bb_lower_20_2sd"],
            )
            atr = conn.execute(f"""
                WITH ranked AS (
                    SELECT
                        symbol_id,
                        atr_14,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol_id ORDER BY timestamp DESC
                        ) AS rn
                    FROM read_parquet('{fs_root}/atr/NSE/*.parquet', union_by_name = true)
                    WHERE symbol_id IN ({sym_list})
                )
                SELECT
                    symbol_id,
                    MAX(atr_14) FILTER (WHERE rn = 1) AS atr_14,
                    AVG(atr_14) FILTER (WHERE rn BETWEEN 2 AND 21) AS atr_20_prev_avg
                FROM ranked
                GROUP BY symbol_id
            """).fetchdf()
        finally:
            conn.close()
        out = (
            sma.merge(rsi, on="symbol_id", how="outer")
            .merge(adx, on="symbol_id", how="outer")
            .merge(macd, on="symbol_id", how="outer")
            .merge(bb, on="symbol_id", how="outer")
            .merge(atr, on="symbol_id", how="outer")
        )
        return out
    except Exception as exc:
        LOG.warning("_load_indicators_for_symbols failed: %s", exc)
        return pd.DataFrame()


def _load_feature_technicals(root: Path, symbols: list[str]) -> pd.DataFrame:
    """Latest sma_20, sma_50, sma_150, adx_14, high_52w from feature store."""
    if not symbols:
        return pd.DataFrame()
    try:
        # Use the same latest-run resolver as the rest of the execution UI.
        # Alphabetic path sorting can accidentally select old ``ui-*`` runs.
        df = load_latest_operational_snapshot(root).frames.get("ranked_signals", pd.DataFrame())
        if df.empty:
            return pd.DataFrame()
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
        # Keep this endpoint aligned with /api/execution/market and workspace
        # snapshots: pick the newest live operational payload by mtime/control
        # plane metadata, not the lexicographically last artifact path.
        df = load_latest_operational_snapshot(root).frames.get("sector_dashboard", pd.DataFrame())
        if not df.empty:
            sector_rs = df.to_dict(orient="records")
    except Exception as exc:
        LOG.warning("sector_dashboard CSV load failed: %s", exc)

    # 2. Stage snapshot
    snap = _load_stage_snapshot(ohlcv_db)

    # 3. Stock→sector mapping from master DB (SQLite). The sector list API is
    #    led by sector_dashboard, but masterdata is the catalog of record; use
    #    it to keep sectors visible even if an RS artifact is incomplete.
    sector_map: dict[str, str] = {}
    sector_counts: dict[str, int] = {}
    try:
        import sqlite3
        con = sqlite3.connect(master_db)
        rows = pd.read_sql_query(
            "SELECT Symbol, Sector FROM stock_details WHERE exchange = 'NSE'", con
        )
        con.close()
        rows = rows.dropna(subset=["Sector"])
        rows = rows[rows["Sector"].astype(str).str.strip() != ""]
        sector_map = dict(zip(rows["Symbol"], rows["Sector"]))
        sector_counts = rows.groupby("Sector")["Symbol"].nunique().astype(int).to_dict()
    except Exception as exc:
        LOG.warning("sector_map load failed: %s", exc)

    # 4. Join stage labels onto sector map
    if not snap.empty and sector_map:
        snap = snap.copy()
        snap.loc[:, "sector"] = snap["symbol"].map(sector_map)
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
    seen_sectors: set[str] = set()
    for s in sector_rs:
        sec_name = s.get("Sector") or s.get("sector") or ""
        if sec_name:
            seen_sectors.add(sec_name)
        dist = stage_by_sector.get(sec_name, {})
        total = dist.get("total", 0)
        enriched.append({
            **s,
            "stage_s1_pct": _pct(dist.get("S1", 0), total),
            "stage_s2_pct": _pct(dist.get("S2", 0), total),
            "stage_s3_pct": _pct(dist.get("S3", 0), total),
            "stage_s4_pct": _pct(dist.get("S4", 0), total),
            "stage_s1_count": dist.get("S1", 0),
            "stage_s2_count": dist.get("S2", 0),
            "stage_s3_count": dist.get("S3", 0),
            "stage_s4_count": dist.get("S4", 0),
            "stage_total": total,
        })

    max_rank = max(
        (_safe_float(row.get("RS_rank")) or 0 for row in enriched),
        default=0,
    )
    for offset, sec_name in enumerate(sorted(set(sector_counts) - seen_sectors), start=1):
        dist = stage_by_sector.get(sec_name, {})
        total = dist.get("total", 0)
        enriched.append({
            "Sector": sec_name,
            "RS": None,
            "RS_20": None,
            "RS_50": None,
            "RS_100": None,
            "Momentum": 0.0,
            "RS_rank": int(max_rank) + offset,
            "RS_rank_pct": 1.0,
            "Momentum_rank": int(max_rank) + offset,
            "Momentum_rank_pct": 1.0,
            "Quadrant": "Unranked",
            "stage_s1_pct": _pct(dist.get("S1", 0), total),
            "stage_s2_pct": _pct(dist.get("S2", 0), total),
            "stage_s3_pct": _pct(dist.get("S3", 0), total),
            "stage_s4_pct": _pct(dist.get("S4", 0), total),
            "stage_s1_count": dist.get("S1", 0),
            "stage_s2_count": dist.get("S2", 0),
            "stage_s3_count": dist.get("S3", 0),
            "stage_s4_count": dist.get("S4", 0),
            "stage_total": total,
            "master_count": sector_counts[sec_name],
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

    # 2. Latest price + volume + 20d-avg volume + 52w high (from _catalog)
    price_df = _load_latest_technicals(ohlcv_db, symbols)

    # 3. Indicators from the feature store parquets — covers ALL symbols, not
    #    just the ranked subset (the prior version read only ranked_signals
    #    which left unranked sector stocks with stub indicators).
    ind_df = _load_indicators_for_symbols(root, symbols)

    # 4. Ranked-only scores (composite_score, rel_strength_score, return_20)
    #    from ranked_signals — populated only for stocks that made the rank cut.
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
                "close_20d_ago": _safe_float(r.get("close_20d_ago")),
                "volume": _safe_float(r.get("volume")),
                "vol_20_avg": _safe_float(r.get("vol_20_avg")),
                "high_52w": _safe_float(r.get("high_52w")),
                "date": str(r.get("date", "")),
            }

    ind_map: dict[str, dict] = {}
    if not ind_df.empty:
        for _, r in ind_df.iterrows():
            ind_map[r["symbol_id"]] = r.to_dict()

    feat_map: dict[str, dict] = {}
    if not feat_df.empty:
        for _, r in feat_df.iterrows():
            feat_map[r["symbol_id"]] = r.to_dict()

    constituents = []
    for _, stock in rows.iterrows():
        sym = stock["Symbol"]
        price_info = price_map.get(sym, {})
        ind_info = ind_map.get(sym, {})
        feat_info = feat_map.get(sym, {})
        stage_info = snap_map.get(sym, {"stage_label": None, "stage_confidence": None, "stage_week": None})

        close = price_info.get("close") or _safe_float(feat_info.get("close"))
        # Indicators: prefer feature-store values (cover all symbols); fall
        # back to ranked_signals only for ranked stocks (legacy behavior).
        sma20  = _safe_float(ind_info.get("sma_20"))  or _safe_float(feat_info.get("sma_20"))
        sma50  = _safe_float(ind_info.get("sma_50"))  or _safe_float(feat_info.get("sma_50"))
        sma200 = _safe_float(ind_info.get("sma_200")) or _safe_float(feat_info.get("sma_150"))
        adx    = _safe_float(ind_info.get("adx_14"))  or _safe_float(feat_info.get("adx_14"))
        rsi    = _safe_float(ind_info.get("rsi_14"))
        macd_hist = _safe_float(ind_info.get("macd_histogram"))
        bb_middle = _safe_float(ind_info.get("bb_middle_20"))
        bb_upper = _safe_float(ind_info.get("bb_upper_20_2sd"))
        bb_lower = _safe_float(ind_info.get("bb_lower_20_2sd"))
        atr = _safe_float(ind_info.get("atr_14"))
        atr_prev_avg = _safe_float(ind_info.get("atr_20_prev_avg"))
        high52w = price_info.get("high_52w") or _safe_float(feat_info.get("high_52w"))
        vol = price_info.get("volume")
        vol_avg = price_info.get("vol_20_avg") or _safe_float(feat_info.get("vol_20_avg"))
        rs_score = _safe_float(feat_info.get("rel_strength_score"))
        ret20 = _safe_float(feat_info.get("return_20"))
        composite = _safe_float(feat_info.get("composite_score"))
        close_20d_ago = price_info.get("close_20d_ago")
        if ret20 is None and close is not None and close_20d_ago and close_20d_ago > 0:
            ret20 = (close / close_20d_ago) - 1
        bb_width = (
            (bb_upper - bb_lower) / bb_middle
            if bb_middle and bb_upper is not None and bb_lower is not None
            else None
        )

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
            "sma_200": sma200,
            "high_52w": high52w,
            "adx_14": adx,
            "rsi_14": rsi,
            "macd_histogram": macd_hist,
            "bb_width": _safe_float(bb_width),
            "atr_14": atr,
            "vol_mult": round(vol / vol_avg, 2) if (vol and vol_avg and vol_avg > 0) else None,
            # Derived booleans
            "above_ma50": bool(close and sma50 and close > sma50),
            "above_ma200": bool(close and sma200 and close > sma200),
            "golden_cross": bool(sma20 and sma50 and sma20 > sma50),
            "near_52w_high": bool(close and high52w and high52w > 0 and (high52w - close) / high52w <= 0.05),
            "adx_above_20": bool(adx and adx > 20),
            "vol_expand": bool(vol and vol_avg and vol_avg > 0 and vol / vol_avg > 1.5),
            "macd_bullish": bool(macd_hist is not None and macd_hist > 0),
            "bb_squeeze": bool(bb_width is not None and bb_width <= 0.08),
            "atr_rising": bool(atr and atr_prev_avg and atr > atr_prev_avg),
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
