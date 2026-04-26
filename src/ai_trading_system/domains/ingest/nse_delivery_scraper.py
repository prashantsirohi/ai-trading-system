"""NSE security-wise delivery scraper for historical delivery backfills."""

from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests

from ai_trading_system.platform.utils.bootstrap import ensure_project_root_on_path
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.platform.logging.logger import logger


def _resolve_project_root(anchor: str | Path) -> Path:
    env_root = os.getenv("AI_TRADING_PROJECT_ROOT")
    if env_root:
        root = Path(env_root).resolve()
        if root.exists():
            ensure_project_root_on_path(root)
            return root

    anchor_path = Path(anchor).resolve()
    for parent in anchor_path.parents:
        if parent.name == "src" and (parent / "ai_trading_system").exists():
            root = parent.parent
            ensure_project_root_on_path(root)
            return root
    return ensure_project_root_on_path(anchor_path)


class NseHistoricalDeliveryScraper:
    """Fetch delivery history from NSE's security-wise historical CSV endpoint."""

    API_URL = (
        "https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData"
    )
    NSE_HOME_URL = "https://www.nseindia.com/"
    REFERER = "https://www.nseindia.com/report-detail/eq_security"

    def __init__(
        self,
        masterdb_path: str | None = None,
        raw_dir: str | None = None,
        data_domain: str = "operational",
        session: requests.Session | None = None,
    ) -> None:
        paths = ensure_domain_layout(
            project_root=_resolve_project_root(__file__),
            data_domain=data_domain,
        )
        self.masterdb_path = masterdb_path or str(paths.master_db_path)
        if raw_dir is None:
            raw_dir = os.path.join(str(paths.root_dir), "raw", "NSE_security_delivery")
        self.raw_dir = raw_dir
        self.data_domain = data_domain
        os.makedirs(self.raw_dir, exist_ok=True)

        self.session = session or requests.Session()
        self._bootstrap_session()

    def _bootstrap_session(self) -> None:
        """Prime an NSE session with browser-like headers and cookies."""
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": "text/csv,text/plain,application/json,text/html,*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": self.REFERER,
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )
        try:
            self.session.get(self.NSE_HOME_URL, timeout=20)
        except requests.RequestException as exc:
            logger.debug("NSE delivery session bootstrap failed: %s", exc)

    def get_nse_symbols(self, limit: int | None = None) -> list[str]:
        """Load NSE symbols from the canonical master DB."""
        conn = sqlite3.connect(self.masterdb_path)
        try:
            query = """
                SELECT Symbol
                FROM stock_details
                WHERE exchange = 'NSE'
                  AND Symbol IS NOT NULL
                  AND trim(Symbol) <> ''
                ORDER BY Symbol
            """
            if limit:
                query += f" LIMIT {int(limit)}"
            return [row[0] for row in conn.execute(query).fetchall()]
        finally:
            conn.close()

    def _iter_year_chunks(self, from_date: str, to_date: str) -> Iterable[tuple[str, str]]:
        start_dt = datetime.fromisoformat(from_date).date()
        end_dt = datetime.fromisoformat(to_date).date()
        for year in range(start_dt.year, end_dt.year + 1):
            year_start = max(start_dt, datetime(year, 1, 1).date())
            year_end = min(end_dt, datetime(year, 12, 31).date())
            yield year_start.strftime("%d-%m-%Y"), year_end.strftime("%d-%m-%Y")

    def _download_chunk(self, symbol: str, from_date: str, to_date: str) -> pd.DataFrame:
        """Download one symbol/date chunk with retry and session refresh."""
        url = (
            f"{self.API_URL}?from={from_date}&to={to_date}&symbol={symbol}"
            "&type=priceVolumeDeliverable&series=ALL&csv=true"
        )
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code in {401, 403}:
                    self._bootstrap_session()
                    time.sleep(min(2**attempt, 5))
                    response = self.session.get(url, timeout=30)
                if response.status_code == 404:
                    logger.info(
                        "NSE delivery data unavailable for %s %s -> %s",
                        symbol,
                        from_date,
                        to_date,
                    )
                    return pd.DataFrame()
                response.raise_for_status()
                return pd.read_csv(StringIO(response.text))
            except requests.RequestException as exc:
                last_error = exc
                time.sleep(min(2**attempt, 5))
            except pd.errors.EmptyDataError:
                return pd.DataFrame()

        logger.warning(
            "Failed to fetch NSE delivery for %s %s -> %s: %s",
            symbol,
            from_date,
            to_date,
            last_error,
        )
        return pd.DataFrame()

    def normalize_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map raw NSE CSV output into the shared delivery schema."""
        if df is None or df.empty:
            return pd.DataFrame(
                columns=[
                    "symbol_id",
                    "exchange",
                    "timestamp",
                    "delivery_pct",
                    "volume",
                    "delivery_qty",
                ]
            )

        normalized = df.copy()
        normalized.columns = [
            (
                str(col)
                .replace("ï»¿", "")
                .replace("\ufeff", "")
                .replace('"', "")
                .replace("₹", "")
                .strip()
            )
            for col in normalized.columns
        ]
        normalized = normalized.rename(
            columns={
                "Symbol": "symbol_id",
                "Symbol  ": "symbol_id",
                "Series": "series",
                "Series  ": "series",
                "Date": "timestamp",
                "Date  ": "timestamp",
                "Total Traded Quantity": "volume",
                "Total Traded Quantity  ": "volume",
                "Deliverable Qty": "delivery_qty",
                "Deliverable Qty  ": "delivery_qty",
                "% Dly Qt to Traded Qty": "delivery_pct",
                "% Dly Qt to Traded Qty  ": "delivery_pct",
            }
        )
        if "series" in normalized.columns:
            normalized = normalized[normalized["series"] == "EQ"]
        if normalized.empty:
            return pd.DataFrame(
                columns=[
                    "symbol_id",
                    "exchange",
                    "timestamp",
                    "delivery_pct",
                    "volume",
                    "delivery_qty",
                ]
            )

        normalized.loc[:, "exchange"] = "NSE"
        normalized.loc[:, "timestamp"] = pd.to_datetime(
            normalized["timestamp"], dayfirst=True, errors="coerce"
        )
        for col in ["delivery_pct", "volume", "delivery_qty"]:
            if col in normalized.columns:
                normalized.loc[:, col] = (
                    normalized[col]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .str.strip()
                )
        normalized.loc[:, "delivery_pct"] = pd.to_numeric(
            normalized.get("delivery_pct"), errors="coerce"
        )
        normalized.loc[:, "volume"] = pd.to_numeric(normalized.get("volume"), errors="coerce")
        normalized.loc[:, "delivery_qty"] = pd.to_numeric(
            normalized.get("delivery_qty"), errors="coerce"
        )
        normalized = normalized.dropna(subset=["symbol_id", "timestamp", "delivery_pct"])
        volume = pd.to_numeric(normalized["volume"], errors="coerce")
        delivery_qty = pd.to_numeric(normalized["delivery_qty"], errors="coerce")
        normalized.loc[:, "volume"] = volume.where(volume.notna(), 0).astype("int64")
        normalized.loc[:, "delivery_qty"] = delivery_qty.where(delivery_qty.notna(), 0).astype("int64")
        normalized.loc[:, "symbol_id"] = normalized["symbol_id"].astype(str).str.strip()
        normalized = normalized.drop_duplicates(subset=["symbol_id", "exchange", "timestamp"])
        return normalized[
            ["symbol_id", "exchange", "timestamp", "delivery_pct", "volume", "delivery_qty"]
        ].reset_index(drop=True)

    def fetch_symbol_history(
        self,
        symbol: str,
        from_date: str,
        to_date: str,
        save_raw: bool = False,
    ) -> pd.DataFrame:
        """Fetch and normalize delivery history for one symbol across yearly chunks."""
        frames: list[pd.DataFrame] = []
        raw_frames: list[pd.DataFrame] = []
        for chunk_from, chunk_to in self._iter_year_chunks(from_date, to_date):
            raw = self._download_chunk(symbol=symbol, from_date=chunk_from, to_date=chunk_to)
            if raw.empty:
                continue
            raw_frames.append(raw)
            frames.append(self.normalize_frame(raw))

        if not frames:
            return pd.DataFrame(
                columns=[
                    "symbol_id",
                    "exchange",
                    "timestamp",
                    "delivery_pct",
                    "volume",
                    "delivery_qty",
                ]
            )

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(subset=["symbol_id", "exchange", "timestamp"])
        if save_raw and raw_frames:
            raw_path = Path(self.raw_dir) / f"{symbol}_{from_date}_to_{to_date}.csv"
            pd.concat(raw_frames, ignore_index=True).to_csv(raw_path, index=False)
        logger.info(
            "Fetched NSE security-wise delivery for %s: %s rows",
            symbol,
            len(combined),
        )
        return combined
