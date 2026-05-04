import os
import time
from datetime import datetime, timedelta
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import duckdb
import requests
from ai_trading_system.domains.ingest.nse_delivery_scraper import NseHistoricalDeliveryScraper
from ai_trading_system.domains.ingest.validation import validate_delivery_frame
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.platform.logging.logger import logger
from ai_trading_system.domains.ingest.series_policy import is_supported


class DeliveryCollector:
    """
    Downloads NSE delivery data and writes it into the shared delivery store.

    Supported sources:
      - `mto`: NSE MTO daily archive files
      - `nse_securitywise`: NSE security-wise historical CSV endpoint

    Storage:
      - DuckDB table: ohlcv.duckdb::_delivery (symbol_id, exchange, timestamp, delivery_pct, volume, delivery_qty)
      - Partitioned parquet: feature_store/delivery/NSE/data_*.parquet
    """

    MTO_URL = (
        "https://nsearchives.nseindia.com/archives/equities/mto/MTO_{date_str}.DAT"
    )
    NSE_HOME_URL = "https://www.nseindia.com/"
    ARCHIVE_REFERER = "https://www.nseindia.com/all-reports-derivatives"

    def __init__(
        self,
        ohlcv_db_path: str = None,
        feature_store_dir: str = None,
        raw_dir: str = None,
        data_domain: str = "operational",
        source: str = "mto",
        fallback_source: str | None = "nse_securitywise",
        masterdb_path: str | None = None,
    ):
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))
        paths = ensure_domain_layout(
            project_root=project_root,
            data_domain=data_domain,
        )
        if ohlcv_db_path is None:
            ohlcv_db_path = str(paths.ohlcv_db_path)
        if feature_store_dir is None:
            feature_store_dir = str(paths.feature_store_dir)
        if raw_dir is None:
            raw_dir = os.path.join(str(paths.root_dir), "raw", "NSE_MTO")
        self.ohlcv_db_path = ohlcv_db_path
        self.feature_store_dir = feature_store_dir
        self.raw_dir = raw_dir
        self.data_domain = data_domain
        self.source = source
        self.fallback_source = fallback_source
        self.masterdb_path = masterdb_path or str(paths.master_db_path)
        os.makedirs(raw_dir, exist_ok=True)

        self.session = requests.Session()
        self._bootstrap_session()
        self.security_scraper = NseHistoricalDeliveryScraper(
            masterdb_path=self.masterdb_path,
            raw_dir=os.path.join(str(paths.root_dir), "raw", "NSE_security_delivery"),
            data_domain=data_domain,
        )

    def _get_conn(self):
        return duckdb.connect(self.ohlcv_db_path)

    def _bootstrap_session(self) -> None:
        """Prime the session with browser-like headers and NSE cookies."""
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/plain,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": self.ARCHIVE_REFERER,
                "Connection": "keep-alive",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )
        try:
            self.session.get(self.NSE_HOME_URL, timeout=20)
        except requests.RequestException as exc:
            logger.debug("NSE session bootstrap failed: %s", exc)

    def _download_mto_file(self, url: str, date_str: str) -> bytes:
        """Download one MTO file with light retry/backoff and session refresh."""
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code in {401, 403}:
                    self._bootstrap_session()
                    time.sleep(min(2**attempt, 5))
                    resp = self.session.get(url, timeout=30)
                if resp.status_code == 404:
                    raise requests.HTTPError(
                        f"404 Not Found for {date_str}", response=resp
                    )
                resp.raise_for_status()
                return resp.content
            except requests.RequestException as exc:
                last_error = exc
                if getattr(exc, "response", None) is not None and exc.response.status_code == 404:
                    break
                time.sleep(min(2**attempt, 5))
        if last_error is None:
            raise RuntimeError(f"Failed to download MTO {date_str}")
        raise last_error

    def _ensure_delivery_table(self):
        conn = self._get_conn()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS _delivery (
                    symbol_id     VARCHAR,
                    exchange      VARCHAR DEFAULT 'NSE',
                    timestamp     DATE,
                    delivery_pct  DOUBLE,
                    volume        BIGINT,
                    delivery_qty  BIGINT,
                    PRIMARY KEY (symbol_id, exchange, timestamp)
                )
            """)
        finally:
            conn.close()

    def _prev_trading_day(self, dt: datetime) -> datetime:
        """Get the previous trading day (skip Sat/Sun)."""
        d = dt - timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d

    def fetch_bhavcopy(self, date: datetime) -> pd.DataFrame:
        """
        Download and parse a single NSE MTO file for the given date.
        Returns DataFrame with: symbol_id, exchange, timestamp, delivery_pct, volume, delivery_qty
        """
        date_str = date.strftime("%d%m%Y")
        url = self.MTO_URL.format(date_str=date_str)
        mto_path = os.path.join(self.raw_dir, f"MTO_{date_str}.DAT")

        try:
            if not os.path.exists(mto_path):
                logger.info(f"Downloading MTO {date_str}...")
                content = self._download_mto_file(url, date_str)
                with open(mto_path, "wb") as f:
                    f.write(content)

            with open(mto_path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
        except requests.HTTPError as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code == 404:
                logger.info("MTO %s not available in archive", date_str)
            else:
                logger.warning(f"Failed MTO {date_str}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"Failed MTO {date_str}: {e}")
            return pd.DataFrame()

        data_lines = []
        for line in lines:
            parts = line.strip().split(",")
            if len(parts) < 6:
                continue
            if parts[0] != "20":
                continue
            series = parts[3].strip()
            if not is_supported(series):
                continue
            try:
                symbol = parts[2].strip()
                qty_traded = int(parts[4].strip())
                deliv_qty = int(parts[5].strip())
                deliv_pct = (
                    float(parts[6].strip())
                    if len(parts) > 6 and parts[6].strip()
                    else None
                )
                data_lines.append((symbol, qty_traded, deliv_qty, deliv_pct))
            except (ValueError, IndexError):
                continue

        if not data_lines:
            logger.warning(f"No EQ records found in MTO {date_str}")
            return pd.DataFrame()

        df = pd.DataFrame(
            data_lines, columns=["symbol_id", "volume", "delivery_qty", "delivery_pct"]
        )
        df["exchange"] = "NSE"
        df["timestamp"] = pd.to_datetime(date.date())

        ts_str = date.strftime("%d%b%Y").upper()
        valid = df[df["delivery_pct"].notna()]
        avg_del = valid["delivery_pct"].mean() if len(valid) > 0 else 0
        logger.info(
            f"  MTO {ts_str}: {len(df)} EQ records, avg delivery {avg_del:.1f}%"
        )
        return df[
            [
                "symbol_id",
                "exchange",
                "timestamp",
                "delivery_pct",
                "volume",
                "delivery_qty",
            ]
        ]

    def fetch_range(
        self,
        from_date: str,
        to_date: str,
        n_workers: int = 4,
        symbols: list[str] | None = None,
        save_raw: bool = False,
    ) -> int:
        """
        Download bhavcopy for all trading days in [from_date, to_date].
        Returns number of records ingested.
        """
        self._ensure_delivery_table()
        if self.source == "nse_securitywise":
            return self._fetch_range_securitywise(
                from_date=from_date,
                to_date=to_date,
                n_workers=n_workers,
                symbols=symbols,
                save_raw=save_raw,
            )

        dates = pd.bdate_range(from_date, to_date)
        total = 0
        missing_dates: list[pd.Timestamp] = []

        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            futures = {ex.submit(self.fetch_bhavcopy, d): d for d in dates}
            for fut in as_completed(futures):
                df = fut.result()
                if df.empty:
                    missing_dates.append(futures[fut])
                    continue
                total += self._upsert_delivery(df)

        if missing_dates and self.fallback_source == "nse_securitywise":
            fallback_from = min(missing_dates).strftime("%Y-%m-%d")
            fallback_to = max(missing_dates).strftime("%Y-%m-%d")
            logger.warning(
                "MTO missed %s trading day(s); falling back to NSE security-wise delivery for %s -> %s",
                len(missing_dates),
                fallback_from,
                fallback_to,
            )
            total += self._fetch_range_securitywise(
                from_date=fallback_from,
                to_date=fallback_to,
                n_workers=n_workers,
                symbols=symbols,
                save_raw=save_raw,
            )

        logger.info(f"Delivery fetch complete: {total} records inserted")
        return total

    def _fetch_range_securitywise(
        self,
        from_date: str,
        to_date: str,
        n_workers: int = 4,
        symbols: list[str] | None = None,
        save_raw: bool = False,
    ) -> int:
        """Backfill delivery data using the NSE security-wise historical endpoint."""
        target_symbols = symbols or self.security_scraper.get_nse_symbols()
        total = 0

        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            futures = {
                ex.submit(
                    self.security_scraper.fetch_symbol_history,
                    symbol,
                    from_date,
                    to_date,
                    save_raw,
                ): symbol
                for symbol in target_symbols
            }
            for fut in as_completed(futures):
                symbol = futures[fut]
                try:
                    df = fut.result()
                except Exception as exc:
                    logger.warning("Security-wise delivery fetch failed for %s: %s", symbol, exc)
                    continue
                if df.empty:
                    continue
                total += self._upsert_delivery(df)

        logger.info(
            "Security-wise delivery fetch complete: %s rows inserted for %s symbols",
            total,
            len(target_symbols),
        )
        return total

    def _upsert_delivery(self, df: pd.DataFrame) -> int:
        """Upsert delivery data into DuckDB."""
        if df.empty:
            return 0
        validated = validate_delivery_frame(
            df,
            source_label="delivery_collector._upsert_delivery",
        )
        conn = self._get_conn()
        try:
            conn.execute("CREATE TEMP VIEW _tmp_delivery AS SELECT * FROM validated")
            conn.execute("""
                INSERT INTO _delivery (symbol_id, exchange, timestamp, delivery_pct, volume, delivery_qty)
                SELECT symbol_id, exchange, timestamp::DATE, delivery_pct,
                       COALESCE(volume, 0)::BIGINT, COALESCE(delivery_qty, 0)::BIGINT
                FROM _tmp_delivery
                ON CONFLICT (symbol_id, exchange, timestamp)
                DO UPDATE SET
                    delivery_pct = EXCLUDED.delivery_pct,
                    volume = EXCLUDED.volume,
                    delivery_qty = EXCLUDED.delivery_qty
            """)
            conn.execute("DROP VIEW _tmp_delivery")
        finally:
            conn.close()
        return len(validated)

    def compute_delivery_features(self, exchange: str = "NSE") -> int:
        """
        Compute delivery-based features from _delivery table and write partitioned parquet.
        Features:
          - delivery_pct: raw delivery %
          - delivery_5d_avg: 5-day rolling avg delivery %
          - delivery_20d_avg: 20-day rolling avg delivery %
          - delivery_pctile: percentile rank of today's delivery %
        Returns total rows written.
        """
        conn = self._get_conn()
        try:
            df = conn.execute(f"""
                SELECT symbol_id, exchange, timestamp::DATE AS timestamp,
                       delivery_pct, volume, delivery_qty
                FROM _delivery
                WHERE exchange = '{exchange}'
                ORDER BY symbol_id, timestamp
            """).fetchdf()
        finally:
            conn.close()

        df["timestamp"] = pd.to_datetime(df["timestamp"])

        df = df.sort_values(["symbol_id", "timestamp"])

        for w, col in [(5, "delivery_5d_avg"), (20, "delivery_20d_avg")]:
            df[col] = df.groupby("symbol_id")["delivery_pct"].transform(
                lambda x: x.rolling(w, min_periods=1).mean()
            )

        df["delivery_pctile"] = (
            df.groupby("timestamp")["delivery_pct"].rank(pct=True) * 100
        )

        df = df.dropna(subset=["delivery_pct"])

        out_dir = os.path.join(self.feature_store_dir, "delivery", exchange)
        os.makedirs(out_dir, exist_ok=True)

        n_files = 6
        import numpy as np

        df["_partition"] = np.arange(len(df)) % n_files

        total = 0
        for i in range(n_files):
            part = df[df["_partition"] == i].drop(columns=["_partition"])
            if part.empty:
                continue
            out_path = os.path.join(out_dir, f"data_{i}.parquet")
            part.to_parquet(out_path, index=False)
            total += len(part)

        logger.info(f"Delivery features: {total} rows written to {out_dir}")
        return total

    def get_last_delivery_date(self) -> Optional[str]:
        """Get the most recent delivery data date in the database."""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT MAX(timestamp) FROM _delivery WHERE exchange = 'NSE'"
            ).fetchone()
            return str(row[0])[:10] if row and row[0] else None
        finally:
            conn.close()

    def get_delivery_for_symbol(
        self,
        symbol: str,
        from_date: str = None,
        to_date: str = None,
    ) -> pd.DataFrame:
        """Get delivery data for a specific symbol."""
        conn = self._get_conn()
        try:
            q = f"""
                SELECT timestamp::DATE AS date, delivery_pct, delivery_5d_avg,
                       delivery_20d_avg, delivery_pctile, volume
                FROM _delivery
                WHERE symbol_id = '{symbol}' AND exchange = 'NSE'
            """
            if from_date:
                q += f" AND timestamp >= '{from_date}'"
            if to_date:
                q += f" AND timestamp <= '{to_date}'"
            q += " ORDER BY timestamp"
            return conn.execute(q).fetchdf()
        finally:
            conn.close()
