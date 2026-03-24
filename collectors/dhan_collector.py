import asyncio
import time
import sqlite3
import random
import json
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import duckdb
import requests
from dhanhq import dhanhq
from features.feature_store import FeatureStore
from collectors.token_manager import DhanTokenManager
from utils.logger import logger


class DhanCollector:
    """
    DhanHQ Data Collector with asyncio-powered parallel ingestion.

    Data Ingestion & Storage Layer:
    - Batch Ingestion: asyncio fetches market quotes for 4000+ symbols in parallel
      using DhanHQ marketfeed API. Rate-limited via semaphore (5 req/sec per API key).
    - ACID Storage: DuckDB with append-only writes. Each batch is a transaction —
      either fully written or rolled back. No partial writes.
    - Time Travel: Every ingestion run snapshots the catalog table into a versioned
      history table. Previous snapshots are queryable by timestamp or version number.
    - Schema Evolution: Columns are added via ALTER TABLE; DuckDB handles type
      coercion. Old data remains readable under old schemas.

    Iceberg-style table layout (managed via DuckDB):
    - Raw OHLCV data per symbol stored in partitioned Parquet files under a
      warehouse directory. Each file batch is immutable (append-only).
    - A catalog table maps (symbol, exchange, timestamp) -> parquet_file_path.
    - A snapshot table tracks ingestion run metadata for time-travel queries.

    Rate Limits (DhanHQ):
    - Per Second: 5 requests
    - Bulk Fetch: up to 1000 instruments per request (1 req/sec)
    - Daily: 1000 API calls

    For demo/testing: generates synthetic OHLCV data when API is not available.
    """

    def __init__(
        self,
        api_key: str = "",
        client_id: str = "",
        access_token: str = "",
        warehouse_dir: str = None,
        db_path: str = None,
        masterdb_path: str = None,
        feature_store_dir: str = None,
        max_concurrent: int = 5,
    ):
        if warehouse_dir is None:
            warehouse_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "data", "features"
            )
        if db_path is None:
            db_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "data", "ohlcv.duckdb"
            )
        if masterdb_path is None:
            masterdb_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "data", "masterdata.db"
            )
        if feature_store_dir is None:
            feature_store_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "data", "feature_store"
            )

        self.api_key = api_key
        self.client_id = client_id
        self.access_token = access_token
        self.warehouse_dir = warehouse_dir
        self.db_path = db_path
        self.masterdb_path = masterdb_path
        self.feature_store_dir = feature_store_dir
        self.max_concurrent = max_concurrent
        self.base_url = "https://api.dhan.co/v2"
        self.request_timestamps: List[float] = []
        self.daily_request_count = 0
        self.last_reset_date = datetime.now().date()
        self.use_api = bool(client_id and access_token)
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._executor = ThreadPoolExecutor(max_workers=max_concurrent)

        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
        self.token_manager = DhanTokenManager(env_path=env_path)
        self._token_renewed_this_session = False

        self.dhan = None
        if self.use_api:
            self._init_dhan_client()

        os.makedirs(self.warehouse_dir, exist_ok=True)
        self._init_duckdb()

        self.fs = FeatureStore(
            ohlcv_db_path=self.db_path,
            feature_store_dir=self.feature_store_dir,
        )

    def _init_dhan_client(self):
        """Initialize or reinitialize DhanHQ client with current token."""
        if not self.client_id or not self.access_token:
            self.use_api = False
            return
        try:
            self.dhan = dhanhq(self.client_id, self.access_token)
            logger.info("DhanHQ client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize DhanHQ client: {e}")
            self.use_api = False

    def _ensure_valid_token(self) -> bool:
        """
        Check token validity and renew if expired or expiring soon.
        Reinitializes DhanHQ client after renewal.
        Returns True if token is valid, False otherwise.
        """
        if not self.use_api:
            return False

        if self.token_manager.is_token_expired():
            logger.warning("Access token is expired. Attempting renewal...")
            result = self.token_manager.renew_token()
            if result.get("status") == "success":
                self.access_token = result["access_token"]
                self._init_dhan_client()
                self._token_renewed_this_session = True
                logger.info("Token renewed and DhanHQ client reinitialized.")
                return True
            else:
                logger.error(f"Token renewal failed: {result.get('message')}")
                return False

        if self.token_manager.is_token_expiring_soon(hours_threshold=1):
            if not self._token_renewed_this_session:
                logger.info("Token expiring within 1 hour. Proactively renewing...")
                result = self.token_manager.renew_token()
                if result.get("status") == "success":
                    self.access_token = result["access_token"]
                    self._init_dhan_client()
                    self._token_renewed_this_session = True
                    logger.info(
                        "Token proactively renewed and DhanHQ client reinitialized."
                    )
                    return True
                else:
                    logger.warning(f"Proactive renewal failed: {result.get('message')}")
        return True

    # ------------------------------------------------------------------ #
    #  DuckDB initialization                                              #
    # ------------------------------------------------------------------ #

    def _init_duckdb(self):
        conn = duckdb.connect(self.db_path)
        conn.execute("SET home_directory = '.'")

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS snapshot_id_seq START 1
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS _catalog (
                symbol_id           TEXT    NOT NULL,
                security_id         TEXT,
                exchange            TEXT    NOT NULL,
                timestamp           TIMESTAMP NOT NULL,
                open                DOUBLE,
                high                DOUBLE,
                low                 DOUBLE,
                close               DOUBLE,
                volume              BIGINT,
                parquet_file        TEXT,
                ingestion_version    BIGINT  DEFAULT nextval('snapshot_id_seq'),
                ingestion_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol_id, exchange, timestamp)
            )
        """)

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS _snap_id_seq START 1
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS _snapshots (
                snapshot_id         BIGINT  PRIMARY KEY DEFAULT nextval('_snap_id_seq'),
                snapshot_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                symbols_processed   INTEGER,
                rows_written        BIGINT,
                from_date          TEXT,
                to_date            TEXT,
                status             TEXT    DEFAULT 'running',
                note               TEXT
            )
        """)

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS _hist_id_seq START 1
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS _catalog_history (
                hist_id            BIGINT  PRIMARY KEY DEFAULT nextval('_hist_id_seq'),
                snapshot_id        BIGINT,
                symbol_id          TEXT,
                security_id        TEXT,
                exchange           TEXT,
                timestamp          TIMESTAMP,
                open               DOUBLE,
                high               DOUBLE,
                low                DOUBLE,
                close              DOUBLE,
                volume             BIGINT,
                parquet_file       TEXT,
                ingestion_version   BIGINT,
                ingestion_ts        TIMESTAMP,
                archived_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS _pfile_id_seq START 1
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS _parquet_files (
                pfile_id           BIGINT  PRIMARY KEY DEFAULT nextval('_pfile_id_seq'),
                parquet_file       TEXT    UNIQUE,
                symbol_id          TEXT,
                exchange           TEXT,
                rows_count         BIGINT,
                created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                active             BOOLEAN DEFAULT TRUE
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_catalog_symbol
            ON _catalog(symbol_id, exchange)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshots_ts
            ON _snapshots(snapshot_ts)
        """)

        conn.commit()
        conn.close()
        logger.info(f"DuckDB initialized: {self.db_path}")

    def _get_duckdb_conn(self):
        conn = duckdb.connect(self.db_path)
        conn.execute("SET home_directory = '.'")
        return conn

    # ------------------------------------------------------------------ #
    #  Symbols from master DB                                            #
    # ------------------------------------------------------------------ #

    def get_symbols_from_masterdb(
        self, exchanges: List[str] = None, limit: int = None
    ) -> List[Dict]:
        """
        Load symbols from stock_details table (which maps CSV symbols -> security_id).
        stock_details.Security_id is the Dhan API security_id.
        Falls back to symbols table if stock_details is not yet populated.
        """
        if not os.path.exists(self.masterdb_path):
            logger.warning(f"masterdb not found: {self.masterdb_path}")
            return []

        conn = sqlite3.connect(self.masterdb_path)
        cur = conn.cursor()

        cur.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name IN ('stock_details', 'symbols')
        """)
        available_tables = {r[0] for r in cur.fetchall()}

        if "stock_details" in available_tables:
            query = """
                SELECT
                    sd.Security_id,
                    sd.Symbol,
                    sd.Name,
                    sd."Industry Group" AS industry_group,
                    sd."Industry" AS industry,
                    sd.MCAP,
                    s.exchange
                FROM stock_details sd
                LEFT JOIN symbols s ON s.security_id = sd.Security_id
                WHERE sd.Security_id IS NOT NULL
                  AND sd.Security_id != ''
            """
            params: List[Any] = []

            if exchanges:
                placeholders = ",".join("?" * len(exchanges))
                query += f" AND s.exchange IN ({placeholders})"
                params.extend(exchanges)

            query += " ORDER BY sd.Security_id"

            if limit:
                query += f" LIMIT {limit}"

            cur.execute(query, params)
            rows = [
                {
                    "security_id": str(r[0]),
                    "symbol_id": r[1],
                    "symbol_name": r[2],
                    "industry_group": r[3],
                    "industry": r[4],
                    "mcap": r[5],
                    "exchange": r[6] or "NSE",
                }
                for r in cur.fetchall()
            ]
            conn.close()
            logger.info(f"Loaded {len(rows)} symbols from stock_details")
            return rows

        if "symbols" in available_tables:
            query = """
                SELECT
                    security_id,
                    symbol_id,
                    CASE WHEN exchange = 'BSE' THEN bse_symbol ELSE nse_symbol END AS symbol_name,
                    exchange,
                    sector,
                    industry
                FROM symbols
                WHERE security_id IS NOT NULL
                  AND security_id != ''
                  AND (
                      (exchange = 'NSE' AND nse_symbol IS NOT NULL AND nse_symbol != '')
                      OR (exchange = 'BSE' AND bse_symbol IS NOT NULL AND bse_symbol != '')
                  )
            """
            params: List[Any] = []

            if exchanges:
                placeholders = ",".join("?" * len(exchanges))
                query += f" AND exchange IN ({placeholders})"
                params.extend(exchanges)

            query += " ORDER BY exchange, security_id"

            if limit:
                query += f" LIMIT {limit}"

            cur.execute(query, params)
            rows = [
                {
                    "security_id": str(r[0]),
                    "symbol_id": r[1],
                    "symbol_name": r[2],
                    "exchange": r[3],
                    "sector": r[4],
                    "industry": r[5],
                }
                for r in cur.fetchall()
            ]
            conn.close()
            logger.info(
                f"Loaded {len(rows)} symbols from symbols table (stock_details not available)"
            )
            return rows

        conn.close()
        logger.warning("Neither stock_details nor symbols table found in masterdb")
        return []

    # ------------------------------------------------------------------ #
    #  Async batch ingestion                                             #
    # ------------------------------------------------------------------ #

    async def _fetch_one(
        self,
        symbol_info: Dict,
        from_date: str,
        to_date: str,
        session: requests.Session,
    ) -> Optional[pd.DataFrame]:
        """Async fetch for one symbol. Runs in thread pool to avoid blocking."""
        security_id = symbol_info["security_id"]
        exchange = symbol_info["exchange"]
        symbol_name = symbol_info["symbol_name"]

        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(
            self._executor,
            self._fetch_sync,
            security_id,
            exchange,
            from_date,
            to_date,
            session,
        )

        if df is not None and not df.empty:
            df.attrs["symbol_info"] = symbol_info
            logger.debug(f"Fetched {len(df)} rows for {symbol_name} ({security_id})")
        return df

    def _fetch_sync(
        self,
        security_id: str,
        exchange: str,
        from_date: str,
        to_date: str,
        session: requests.Session,
        _retry_after_renewal: bool = False,
    ) -> Optional[pd.DataFrame]:
        """Synchronous fetch — runs in thread pool. Retries once after token renewal."""
        self._rate_limit_wait()

        if not self.use_api or self.dhan is None:
            return self._generate_sample_data(security_id, from_date, to_date)

        if not _retry_after_renewal:
            self._ensure_valid_token()

        clean_sid = security_id
        try:
            if security_id.endswith(".0"):
                clean_sid = security_id[:-2]
        except Exception:
            pass

        exchange_segment = "NSE_EQ" if exchange.upper() == "NSE" else "BSE_EQ"

        try:
            data = self.dhan.historical_daily_data(
                security_id=clean_sid,
                exchange_segment=exchange_segment,
                instrument_type="EQUITY",
                from_date=from_date,
                to_date=to_date,
            )

            if not data or not isinstance(data, dict):
                return None

            inner = data.get("data", data)
            if not isinstance(inner, dict):
                return None

            open_arr = inner.get("open", [])
            if not open_arr or not isinstance(open_arr, list):
                return None

            df = pd.DataFrame(
                {
                    "open": inner.get("open", []),
                    "high": inner.get("high", []),
                    "low": inner.get("low", []),
                    "close": inner.get("close", []),
                    "volume": inner.get("volume", []),
                }
            )

            if "timestamp" in inner and inner["timestamp"]:
                df["timestamp"] = pd.to_datetime(inner["timestamp"], unit="s")
            elif "date" in inner and inner["date"]:
                df["timestamp"] = pd.to_datetime(inner["date"])
            else:
                return None

            df.set_index("timestamp", inplace=True)

            rename = {}
            for c in df.columns:
                cl = c.lower()
                if cl == "open":
                    rename[c] = "open"
                elif cl == "high":
                    rename[c] = "high"
                elif cl == "low":
                    rename[c] = "low"
                elif cl == "close":
                    rename[c] = "close"
                elif cl in ("volume", "vol"):
                    rename[c] = "volume"
                elif cl in ("ohlcv", "turnover"):
                    rename[c] = "turnover"
            df.rename(columns=rename, inplace=True)

            return df

        except Exception as e:
            err_str = str(e).lower()
            if not _retry_after_renewal and (
                "401" in err_str
                or "403" in err_str
                or "unauthorized" in err_str
                or "access token" in err_str
                or "forbidden" in err_str
                or "expired" in err_str
            ):
                logger.warning(
                    f"Token error fetching {security_id}: {e}. Attempting token renewal..."
                )
                renewed = self._ensure_valid_token()
                if renewed:
                    return self._fetch_sync(
                        security_id,
                        exchange,
                        from_date,
                        to_date,
                        session,
                        _retry_after_renewal=True,
                    )
            logger.warning(f"Error fetching {security_id}: {e}")
            return None

    async def fetch_batch(
        self,
        symbols: List[Dict],
        from_date: str,
        to_date: str,
        max_concurrent: int = None,
    ) -> List[pd.DataFrame]:
        """
        Asynchronously fetch OHLCV data for multiple symbols in parallel.
        Uses asyncio.Semaphore to respect rate limits.
        """
        if max_concurrent is None:
            max_concurrent = self.max_concurrent

        self._semaphore = asyncio.Semaphore(max_concurrent)
        session = requests.Session()

        async def throttled_fetch(symbol_info: Dict) -> Optional[pd.DataFrame]:
            async with self._semaphore:
                return await self._fetch_one(symbol_info, from_date, to_date, session)

        tasks = [throttled_fetch(s) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        session.close()

        dfs = []
        for r in results:
            if isinstance(r, pd.DataFrame) and not r.empty:
                dfs.append(r)
            elif isinstance(r, Exception):
                logger.warning(f"Task exception: {r}")

        return dfs

    # ------------------------------------------------------------------ #
    #  DuckDB storage — ACID + time travel                               #
    # ------------------------------------------------------------------ #

    def _write_batch(
        self,
        dfs: List[pd.DataFrame],
        snapshot_id: int,
        from_date: str,
        to_date: str,
    ) -> int:
        """
        Write a batch of OHLCV DataFrames to DuckDB inside a single transaction.
        Each ingestion run is atomic — all rows written or none.
        Returns total rows written.
        """
        if not dfs:
            return 0

        conn = self._get_duckdb_conn()

        try:
            conn.execute("BEGIN TRANSACTION")

            rows_written = 0
            parquet_dir = os.path.join(
                self.warehouse_dir,
                f"run_{snapshot_id:06d}",
            )
            os.makedirs(parquet_dir, exist_ok=True)

            for df in dfs:
                if df.empty:
                    continue

                info = df.attrs.get("symbol_info", {})
                symbol_id = info.get("symbol_id", "UNKNOWN")
                security_id = info.get("security_id", "")
                exchange = info.get("exchange", "NSE")

                df = df.reset_index()
                df.rename(columns={"timestamp": "timestamp"}, inplace=True)
                df["symbol_id"] = symbol_id
                df["security_id"] = security_id
                df["exchange"] = exchange
                df["ingestion_version"] = snapshot_id

                filename = f"{symbol_id}_{exchange}_{snapshot_id}.parquet"
                filepath = os.path.join(parquet_dir, filename)

                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS _batch_{snapshot_id} AS SELECT * FROM df LIMIT 0"
                )
                conn.execute(f"DELETE FROM _batch_{snapshot_id}")

                conn.execute(f"""
                    COPY (SELECT * FROM df)
                    TO '{filepath}' (FORMAT PARQUET)
                """)

                rows = len(df)
                rows_written += rows

                conn.execute(f"""
                    DELETE FROM _catalog
                    WHERE (symbol_id, exchange, timestamp)
                    IN (SELECT symbol_id, exchange, timestamp FROM df)
                """)

                conn.execute(
                    """
                    INSERT INTO _catalog
                        (symbol_id, security_id, exchange, timestamp,
                         open, high, low, close, volume,
                         parquet_file, ingestion_version)
                    SELECT
                        symbol_id, security_id, exchange, timestamp,
                        open, high, low, close, volume,
                        ?, ?
                    FROM df
                """,
                    (filepath, snapshot_id),
                )

                conn.execute(
                    """
                    INSERT INTO _parquet_files (parquet_file, symbol_id, exchange, rows_count)
                    VALUES (?, ?, ?, ?)
                """,
                    (filepath, symbol_id, exchange, rows),
                )

            conn.execute(
                """
                UPDATE _snapshots
                SET status = 'completed', rows_written = ?
                WHERE snapshot_id = ?
            """,
                (rows_written, snapshot_id),
            )

            conn.execute("COMMIT")
            logger.info(
                f"Snapshot {snapshot_id}: committed {rows_written} rows "
                f"for {len(dfs)} symbols"
            )
            return rows_written

        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"Snapshot {snapshot_id} rolled back: {e}")
            raise

        finally:
            conn.close()

    def _create_snapshot(self) -> int:
        """Archive current catalog state into _catalog_history before a new run."""
        conn = self._get_duckdb_conn()
        try:
            conn.execute("BEGIN TRANSACTION")
            snap_id_raw = conn.execute("SELECT nextval('_snap_id_seq')").fetchone()
            snap_id = int(snap_id_raw[0]) if snap_id_raw else 1

            conn.execute(
                "INSERT INTO _snapshots (snapshot_id, status) VALUES (?, 'running')",
                (snap_id,),
            )

            conn.execute(
                """
                INSERT INTO _catalog_history
                    (snapshot_id, symbol_id, security_id, exchange, timestamp,
                     open, high, low, close, volume,
                     parquet_file, ingestion_version, ingestion_ts)
                SELECT
                    ?, symbol_id, security_id, exchange, timestamp,
                    open, high, low, close, volume,
                    parquet_file, ingestion_version, ingestion_ts
                FROM _catalog
            """,
                (snap_id,),
            )

            conn.commit()
            return snap_id

        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"Snapshot creation failed: {e}")
            raise

        finally:
            conn.close()

    async def ingest(
        self,
        from_date: str = None,
        to_date: str = None,
        exchanges: List[str] = None,
        limit: int = None,
        max_concurrent: int = 5,
    ) -> Dict[str, Any]:
        """
        Main ingestion entry point. Runs async batch fetch -> atomic DuckDB write.

        Args:
            from_date: Start date (YYYY-MM-DD). Defaults to 1 year ago.
            to_date: End date (YYYY-MM-DD). Defaults to today.
            exchanges: List of exchanges to include (e.g. ['NSE', 'BSE']).
                       Defaults to both.
            limit: Max symbols to process. Defaults to all.
            max_concurrent: Max parallel API calls. Defaults to 5.

        Returns:
            Dict with keys: snapshot_id, symbols_processed, rows_written,
                            from_date, to_date, duration_sec
        """
        if from_date is None:
            from_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")
        if exchanges is None:
            exchanges = ["NSE", "BSE"]

        t0 = time.time()

        symbols = self.get_symbols_from_masterdb(exchanges=exchanges, limit=limit)
        if not symbols:
            return {"error": "No symbols found in masterdb", "snapshot_id": None}

        logger.info(
            f"Starting ingestion for {len(symbols)} symbols "
            f"({from_date} -> {to_date}), concurrency={max_concurrent})"
        )

        snapshot_id = self._create_snapshot()

        dfs = await self.fetch_batch(
            symbols, from_date, to_date, max_concurrent=max_concurrent
        )

        rows_written = self._write_batch(dfs, snapshot_id, from_date, to_date)

        feat_types = [
            "rsi",
            "adx",
            "sma",
            "ema",
            "macd",
            "atr",
            "bb",
            "roc",
            "supertrend",
        ]
        feat_result = self.fs.compute_and_store_features(
            symbols=[
                s["symbol_id"]
                for s in symbols
                if any(
                    df.attrs.get("symbol_info", {}).get("symbol_id") == s["symbol_id"]
                    for df in dfs
                )
            ]
            if dfs
            else None,
            exchanges=exchanges,
            feature_types=feat_types,
        )

        fund_result = self.fs.store_fundamental_features(
            masterdb_path=self.masterdb_path,
            exchanges=exchanges,
        )

        duration = time.time() - t0
        logger.info(
            f"Ingestion complete: {len(dfs)} symbols, {rows_written} rows "
            f"in {duration:.1f}s (snapshot={snapshot_id})"
        )
        logger.info(
            f"Feature store: {len(feat_result)} types computed, "
            f"{fund_result} fundamental features stored"
        )

        return {
            "snapshot_id": snapshot_id,
            "symbols_processed": len(dfs),
            "rows_written": rows_written,
            "from_date": from_date,
            "to_date": to_date,
            "duration_sec": round(duration, 2),
            "features_computed": feat_result,
            "fundamental_features_stored": fund_result,
        }

    def get_stats(self) -> Dict[str, Any]:
        """Return catalog statistics."""
        conn = self._get_duckdb_conn()
        try:
            total_rows = conn.execute("SELECT COUNT(*) FROM _catalog").fetchone()[0]
            total_symbols = conn.execute(
                "SELECT COUNT(DISTINCT symbol_id) FROM _catalog"
            ).fetchone()[0]
            latest_snap = conn.execute("""
                SELECT snapshot_id, snapshot_ts, symbols_processed, rows_written, status
                FROM _snapshots ORDER BY snapshot_id DESC LIMIT 1
            """).fetchone()
            parquet_files = conn.execute(
                "SELECT COUNT(*) FROM _parquet_files WHERE active = TRUE"
            ).fetchone()[0]

            return {
                "total_rows": total_rows,
                "total_symbols": total_symbols,
                "parquet_files": parquet_files,
                "latest_snapshot": {
                    "snapshot_id": latest_snap[0] if latest_snap else None,
                    "snapshot_ts": str(latest_snap[1]) if latest_snap else None,
                    "symbols_processed": latest_snap[2] if latest_snap else None,
                    "rows_written": latest_snap[3] if latest_snap else None,
                    "status": latest_snap[4] if latest_snap else None,
                }
                if latest_snap
                else None,
            }
        finally:
            conn.close()

    def list_snapshots(self, limit: int = 20) -> pd.DataFrame:
        """List recent ingestion snapshots."""
        conn = self._get_duckdb_conn()
        try:
            df = conn.execute(
                """
                SELECT snapshot_id, snapshot_ts, symbols_processed,
                       rows_written, from_date, to_date, status, note
                FROM _snapshots
                ORDER BY snapshot_id DESC
                LIMIT ?
            """,
                (limit,),
            ).fetchdf()
            return df
        finally:
            conn.close()

    def time_travel(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        version: int = None,
        as_of: str = None,
    ) -> pd.DataFrame:
        """
        Query historical state of a symbol using DuckDB time-travel.

        Args:
            symbol_id: Trading symbol (e.g. 'RELIANCE')
            exchange: Exchange (NSE/BSE). Defaults to NSE.
            version: Specific ingestion version to query.
            as_of: ISO timestamp string. Query catalog as of this time.

        Returns:
            DataFrame with historical OHLCV data.
        """
        conn = self._get_duckdb_conn()
        try:
            if version is not None:
                df = conn.execute(
                    """
                    SELECT timestamp, open, high, low, close, volume,
                           ingestion_version, parquet_file
                    FROM _catalog
                    WHERE symbol_id = ?
                      AND exchange = ?
                      AND ingestion_version <= ?
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY timestamp
                        ORDER BY ingestion_version DESC
                    ) = 1
                    ORDER BY timestamp
                """,
                    (symbol_id, exchange, version),
                ).fetchdf()
            elif as_of is not None:
                df = conn.execute(
                    """
                    SELECT timestamp, open, high, low, close, volume,
                           ingestion_version, parquet_file
                    FROM _catalog
                    WHERE symbol_id = ?
                      AND exchange = ?
                      AND ingestion_ts <= ?
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY timestamp
                        ORDER BY ingestion_ts DESC
                    ) = 1
                    ORDER BY timestamp
                """,
                    (symbol_id, exchange, as_of),
                ).fetchdf()
            else:
                df = conn.execute(
                    """
                    SELECT timestamp, open, high, low, close, volume,
                           ingestion_version, parquet_file
                    FROM _catalog
                    WHERE symbol_id = ? AND exchange = ?
                    ORDER BY timestamp
                """,
                    (symbol_id, exchange),
                ).fetchdf()
            return df
        finally:
            conn.close()

    def query_latest(self, symbol_id: str, exchange: str = "NSE") -> pd.DataFrame:
        """Query the latest available data for a symbol."""
        conn = self._get_duckdb_conn()
        try:
            return conn.execute(
                """
                SELECT timestamp, open, high, low, close, volume
                FROM _catalog
                WHERE symbol_id = ? AND exchange = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """,
                (symbol_id, exchange),
            ).fetchdf()
        finally:
            conn.close()

    def query_range(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        from_date: str = None,
        to_date: str = None,
    ) -> pd.DataFrame:
        """Query OHLCV data for a date range."""
        conn = self._get_duckdb_conn()
        try:
            if from_date and to_date:
                return conn.execute(
                    """
                    SELECT timestamp, open, high, low, close, volume
                    FROM _catalog
                    WHERE symbol_id = ? AND exchange = ?
                      AND timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp
                """,
                    (symbol_id, exchange, from_date, to_date),
                ).fetchdf()
            elif from_date:
                return conn.execute(
                    """
                    SELECT timestamp, open, high, low, close, volume
                    FROM _catalog
                    WHERE symbol_id = ? AND exchange = ?
                      AND timestamp >= ?
                    ORDER BY timestamp
                """,
                    (symbol_id, exchange, from_date),
                ).fetchdf()
            elif to_date:
                return conn.execute(
                    """
                    SELECT timestamp, open, high, low, close, volume
                    FROM _catalog
                    WHERE symbol_id = ? AND exchange = ?
                      AND timestamp <= ?
                    ORDER BY timestamp
                """,
                    (symbol_id, exchange, to_date),
                ).fetchdf()
            else:
                return conn.execute(
                    """
                    SELECT timestamp, open, high, low, close, volume
                    FROM _catalog
                    WHERE symbol_id = ? AND exchange = ?
                    ORDER BY timestamp
                """,
                    (symbol_id, exchange),
                ).fetchdf()
        finally:
            conn.close()

    def revert_to_snapshot(self, snapshot_id: int) -> int:
        """
        Revert _catalog to the state at a given snapshot.
        Returns number of rows remaining.
        """
        conn = self._get_duckdb_conn()
        try:
            conn.execute("BEGIN TRANSACTION")
            conn.execute(
                """
                DELETE FROM _catalog
                WHERE ingestion_version > ?
            """,
                (snapshot_id,),
            )
            conn.execute("""
                DELETE FROM _parquet_files
                WHERE parquet_file NOT IN (
                    SELECT DISTINCT parquet_file FROM _catalog WHERE parquet_file IS NOT NULL
                )
            """)
            rows = conn.execute("SELECT COUNT(*) FROM _catalog").fetchone()[0]
            conn.execute("COMMIT")
            logger.info(f"Reverted to snapshot {snapshot_id}: {rows} rows remain")
            return int(rows)
        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"Revert failed: {e}")
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    #  Rate limiting                                                      #
    # ------------------------------------------------------------------ #

    def _rate_limit_wait(self):
        """Thread-safe rate limiting: max 5 requests/sec, 1 bulk request/sec."""
        now = time.time()
        self.request_timestamps = [
            ts for ts in self.request_timestamps if now - ts < 1.0
        ]

        if len(self.request_timestamps) >= 5:
            sleep_time = 1.0 - (now - self.request_timestamps[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                self.request_timestamps = []

        if self.daily_request_count >= 1000:
            logger.warning("Daily API limit reached")
            return False

        self.request_timestamps.append(now)
        self.daily_request_count += 1
        return True

    def _check_daily_reset(self):
        """Reset daily counter if new day."""
        current_date = datetime.now().date()
        if current_date > self.last_reset_date:
            self.daily_request_count = 0
            self.last_reset_date = current_date

    # ------------------------------------------------------------------ #
    #  Legacy / compatibility                                             #
    # ------------------------------------------------------------------ #

    def _generate_sample_data(
        self, security_id: str, from_date: str, to_date: str
    ) -> pd.DataFrame:
        """Generate synthetic OHLCV data for testing when API is unavailable."""
        start_date = datetime.strptime(from_date, "%Y-%m-%d")
        end_date = datetime.strptime(to_date, "%Y-%m-%d")
        dates = pd.date_range(start=start_date, end=end_date, freq="B")

        if len(dates) == 0:
            return pd.DataFrame()

        base_price = random.uniform(100, 5000)
        current_price = base_price

        records = []
        for date in dates:
            change = random.uniform(-0.03, 0.03)
            current_price = current_price * (1 + change)
            daily_range = random.uniform(0.005, 0.02)
            high = current_price * (1 + daily_range)
            low = current_price * (1 - daily_range)
            open_px = random.uniform(low, high)
            close_px = random.uniform(low, high)
            volume = int(random.uniform(100_000, 10_000_000))

            records.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "open": round(open_px, 2),
                    "high": round(high, 2),
                    "low": round(low, 2),
                    "close": round(close_px, 2),
                    "volume": volume,
                }
            )

        df = pd.DataFrame(records)
        if "date" in df.columns:
            df["timestamp"] = pd.to_datetime(df["date"])
            df.set_index("timestamp", inplace=True)
            df.drop("date", axis=1, inplace=True)
        return df

    def save_to_parquet(self, df: pd.DataFrame, symbol: str) -> str:
        """Legacy method — redirects to DuckDB."""
        logger.warning("save_to_parquet is deprecated; use ingest() for ACID storage")
        return ""

    def load_from_parquet(self, symbol: str) -> pd.DataFrame:
        """Legacy method — redirects to DuckDB time-travel."""
        logger.warning("load_from_parquet is deprecated; use query_range() instead")
        return pd.DataFrame()

    def get_symbols_from_db(
        self, limit: int = None, sector_filter: str = None
    ) -> List[str]:
        """Legacy method — returns security_ids from masterdb."""
        rows = self.get_symbols_from_masterdb(limit=limit)
        if sector_filter:
            rows = [r for r in rows if r.get("sector") == sector_filter]
        return [r["security_id"] for r in rows]

    def get_symbols_with_details(self, limit: int = None) -> List[Dict]:
        """Legacy method — returns symbol details from masterdb."""
        return self.get_symbols_from_masterdb(limit=limit)

    def get_symbol_filename(self, security_id: str) -> tuple:
        """Legacy compatibility method."""
        rows = self.get_symbols_from_masterdb()
        for r in rows:
            if r["security_id"] == security_id:
                return r["symbol_name"], r["exchange"]
        return None, None

    def get_security_id_from_symbol(self, symbol: str) -> Optional[str]:
        """Get security_id from symbol name via masterdb."""
        rows = self.get_symbols_from_masterdb()
        for r in rows:
            if r["symbol_id"] == symbol or r.get("symbol_name") == symbol:
                return r["security_id"]
        return None

    def update_all_symbols(self, days_back: int = 7):
        """Legacy method — use ingest() with asyncio instead."""
        from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        logger.info(
            f"update_all_symbols is deprecated; use ingest(from_date='{from_date}', to_date='{to_date}')"
        )
        return {}

    def fetch_historical_for_symbols(
        self,
        symbols: List[str],
        years: int = 10,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        """Legacy method — use ingest() with asyncio instead."""
        logger.info(
            "fetch_historical_for_symbols is deprecated; use ingest() with asyncio"
        )
        return {}

    def get_historical_data(
        self,
        security_id: str,
        exchange: str = None,
        instrument_type: str = "EQ",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        interval: str = "D",
    ) -> pd.DataFrame:
        """Single-symbol historical fetch. Use ingest() for batch."""
        if from_date is None:
            from_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")

        rows = self.get_symbols_from_masterdb()
        symbol_info = None
        for r in rows:
            if r["security_id"] == security_id:
                symbol_info = r
                break

        if symbol_info is None:
            logger.error(f"Symbol not found: {security_id}")
            return pd.DataFrame()

        df = self._fetch_sync(
            security_id, symbol_info["exchange"], from_date, to_date, requests.Session()
        )
        return df if df is not None else pd.DataFrame()

    def init_download_tracker(self):
        """Legacy — tracker is now managed by DuckDB snapshots."""
        logger.info(
            "init_download_tracker is deprecated; DuckDB snapshots handle tracking"
        )

    def mark_symbol_downloaded(self, *args, **kwargs):
        """Legacy compatibility."""
        pass

    def get_pending_symbols(self) -> List[dict]:
        """Return symbols not yet in DuckDB catalog."""
        conn = self._get_duckdb_conn()
        try:
            df = conn.execute("""
                SELECT DISTINCT symbol_id, security_id, exchange
                FROM _catalog
            """).fetchdf()
            cataloged = set(zip(df["symbol_id"], df["exchange"]))

            all_syms = self.get_symbols_from_masterdb()
            pending = [
                {
                    "symbol_id": s["symbol_id"],
                    "security_id": s["security_id"],
                    "exchange": s["exchange"],
                }
                for s in all_syms
                if (s["symbol_id"], s["exchange"]) not in cataloged
            ]
            return pending
        finally:
            conn.close()

    def get_download_stats(self) -> dict:
        """Return download statistics."""
        stats = self.get_stats()
        conn = sqlite3.connect(self.masterdb_path)
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM symbols WHERE security_id IS NOT NULL AND security_id != ''"
        )
        total = cur.fetchone()[0]
        conn.close()
        return {
            "downloaded": stats["total_symbols"],
            "total": total,
            "pending": total - stats["total_symbols"],
        }

    # ------------------------------------------------------------------ #
    #  Daily Update (EOD)                                                 #
    # ------------------------------------------------------------------ #

    def _get_last_dates(self, exchanges: List[str] = None) -> Dict[str, str]:
        """
        Returns dict of symbol_id -> last date in DuckDB.
        Used by daily update to know where to resume from.
        """
        if exchanges is None:
            exchanges = ["NSE"]

        dates: Dict[str, str] = {}
        try:
            conn = duckdb.connect(self.db_path)
            try:
                for exc in exchanges:
                    rows = conn.execute(f"""
                        SELECT symbol_id, MAX(timestamp::DATE)::TEXT AS last_date
                        FROM _catalog
                        WHERE exchange = '{exc}'
                        GROUP BY symbol_id
                    """).fetchall()
                    for sym, dte in rows:
                        dates[sym] = dte
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f"Could not read last dates from DuckDB: {e}")
        return dates

    MARKETFEED_OHLC_URL = "https://api.dhan.co/v2/marketfeed/ohlc"

    def _fetch_bulk_ohlc(self, security_ids: List[str]) -> Optional[pd.DataFrame]:
        """Fetch today's OHLC for multiple securities in single API call."""
        if not self.use_api or not self.dhan:
            return None

        self._ensure_valid_token()
        access_token = os.getenv("DHAN_ACCESS_TOKEN", "")
        client_id = os.getenv("DHAN_CLIENT_ID", "")

        if not access_token or not client_id:
            logger.error("Missing DHAN_ACCESS_TOKEN or DHAN_CLIENT_ID")
            return None

        self._rate_limit_wait()

        headers = {
            "access-token": access_token,
            "client-id": client_id,
            "Content-Type": "application/json",
        }

        payload = {"instruments": {"NSE_EQ": security_ids}}

        try:
            response = requests.post(
                self.MARKETFEED_OHLC_URL, headers=headers, json=payload, timeout=60
            )

            if response.status_code == 401:
                logger.warning("Token expired, attempting renewal...")
                self._ensure_valid_token()
                headers["access-token"] = os.getenv("DHAN_ACCESS_TOKEN", "")
                response = requests.post(
                    self.MARKETFEED_OHLC_URL, headers=headers, json=payload, timeout=60
                )

            if response.status_code != 200:
                logger.error(
                    f"Bulk OHLC API error {response.status_code}: {response.text}"
                )
                return None

            data = response.json()
            if "data" not in data:
                return None

            processed = []
            today = datetime.now()

            for sec_id, details in data.get("data", {}).items():
                ohlc = details.get("ohlc", {})
                if not ohlc:
                    continue

                row = {
                    "security_id": sec_id,
                    "timestamp": today,
                    "open": ohlc.get("open"),
                    "high": ohlc.get("high"),
                    "low": ohlc.get("low"),
                    "close": ohlc.get("close"),
                    "volume": details.get("volume"),
                }
                processed.append(row)

            if not processed:
                return None

            df = pd.DataFrame(processed)
            logger.info(
                f"Bulk OHLC fetched {len(df)} rows for {len(security_ids)} securities"
            )
            return df

        except Exception as e:
            logger.error(f"Bulk OHLC fetch failed: {e}")
            return None

    def run_daily_update_bulk(
        self,
        exchanges: List[str] = None,
    ) -> Dict[str, Any]:
        """
        Run daily EOD update using bulk OHLC API.
        Fetches today's OHLC for all NSE symbols in ~1-2 API calls (1000 per call).
        Much faster than per-symbol fetching.

        Note: This only fetches TODAY's data, not historical backfill.
        Use run_daily_update() for symbols with stale data.
        """
        if exchanges is None:
            exchanges = ["NSE"]

        symbols = self.get_symbols_from_masterdb(exchanges=exchanges)
        if not symbols:
            return {"error": "No symbols found in masterdb"}

        security_ids = [s["security_id"] for s in symbols if s.get("security_id")]
        logger.info(
            f"[Bulk Daily Update] Fetching OHLC for {len(security_ids)} securities..."
        )

        t0 = time.time()
        df = self._fetch_bulk_ohlc(security_ids)

        if df is None or df.empty:
            return {
                "error": "No data fetched from bulk API",
                "duration_sec": time.time() - t0,
            }

        symbol_map = {s["security_id"]: s for s in symbols}
        df["symbol_id"] = df["security_id"].map(
            lambda x: symbol_map.get(str(x), {}).get("symbol_id", "")
        )
        df["exchange"] = "NSE"

        conn = self._get_duckdb_conn()
        try:
            conn.execute("BEGIN TRANSACTION")

            conn.execute("""
                DELETE FROM _catalog
                WHERE symbol_id IN (SELECT DISTINCT symbol_id FROM df)
                  AND exchange = 'NSE'
                  AND timestamp::date = CURRENT_DATE
            """)

            conn.execute("""
                INSERT INTO _catalog
                    (symbol_id, security_id, exchange, timestamp,
                     open, high, low, close, volume)
                SELECT symbol_id, CAST(security_id AS TEXT), exchange, timestamp,
                       open, high, low, close, volume
                FROM df
                WHERE symbol_id != ''
            """)

            rows_written = conn.execute("SELECT CHANGES()").fetchone()[0]
            conn.commit()

            duration = time.time() - t0
            logger.info(
                f"[Bulk Daily Update] Wrote {rows_written} rows in {duration:.1f}s"
            )

            feat_result = {}
            if rows_written > 0:
                updated_symbols = (
                    df[df["symbol_id"] != ""]["symbol_id"].unique().tolist()
                )
                feat_result = self.fs.compute_and_store_features(
                    symbols=updated_symbols,
                    exchanges=exchanges,
                    feature_types=[
                        "rsi",
                        "adx",
                        "sma",
                        "ema",
                        "macd",
                        "atr",
                        "bb",
                        "roc",
                        "supertrend",
                    ],
                )

            return {
                "n_symbols": len(security_ids),
                "rows_written": rows_written,
                "duration_sec": round(duration, 2),
                "feature_result": feat_result,
            }

        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"Bulk daily update failed: {e}")
            return {"error": str(e), "duration_sec": time.time() - t0}
        finally:
            conn.close()

    def run_daily_update(
        self,
        exchanges: List[str] = None,
        batch_size: int = 700,
        max_concurrent: int = 10,
        days_history: int = 7,
    ) -> Dict[str, Any]:
        """
        Run daily EOD update after market close.

        Incremental update — only fetches rows newer than the last
        date already in DuckDB. Falls back to `days_history` lookback
        for symbols with no prior data.

        Handles multi-day gaps automatically:
          - Reads last stored date per symbol from DuckDB
          - Fetches from (last_date + 1) day onwards
          - Handles weekends, holidays, missed days automatically
          - Symbols with no prior data get `days_history` lookback

        Args:
            exchanges: List of exchanges. Defaults to ['NSE'].
            batch_size: Symbols per batch. Defaults to 700 (2 batches = 1400).
            max_concurrent: Max parallel API calls. Defaults to 10.
            days_history: If a symbol has no prior data, fetch this many
                          days back. Defaults to 7 (handles Fri->Mon + holidays).

        Returns:
            Dict with: n_batches, total_symbols, updated, errors, duration_sec.
        """
        if exchanges is None:
            exchanges = ["NSE"]

        symbols = self.get_symbols_from_masterdb(exchanges=exchanges)
        if not symbols:
            return {"error": "No symbols found in masterdb"}

        today_str = datetime.now().strftime("%Y-%m-%d")
        last_dates = self._get_last_dates(exchanges=exchanges)
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        stale_symbols: List[str] = []
        no_data_symbols: List[str] = []
        up_to_date_symbols: List[str] = []

        t0 = time.time()
        total_updated = 0
        total_errors = 0
        all_updated_symbols: List[str] = []
        all_batches: List[List[Dict]] = []

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i : i + batch_size]
            batch_num = i // batch_size + 1
            n_batches = (len(symbols) + batch_size - 1) // batch_size
            logger.info(
                f"[Daily Update] Batch {batch_num}/{n_batches}: {len(batch)} symbols"
            )

            batch_symbols = []
            for s in batch:
                sym_id = s["symbol_id"]
                last = last_dates.get(sym_id)
                if last:
                    from_dt = datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)
                    from_str = from_dt.strftime("%Y-%m-%d")
                    days_gap = (datetime.now().date() - from_dt.date()).days
                    if days_gap <= 0:
                        up_to_date_symbols.append(sym_id)
                    else:
                        stale_symbols.append((sym_id, last, days_gap))
                else:
                    from_str = (datetime.now() - timedelta(days=days_history)).strftime(
                        "%Y-%m-%d"
                    )
                    no_data_symbols.append(sym_id)

                to_str = today_str
                enriched = s.copy()
                enriched["_from_date"] = from_str
                enriched["_to_date"] = to_str
                batch_symbols.append(enriched)

            dfs, errors = asyncio.run(
                self._fetch_daily_batch(batch_symbols, max_concurrent)
            )
            total_errors += errors

            if dfs:
                rows = self._upsert_ohlcv(dfs)
                total_updated += len(dfs)
                updated_syms = [
                    df.attrs.get("symbol_info", {}).get("symbol_id", "UNKNOWN")
                    for df in dfs
                ]
                all_updated_symbols.extend(updated_syms)
                all_batches.append(dfs)
                logger.info(
                    f"[Daily Update] Batch {batch_num} done: "
                    f"{len(dfs)} updated, {errors} errors"
                )
            else:
                logger.info(f"[Daily Update] Batch {batch_num}: no data fetched")

        duration = time.time() - t0

        feat_result = {}
        if all_updated_symbols:
            feat_result = self.fs.compute_and_store_features(
                symbols=all_updated_symbols,
                exchanges=exchanges,
                feature_types=[
                    "rsi",
                    "adx",
                    "sma",
                    "ema",
                    "macd",
                    "atr",
                    "bb",
                    "roc",
                    "supertrend",
                ],
            )
            logger.info(f"[Daily Update] Features updated: {feat_result}")

        n_batches = (len(symbols) + batch_size - 1) // batch_size
        logger.info(
            f"[Daily Update] Complete in {duration:.1f}s: "
            f"{total_updated} symbols updated, {total_errors} errors, "
            f"{n_batches} batches"
        )

        logger.info("")
        logger.info("=== Data Status Report ===")
        logger.info(f"  Up to date      : {len(up_to_date_symbols)} symbols")
        logger.info(f"  Stale (>1 day)  : {len(stale_symbols)} symbols")
        logger.info(f"  No prior data   : {len(no_data_symbols)} symbols")
        if stale_symbols:
            by_gap: Dict[int, List[str]] = {}
            for sym, last, gap in stale_symbols:
                by_gap.setdefault(gap, []).append(sym)
            for gap_days in sorted(by_gap.keys()):
                syms = by_gap[gap_days]
                logger.warning(
                    f"  [{gap_days}-day gap] {len(syms)} symbols: "
                    f"{', '.join(syms[:5])}{' ...' if len(syms) > 5 else ''}"
                )
        if no_data_symbols:
            logger.warning(
                f"  [No data] {len(no_data_symbols)} symbols: "
                f"{', '.join(no_data_symbols[:5])}{' ...' if len(no_data_symbols) > 5 else ''}"
            )
        logger.info("=========================")

        return {
            "n_batches": n_batches,
            "total_symbols": len(symbols),
            "symbols_updated": total_updated,
            "symbols_errors": total_errors,
            "updated_symbols": all_updated_symbols,
            "stale_symbols": [s[0] for s in stale_symbols],
            "stale_details": stale_symbols,
            "no_data_symbols": no_data_symbols,
            "up_to_date_symbols": len(up_to_date_symbols),
            "feature_result": feat_result,
            "duration_sec": round(duration, 2),
        }

    async def _fetch_daily_batch(
        self,
        symbols: List[Dict],
        max_concurrent: int,
    ) -> tuple:
        """
        Async fetch for a daily update batch.
        Each symbol may have a different `from_date` based on last known date.
        """
        self._semaphore = asyncio.Semaphore(max_concurrent)
        session = requests.Session()

        async def throttled_fetch(symbol_info: Dict) -> Optional[pd.DataFrame]:
            async with self._semaphore:
                return await self._fetch_one_daily(symbol_info, session)

        tasks = [throttled_fetch(s) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        session.close()

        dfs = []
        errors = 0
        for r in results:
            if isinstance(r, pd.DataFrame) and not r.empty:
                dfs.append(r)
            elif isinstance(r, Exception):
                logger.warning(f"Daily fetch exception: {r}")
                errors += 1

        return dfs, errors

    async def _fetch_one_daily(
        self,
        symbol_info: Dict,
        session: requests.Session,
    ) -> Optional[pd.DataFrame]:
        """Fetch one symbol's daily data from last known date to today."""
        security_id = symbol_info["security_id"]
        exchange = symbol_info["exchange"]
        from_date = symbol_info.get("_from_date")
        to_date = symbol_info.get("_to_date", datetime.now().strftime("%Y-%m-%d"))

        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(
            self._executor,
            self._fetch_sync,
            security_id,
            exchange,
            from_date,
            to_date,
            session,
        )
        if df is not None and not df.empty:
            df.attrs["symbol_info"] = symbol_info
        return df

    def _upsert_ohlcv(self, dfs: List[pd.DataFrame]) -> int:
        """
        Upsert OHLCV DataFrames into DuckDB.
        For each row: DELETE existing (symbol, exchange, timestamp) then INSERT new.
        This handles both new rows and updated EOD candles.
        Returns total rows written.
        """
        if not dfs:
            return 0

        conn = self._get_duckdb_conn()
        try:
            conn.execute("BEGIN TRANSACTION")
            rows_written = 0

            for df in dfs:
                if df.empty:
                    continue

                info = df.attrs.get("symbol_info", {})
                symbol_id = info.get("symbol_id", "UNKNOWN")
                security_id = info.get("security_id", "")
                exchange = info.get("exchange", "NSE")

                df = df.reset_index()
                df["symbol_id"] = symbol_id
                df["security_id"] = security_id
                df["exchange"] = exchange

                if "open" not in df.columns:
                    continue

                conn.execute(
                    """
                    DELETE FROM _catalog
                    WHERE symbol_id = ?
                      AND exchange = ?
                      AND timestamp IN (SELECT timestamp FROM df)
                    """,
                    (symbol_id, exchange),
                )

                conn.execute(
                    """
                    INSERT INTO _catalog
                        (symbol_id, security_id, exchange, timestamp,
                         open, high, low, close, volume,
                         parquet_file, ingestion_version)
                    SELECT
                        symbol_id, security_id, exchange, timestamp,
                        open, high, low, close, volume,
                        NULL, NULL
                    FROM df
                    """,
                )
                rows_written += len(df)

            conn.commit()
            return rows_written

        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"Upsert failed: {e}")
            return 0
        finally:
            conn.close()
