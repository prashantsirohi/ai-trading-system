import asyncio
import time
import sqlite3
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import duckdb
import requests
from dhanhq import dhanhq
from ai_trading_system.domains.ingest.repository import (
    ensure_catalog_compatibility,
    get_duckdb_conn,
    get_table_columns,
    initialize_ingest_duckdb,
)
from ai_trading_system.platform.utils.env import load_project_env
from ai_trading_system.domains.features.feature_store import FeatureStore
from ai_trading_system.domains.ingest.validation import validate_ohlcv_frame
from ai_trading_system.domains.ingest.token_manager import DhanTokenManager
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.platform.logging.logger import logger

IST_TZ = timezone(timedelta(hours=5, minutes=30))


def dhan_daily_window_ist(reference: datetime | None = None) -> tuple[str, str]:
    """Return Dhan daily window in IST as (today-1, today)."""
    if reference is None:
        now_ist = datetime.now(timezone.utc).astimezone(IST_TZ)
    elif reference.tzinfo is None:
        now_ist = reference.replace(tzinfo=IST_TZ)
    else:
        now_ist = reference.astimezone(IST_TZ)
    to_date = now_ist.date()
    from_date = to_date - timedelta(days=1)
    return from_date.isoformat(), to_date.isoformat()


def normalize_dhan_timestamps_ist(values: Any) -> pd.Series:
    """
    Normalize Dhan timestamp arrays into naive IST datetimes.

    Dhan timestamp payloads can arrive as epoch seconds, epoch milliseconds,
    or serial day counts. Epoch payloads are UTC-based and must be translated
    to IST trading-day boundaries before date-level comparisons.
    """
    series = pd.Series(values)
    numeric = pd.to_numeric(series, errors="coerce")
    non_null = numeric.dropna()
    if non_null.empty:
        return pd.to_datetime(series, errors="coerce")

    sample = float(non_null.iloc[0])
    if sample > 1_000_000_000_000:
        parsed = pd.to_datetime(numeric, unit="ms", utc=True, errors="coerce")
        return parsed.dt.tz_convert(IST_TZ).dt.tz_localize(None)
    if sample > 1_000_000_000:
        parsed = pd.to_datetime(numeric, unit="s", utc=True, errors="coerce")
        return parsed.dt.tz_convert(IST_TZ).dt.tz_localize(None)
    if sample > 10_000:
        return pd.to_datetime(numeric, unit="D", origin="1899-12-30", errors="coerce")
    return pd.to_datetime(series, errors="coerce")


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

    Operational usage requires authenticated Dhan access. The collector now
    fails loudly instead of generating synthetic OHLCV data.
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
        data_domain: str = "operational",
    ):
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))
        load_project_env(project_root)
        paths = ensure_domain_layout(
            project_root=project_root,
            data_domain=data_domain,
        )
        if warehouse_dir is None:
            warehouse_dir = str(paths.root_dir / "features")
        if db_path is None:
            db_path = str(paths.ohlcv_db_path)
        if masterdb_path is None:
            masterdb_path = str(paths.master_db_path)
        if feature_store_dir is None:
            feature_store_dir = str(paths.feature_store_dir)

        self.api_key = api_key or os.getenv("DHAN_API_KEY", "")
        self.client_id = client_id or os.getenv("DHAN_CLIENT_ID", "")
        self.access_token = access_token or os.getenv("DHAN_ACCESS_TOKEN", "")
        self.warehouse_dir = warehouse_dir
        self.db_path = db_path
        self.masterdb_path = masterdb_path
        self.feature_store_dir = feature_store_dir
        self.max_concurrent = max_concurrent
        self.data_domain = data_domain
        self.base_url = "https://api.dhan.co/v2"
        self.request_timestamps: List[float] = []
        self.daily_request_count = 0
        self.last_reset_date = datetime.now().date()
        self.use_api = bool(self.client_id and self.access_token)
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._executor = ThreadPoolExecutor(max_workers=max_concurrent)

        env_path = os.path.join(project_root, ".env")
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
            data_domain=self.data_domain,
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
        initialize_ingest_duckdb(self.db_path)

    def _ensure_catalog_compatibility(self, conn):
        ensure_catalog_compatibility(conn)

    def _get_table_columns(self, conn, table_name: str) -> set[str]:
        return get_table_columns(conn, table_name)

    def _build_insert_select_sql(self, table_name: str, available_values: dict[str, str], conn) -> str:
        """Build an insert/select statement using only columns present in the target table."""
        target_columns = self._get_table_columns(conn, table_name)
        ordered_columns = [column for column in available_values if column in target_columns]
        columns_sql = ", ".join(ordered_columns)
        values_sql = ", ".join(available_values[column] for column in ordered_columns)
        return f"""
            INSERT INTO {table_name}
                ({columns_sql})
            SELECT
                {values_sql}
            FROM df
        """

    def _get_duckdb_conn(self):
        return get_duckdb_conn(self.db_path)

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
            WHERE type='table' AND name = 'symbols'
        """)
        available_tables = {r[0] for r in cur.fetchall()}

        if "symbols" in available_tables:
            query = """
                SELECT
                    s.security_id,
                    s.symbol_id,
                    s.symbol_name,
                    s.sector AS industry_group,
                    s.industry,
                    s.mcap,
                    s.exchange
                FROM symbols s
                WHERE s.security_id IS NOT NULL
                  AND s.security_id != ''
            """
            params: List[Any] = []

            if exchanges:
                placeholders = ",".join("?" * len(exchanges))
                query += f" AND s.exchange IN ({placeholders})"
                params.extend(exchanges)

            query += " ORDER BY s.security_id"

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
                    "exchange": r[6] if r[6] else "NSE",
                }
                for r in cur.fetchall()
            ]
            logger.info(f"Loaded {len(rows)} symbols from symbols table")
            return rows

        logger.warning("symbols table not found in masterdb")
        return []

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
            raise RuntimeError(
                "Authenticated Dhan access is unavailable; refusing to generate fallback OHLCV data."
            )

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
                expiry_code=0,
                from_date=from_date,
                to_date=to_date,
            )

            logger.debug(f"{security_id}: API Response type: {type(data)}")
            if isinstance(data, dict):
                logger.debug(f"{security_id}: Keys: {list(data.keys())}")
                inner = data.get("data", data)
                if isinstance(inner, dict):
                    logger.debug(f"{security_id}: Inner keys: {list(inner.keys())}")
                    if inner.get("close"):
                        logger.debug(f"{security_id}: close sample: {inner['close'][:3]}")
                    if inner.get("timestamp"):
                        logger.debug(f"{security_id}: timestamp sample: {inner['timestamp'][:3]}")

            if not data or not isinstance(data, dict):
                logger.warning(f"{security_id}: No data returned")
                return None

            inner = data.get("data", data)
            if not isinstance(inner, dict):
                logger.warning(f"{security_id}: Invalid inner data type: {type(inner)}")
                return None

            logger.debug(f"{security_id}: Inner keys: {list(inner.keys())}")
            if inner.get("close"):
                logger.debug(f"Sample close: {inner['close'][:3]}")
            if inner.get("timestamp"):
                logger.debug(f"Sample timestamp: {inner['timestamp'][:3]}")
            if inner.get("date"):
                logger.debug(f"Sample date: {inner['date'][:3]}")

            open_arr = inner.get("open", [])
            if not open_arr or not isinstance(open_arr, list):
                logger.warning(f"No open data for {security_id}")
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
                ts_sample = inner["timestamp"][0] if inner["timestamp"] else 0
                logger.debug(
                    f"Timestamp sample value: {ts_sample} (type: {type(ts_sample)})"
                )
                df["timestamp"] = normalize_dhan_timestamps_ist(inner["timestamp"])
            elif "date" in inner and inner["date"]:
                df["timestamp"] = pd.to_datetime(inner["date"])
            else:
                logger.warning(f"No timestamp or date field for {security_id}")
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

    def _fetch_intraday(
        self,
        security_id: str,
        exchange: str,
        from_date: str,
        to_date: str,
        session: requests.Session = None,
    ) -> Optional[pd.DataFrame]:
        """Fetch intraday minute data (last 5 trading days)."""
        clean_sid = security_id
        try:
            if security_id.endswith(".0"):
                clean_sid = security_id[:-2]
        except Exception:
            pass

        exchange_segment = "NSE_EQ" if exchange.upper() == "NSE" else "BSE_EQ"

        try:
            # Intraday data - last 5 days
            data = self.dhan.intraday_minute_data(
                security_id=clean_sid,
                exchange_segment=exchange_segment,
                instrument_type="EQUITY",
                from_date=from_date,
                to_date=to_date,
                interval=15,  # 15-minute candles
            )

            logger.debug(f"{security_id}: Intraday response type: {type(data)}")
            if isinstance(data, dict):
                logger.debug(f"{security_id}: Keys: {list(data.keys())}")

            if not data or not isinstance(data, dict):
                logger.warning(f"No intraday data for {security_id}")
                return None

            inner = data.get("data", data)
            if not isinstance(inner, dict):
                logger.warning(f"Invalid intraday inner for {security_id}")
                return None

            open_arr = inner.get("open", [])
            if not open_arr or not isinstance(open_arr, list):
                logger.warning(f"No intraday open for {security_id}")
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

            # Handle timestamp - epoch format
            if "timestamp" in inner and inner["timestamp"]:
                df["timestamp"] = pd.to_datetime(inner["timestamp"], unit="s")
            elif "date" in inner and inner["date"]:
                df["timestamp"] = pd.to_datetime(inner["date"])
            else:
                return None

            df.set_index("timestamp", inplace=True)

            # Get last candle of each day (close)
            df["date"] = df.index.date
            df = df.groupby("date").last().reset_index()
            df["timestamp"] = pd.to_datetime(df["date"]) + pd.Timedelta(
                hours=18, minutes=30
            )
            df = df.set_index("timestamp").drop(columns=["date"])

            return df

        except Exception as e:
            logger.warning(f"Error fetching intraday {security_id}: {e}")
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

                df["parquet_file"] = filepath
                df["ingestion_version"] = snapshot_id
                conn.execute(
                    self._build_insert_select_sql(
                        "_catalog",
                        {
                            "symbol_id": "symbol_id",
                            "security_id": "security_id",
                            "exchange": "exchange",
                            "timestamp": "timestamp",
                            "open": "open",
                            "high": "high",
                            "low": "low",
                            "close": "close",
                            "volume": "volume",
                            "parquet_file": "parquet_file",
                            "ingestion_version": "ingestion_version",
                            "provider": "provider",
                            "provider_priority": "provider_priority",
                            "validation_status": "validation_status",
                            "validated_against": "validated_against",
                            "ingest_run_id": "ingest_run_id",
                            "repair_batch_id": "repair_batch_id",
                            "provider_confidence": "provider_confidence",
                            "provider_discrepancy_flag": "provider_discrepancy_flag",
                            "provider_discrepancy_note": "provider_discrepancy_note",
                            "adjusted_open": "adjusted_open",
                            "adjusted_high": "adjusted_high",
                            "adjusted_low": "adjusted_low",
                            "adjusted_close": "adjusted_close",
                            "adjustment_factor": "adjustment_factor",
                            "adjustment_source": "adjustment_source",
                            "instrument_type": "instrument_type",
                            "is_benchmark": "is_benchmark",
                            "benchmark_label": "benchmark_label",
                            "isin": "isin",
                        },
                        conn,
                    )
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
            current_max_raw = conn.execute(
                "SELECT COALESCE(MAX(snapshot_id), 0) FROM _snapshots"
            ).fetchone()
            current_max = int(current_max_raw[0]) if current_max_raw else 0
            snap_id_raw = conn.execute("SELECT nextval('_snap_id_seq')").fetchone()
            seq_value = int(snap_id_raw[0]) if snap_id_raw else 1
            # Older local DBs can have a stale sequence; never reuse an
            # existing snapshot id even if nextval() lags behind.
            snap_id = max(seq_value, current_max + 1)

            conn.execute(
                "INSERT INTO _snapshots (snapshot_id, status) VALUES (?, 'running')",
                (snap_id,),
            )

            catalog_columns = self._get_table_columns(conn, "_catalog")
            history_columns = self._get_table_columns(conn, "_catalog_history")
            shared_columns = [
                column
                for column in (
                    "symbol_id",
                    "security_id",
                    "exchange",
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "parquet_file",
                    "ingestion_version",
                    "ingestion_ts",
                    "provider",
                    "provider_priority",
                    "validation_status",
                    "validated_against",
                    "ingest_run_id",
                    "repair_batch_id",
                    "provider_confidence",
                    "provider_discrepancy_flag",
                    "provider_discrepancy_note",
                    "adjusted_open",
                    "adjusted_high",
                    "adjusted_low",
                    "adjusted_close",
                    "adjustment_factor",
                    "adjustment_source",
                    "instrument_type",
                    "is_benchmark",
                    "benchmark_label",
                    "isin",
                )
                if column in catalog_columns and column in history_columns
            ]
            history_insert_columns = ["snapshot_id", *shared_columns]
            history_select_columns = ["?", *shared_columns]
            conn.execute(
                f"""
                INSERT INTO _catalog_history
                    ({", ".join(history_insert_columns)})
                SELECT
                    {", ".join(history_select_columns)}
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
        """Return full masterdb symbol records not yet present in DuckDB."""
        conn = self._get_duckdb_conn()
        try:
            df = conn.execute("""
                SELECT DISTINCT symbol_id, security_id, exchange
                FROM _catalog
            """).fetchdf()
            cataloged = set(zip(df["symbol_id"], df["exchange"]))

            all_syms = self.get_symbols_from_masterdb()
            pending = [
                s for s in all_syms if (s["symbol_id"], s["exchange"]) not in cataloged
            ]
            return pending
        finally:
            conn.close()

    def backfill_pending_symbols(
        self,
        days_back: int = 365,
        exchanges: List[str] = None,
        max_concurrent: int = None,
    ) -> Dict[str, Any]:
        """
        Backfill symbols present in masterdb but missing from the OHLCV catalog.

        This keeps operational data aligned with the latest stock universe
        without requiring a full reingestion.
        """
        if exchanges is None:
            exchanges = ["NSE"]

        pending = [
            s for s in self.get_pending_symbols() if s.get("exchange") in set(exchanges)
        ]
        if not pending:
            return {
                "pending_before": 0,
                "symbols_processed": 0,
                "rows_written": 0,
                "from_date": None,
                "to_date": None,
                "features_computed": {},
            }

        from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        snapshot_id = self._create_snapshot()
        dfs = asyncio.run(
            self.fetch_batch(
                pending,
                from_date,
                to_date,
                max_concurrent=max_concurrent or self.max_concurrent,
            )
        )
        rows_written = self._write_batch(dfs, snapshot_id, from_date, to_date)
        updated_symbols = [
            df.attrs.get("symbol_info", {}).get("symbol_id")
            for df in dfs
            if getattr(df, "attrs", None)
        ]
        updated_symbols = [s for s in updated_symbols if s]

        feat_result = {}
        if updated_symbols:
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
            "pending_before": len(pending),
            "symbols_processed": len(updated_symbols),
            "rows_written": rows_written,
            "from_date": from_date,
            "to_date": to_date,
            "features_computed": feat_result,
        }

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
        symbol_limit: int | None = None,
        compute_features: bool = False,
        full_rebuild: bool = False,
        feature_tail_bars: int = 252,
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
        if symbol_limit is not None:
            symbols = symbols[:symbol_limit]

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

            df["security_id"] = df["security_id"].astype(str)
            conn.execute(
                self._build_insert_select_sql(
                    "_catalog",
                    {
                        "symbol_id": "symbol_id",
                        "security_id": "security_id",
                        "exchange": "exchange",
                        "timestamp": "timestamp",
                        "open": "open",
                        "high": "high",
                        "low": "low",
                        "close": "close",
                        "volume": "volume",
                    },
                    conn,
                )
                + "\nWHERE symbol_id != ''"
            )

            rows_written = conn.execute("SELECT CHANGES()").fetchone()[0]
            conn.commit()

            duration = time.time() - t0
            logger.info(
                f"[Bulk Daily Update] Wrote {rows_written} rows in {duration:.1f}s"
            )

            feat_result = {}
            if compute_features and rows_written > 0:
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
                    incremental=not full_rebuild,
                    tail_bars=feature_tail_bars,
                    full_rebuild=full_rebuild,
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
        symbol_limit: int | None = None,
        compute_features: bool = False,
        full_rebuild: bool = False,
        feature_tail_bars: int = 252,
        run_id: str | None = None,
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
        if symbol_limit is not None:
            symbols = symbols[:symbol_limit]

        default_from_str, to_str = dhan_daily_window_ist()
        to_date_obj = datetime.fromisoformat(to_str).date()
        yesterday_date = to_date_obj - timedelta(days=1)
        bootstrap_from_str = (to_date_obj - timedelta(days=max(1, int(days_history)))).isoformat()
        last_dates = self._get_last_dates(exchanges=exchanges)

        stale_symbols: List[str] = []
        no_data_symbols: List[str] = []
        up_to_date_symbols: List[str] = []
        from_candidates: List[str] = []

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
                symbol_from_str = default_from_str
                if last:
                    try:
                        last_dt = datetime.strptime(last, "%Y-%m-%d").date()
                    except ValueError:
                        last_dt = None
                    if last_dt is None:
                        no_data_symbols.append(sym_id)
                        symbol_from_str = bootstrap_from_str
                    else:
                        days_gap = (to_date_obj - last_dt).days
                        if last_dt >= to_date_obj:
                            up_to_date_symbols.append(sym_id)
                            continue
                        if last_dt < yesterday_date:
                            stale_symbols.append((sym_id, last, days_gap))
                            symbol_from_str = (last_dt + timedelta(days=1)).isoformat()
                else:
                    no_data_symbols.append(sym_id)
                    symbol_from_str = bootstrap_from_str

                enriched = s.copy()
                enriched["_from_date"] = symbol_from_str
                enriched["_to_date"] = to_str
                enriched["_ingest_run_id"] = run_id
                batch_symbols.append(enriched)
                from_candidates.append(symbol_from_str)

            if not batch_symbols:
                logger.info(f"[Daily Update] Batch {batch_num}: all symbols already up to date")
                continue

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
        if compute_features and all_updated_symbols:
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
                incremental=not full_rebuild,
                tail_bars=feature_tail_bars,
                full_rebuild=full_rebuild,
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
            "window_from_date": min(from_candidates) if from_candidates else default_from_str,
            "window_to_date": to_str,
            "default_daily_from_date": default_from_str,
            "bootstrap_from_date": bootstrap_from_str,
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
        default_from, default_to = dhan_daily_window_ist()
        from_date = symbol_info.get("_from_date", default_from)
        to_date = symbol_info.get("_to_date", default_to)
        ingest_run_id = symbol_info.get("_ingest_run_id")

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
            df["provider"] = "dhan_historical_daily"
            df["provider_priority"] = 1
            df["validation_status"] = "dhan_primary_unverified"
            df["validated_against"] = None
            df["ingest_run_id"] = ingest_run_id
            df["repair_batch_id"] = None
            df["provider_confidence"] = 1.0
            df["provider_discrepancy_flag"] = False
            df["provider_discrepancy_note"] = None
            df["adjusted_open"] = df["open"]
            df["adjusted_high"] = df["high"]
            df["adjusted_low"] = df["low"]
            df["adjusted_close"] = df["close"]
            df["adjustment_factor"] = 1.0
            df["adjustment_source"] = None
            df["instrument_type"] = "equity"
            df["is_benchmark"] = False
            df["benchmark_label"] = None
            df["isin"] = symbol_info.get("isin")
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

                df = validate_ohlcv_frame(
                    df,
                    source_label=f"dhan_collector._upsert_ohlcv:{symbol_id}:{exchange}",
                )

                conn.execute(
                    """
                    DELETE FROM _catalog
                    WHERE symbol_id = ?
                      AND exchange = ?
                      AND timestamp IN (SELECT timestamp FROM df)
                    """,
                    (symbol_id, exchange),
                )

                df["parquet_file"] = None
                df["ingestion_version"] = None
                conn.execute(
                    self._build_insert_select_sql(
                        "_catalog",
                        {
                            "symbol_id": "symbol_id",
                            "security_id": "security_id",
                            "exchange": "exchange",
                            "timestamp": "timestamp",
                            "open": "open",
                            "high": "high",
                            "low": "low",
                            "close": "close",
                            "volume": "volume",
                            "parquet_file": "parquet_file",
                            "ingestion_version": "ingestion_version",
                            "provider": "provider",
                            "provider_priority": "provider_priority",
                            "validation_status": "validation_status",
                            "validated_against": "validated_against",
                            "ingest_run_id": "ingest_run_id",
                            "repair_batch_id": "repair_batch_id",
                            "provider_confidence": "provider_confidence",
                            "provider_discrepancy_flag": "provider_discrepancy_flag",
                            "provider_discrepancy_note": "provider_discrepancy_note",
                            "adjusted_open": "adjusted_open",
                            "adjusted_high": "adjusted_high",
                            "adjusted_low": "adjusted_low",
                            "adjusted_close": "adjusted_close",
                            "adjustment_factor": "adjustment_factor",
                            "adjustment_source": "adjustment_source",
                            "instrument_type": "instrument_type",
                            "is_benchmark": "is_benchmark",
                            "benchmark_label": "benchmark_label",
                            "isin": "isin",
                        },
                        conn,
                    )
                )
                rows_written += len(df)

            conn.commit()
            return rows_written

        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"Upsert failed: {e}")
            raise
        finally:
            conn.close()
