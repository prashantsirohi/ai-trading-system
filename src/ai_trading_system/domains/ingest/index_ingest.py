"""Index OHLCV ingestion for NSE sectoral indices (NIFTY BANK, NIFTY AUTO, etc.)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, date
from typing import List, Optional
import pandas as pd
import duckdb
import requests
from ai_trading_system.platform.logging.logger import logger

# Direct NSE API endpoints work without NSEDownload


# 10 Core Indices to track
CORE_INDICES = [
    ("NIFTY 50", "NIFTY_50", "Broad Market", None),
    ("NIFTY BANK", "NIFTY_BANK", "Sectoral", "Banks"),
    ("NIFTY AUTO", "NIFTY_AUTO", "Sectoral", "Automobiles"),
    ("NIFTY IT", "NIFTY_IT", "Sectoral", "IT"),
    ("NIFTY PHARMA", "NIFTY_PHARMA", "Sectoral", "Pharma"),
    ("NIFTY FMCG", "NIFTY_FMCG", "Sectoral", "FMCG"),
    ("NIFTY METAL", "NIFTY_METAL", "Sectoral", "Metals"),
    ("NIFTY ENERGY", "NIFTY_ENERGY", "Sectoral", "Energy"),
    ("NIFTY REALTY", "NIFTY_REALTY", "Sectoral", "Realty"),
    ("NIFTY INFRA", "NIFTY_INFRA", "Sectoral", "Infrastructure"),
]

# Mapping from 23 system sectors to index codes
SECTOR_MAPPING = {
    "Banks": ("NIFTY_BANK", "NIFTY BANK", True),
    "Finance": ("NIFTY_BANK", "NIFTY BANK", True),
    "Auto Components": ("NIFTY_AUTO", "NIFTY AUTO", True),
    "Automobiles": ("NIFTY_AUTO", "NIFTY AUTO", True),
    "IT": ("NIFTY_IT", "NIFTY IT", True),
    "Pharma": ("NIFTY_PHARMA", "NIFTY PHARMA", True),
    "Healthcare": ("NIFTY_PHARMA", "NIFTY PHARMA", False),
    "FMCG": ("NIFTY_FMCG", "NIFTY FMCG", True),
    "Consumer": ("NIFTY_FMCG", "NIFTY FMCG", False),
    "Consumer Durables": ("NIFTY_FMCG", "NIFTY FMCG", False),
    "Metals": ("NIFTY_METAL", "NIFTY METAL", True),
    "Mining": ("NIFTY_METAL", "NIFTY METAL", False),
    "Energy": ("NIFTY_ENERGY", "NIFTY ENERGY", True),
    "Power": ("NIFTY_ENERGY", "NIFTY ENERGY", False),
    "Realty": ("NIFTY_REALTY", "NIFTY REALTY", True),
    "Infrastructure": ("NIFTY_INFRA", "NIFTY INFRA", True),
    "Industrial": ("NIFTY_INFRA", "NIFTY INFRA", False),
    "Aerospace": ("NIFTY_50", "NIFTY 50", False),
    "Agri": ("NIFTY_50", "NIFTY 50", False),
    "Chemicals": ("NIFTY_50", "NIFTY 50", False),
    "Diversified": ("NIFTY_50", "NIFTY 50", False),
    "Materials": ("NIFTY_50", "NIFTY 50", False),
    "Other": ("NIFTY_50", "NIFTY 50", False),
    "Services": ("NIFTY_50", "NIFTY 50", False),
}


@dataclass
class IndexIngestConfig:
    ohlcv_db_path: str
    indices: List[tuple] = None
    provider: str = "nseindia"

    def __post_init__(self):
        if self.indices is None:
            self.indices = CORE_INDICES


class IndexCollector:
    """Collector for NSE sectoral index OHLCV data."""

    def __init__(self, config: Optional[IndexIngestConfig] = None):
        from ai_trading_system.domains.ingest.trust import ensure_index_schema
        
        if config is None:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
            ohlcv_db_path = os.path.join(project_root, "data", "ohlcv.duckdb")
            config = IndexIngestConfig(ohlcv_db_path=ohlcv_db_path)
        
        self.config = config
        self._ensure_tables()

    def _ensure_tables(self):
        """Ensure index tables exist."""
        from ai_trading_system.domains.ingest.trust import ensure_index_schema
        ensure_index_schema(self.config.ohlcv_db_path)

    def _register_indices(self) -> int:
        """Register core indices in _index_metadata table."""
        conn = duckdb.connect(self.config.ohlcv_db_path)
        try:
            for display_name, index_code, family, benchmark_for in self.config.indices:
                benchmark_val = "NULL" if benchmark_for is None else f"'{benchmark_for}'"
                conn.execute(f"""
                    INSERT OR IGNORE INTO _index_metadata 
                    (index_code, display_name, family, is_sectoral, benchmark_for, source, active)
                    VALUES ('{index_code}', '{display_name}', '{family}', TRUE, {benchmark_val}, 'nseindia', TRUE)
                """)
            
            count = conn.execute("SELECT COUNT(*) FROM _index_metadata").fetchone()[0]
            logger.info(f"Registered {count} indices in _index_metadata")
            return count
        finally:
            conn.close()

    def fetch_index_ohlc(
        self, 
        index_name: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """Fetch current OHLC for a single index using direct NSE API."""
        # Use direct NSE India API
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        
        # Bootstrap session
        try:
            session.get('https://www.nseindia.com/', timeout=10)
        except Exception:
            pass
        
        # Get index code for API
        index_code = None
        api_index_name = index_name
        for display_name, code, _, _ in self.config.indices:
            if display_name == index_name:
                index_code = code
                # Convert to NSE API format (e.g., "NIFTY%20BANK")
                api_index_name = index_name.replace(" ", "%20")
                break
        
        if index_code is None:
            index_code = index_name.upper().replace(" ", "_")
        
        try:
            url = f'https://www.nseindia.com/api/equity-stockIndices?index={api_index_name}'
            resp = session.get(url, timeout=15)
            
            if resp.status_code != 200:
                logger.warning(f"NSE API returned {resp.status_code} for {index_name}")
                return pd.DataFrame()
            
            data = resp.json()
            
            if 'data' not in data or not data['data']:
                logger.warning(f"No data in response for {index_name}")
                return pd.DataFrame()
            
            # Extract OHLC from first data point (current day)
            info = data['data'][0]
            
            trade_date = end_date or start_date or date.today().isoformat()
            
            result = pd.DataFrame([{
                'index_code': index_code,
                'date': trade_date,
                'open': info.get('open'),
                'high': info.get('dayHigh'),
                'low': info.get('dayLow'),
                'close': info.get('lastPrice'),
                'volume': info.get('totalTradedVolume'),
                'value': info.get('totalTradedValue'),
            }])
            
            # Convert numeric columns
            for col in ['open', 'high', 'low', 'close', 'volume', 'value']:
                if col in result.columns:
                    result.loc[:, col] = pd.to_numeric(result[col], errors='coerce')
            
            logger.info(f"Fetched {index_name}: close={result['close'].iloc[0]}")
            return result
            
        except Exception as e:
            logger.warning(f"Error fetching {index_name}: {e}")
            return pd.DataFrame()

    def fetch_latest(self, dates: List[str]) -> pd.DataFrame:
        """Fetch latest index data for given dates."""
        all_data = []
        unique_dates = list(dict.fromkeys(str(date_str) for date_str in dates))
        
        for index_name, _, _, _ in self.config.indices:
            for date_str in unique_dates:
                try:
                    df = self.fetch_index_ohlc(index_name, date_str, date_str)
                    if not df.empty:
                        all_data.append(df)
                except Exception as e:
                    logger.debug(f"Error fetching {index_name} for {date_str}: {e}")
                    continue
        
        if not all_data:
            return pd.DataFrame()
        
        result = pd.concat(all_data, ignore_index=True)
        logger.info(f"Fetched {len(result)} index records for {len(dates)} dates")
        return result

    def ingest(self, df: pd.DataFrame, run_id: Optional[str] = None) -> int:
        """Upsert index data to _index_catalog."""
        if df.empty:
            return 0
        
        conn = duckdb.connect(self.config.ohlcv_db_path)
        try:
            # Add run_id if provided
            if run_id and "ingest_run_id" not in df.columns:
                df = df.copy()
                df["ingest_run_id"] = run_id
            
            # Ensure columns exist
            for col in ["provider", "ingest_run_id"]:
                if col not in df.columns:
                    df = df.copy()
                    df[col] = self.config.provider if col == "provider" else run_id
            
            # Convert date to string for DuckDB
            df = df.copy()
            if 'date' in df.columns:
                df.loc[:, 'date'] = df['date'].astype(str)
            
            # Guard against duplicate keys inside the same batch insert.
            df = df.drop_duplicates(subset=["index_code", "date"], keep="last").reset_index(drop=True)
            
            # Use bulk insert via temp view
            conn.register('index_data', df)
            conn.execute("""
                INSERT INTO _index_catalog 
                (index_code, date, open, high, low, close, volume, value, provider, ingest_run_id)
                SELECT index_code, date::DATE, open, high, low, close, volume, value, provider, ingest_run_id
                FROM index_data
                ON CONFLICT (index_code, date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    value = EXCLUDED.value,
                    provider = EXCLUDED.provider,
                    ingest_run_id = EXCLUDED.ingest_run_id
            """)
            conn.execute("DROP VIEW index_data")
            
            count = len(df)
            logger.info(f"Ingested {count} index records to _index_catalog")
            return count
        finally:
            conn.close()

    def get_sector_index(self, system_sector: str) -> Optional[tuple]:
        """Get index code and name for a system sector."""
        return SECTOR_MAPPING.get(system_sector)

    def get_all_sector_mappings(self) -> pd.DataFrame:
        """Get all sector to index mappings as DataFrame."""
        data = []
        for sector, (code, name, primary) in SECTOR_MAPPING.items():
            data.append({
                "system_sector": sector,
                "index_code": code,
                "index_name": name,
                "is_primary": primary,
            })
        return pd.DataFrame(data)

    def populate_sector_mapping(self) -> int:
        """Populate sector_to_index mapping table."""
        mappings = self.get_all_sector_mappings()
        conn = duckdb.connect(self.config.ohlcv_db_path)
        try:
            for _, row in mappings.iterrows():
                fallback = "'NIFTY_50'" if not row["is_primary"] else "NULL"
                conn.execute(f"""
                    INSERT OR REPLACE INTO sector_to_index 
                    (system_sector, index_code, index_name, is_primary, fallback_index)
                    VALUES ('{row['system_sector']}', '{row['index_code']}', '{row['index_name']}', {row['is_primary']}, {fallback})
                """)
            
            count = conn.execute("SELECT COUNT(*) FROM sector_to_index").fetchone()[0]
            logger.info(f"Populated {count} sector_to_index mappings")
            return count
        finally:
            conn.close()

    def backfill_indices(self, from_date: str, to_date: str, batch_size: int = 50) -> int:
        """Backfill historical index data for date range.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            batch_size: Number of dates to process before batching insert
            
        Returns:
            Total number of records ingested
        """
        import time
        
        all_data = []
        total_fetched = 0
        total_ingested = 0
        
        # Get all business days in range
        dates = pd.bdate_range(from_date, to_date)
        total_dates = len(dates)
        
        logger.info(f"Starting index backfill: {len(self.config.indices)} indices × {total_dates} dates")
        
        for idx, date in enumerate(dates):
            date_str = date.strftime('%Y-%m-%d')
            
            for index_name, _, _, _ in self.config.indices:
                try:
                    df = self.fetch_index_ohlc(index_name, date_str, date_str)
                    if not df.empty:
                        all_data.append(df)
                        total_fetched += 1
                except Exception as e:
                    logger.debug(f"Error fetching {index_name} for {date_str}: {e}")
                    continue
            
            # Batch insert every batch_size dates
            if len(all_data) >= batch_size or (idx == total_dates - 1 and all_data):
                result = pd.concat(all_data, ignore_index=True)
                result['provider'] = 'nseindia'
                result['ingest_run_id'] = 'backfill'
                
                count = self.ingest(result)
                total_ingested += count
                logger.info(f"Backfill progress: {idx+1}/{total_dates} dates, {total_ingested} records ingested")
                
                all_data = []
                
                # Rate limiting - small delay to avoid NSE throttling
                time.sleep(0.1)
        
        logger.info(f"Index backfill complete: {total_fetched} fetched, {total_ingested} ingested")
        return total_ingested


def get_index_collector(ohlcv_db_path: str = None) -> IndexCollector:
    """Get IndexCollector instance with default config."""
    if ohlcv_db_path is None:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
        ohlcv_db_path = os.path.join(project_root, "data", "ohlcv.duckdb")
    
    config = IndexIngestConfig(ohlcv_db_path=ohlcv_db_path)
    return IndexCollector(config)
