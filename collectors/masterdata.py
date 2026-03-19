import os
import sqlite3
import requests
import pandas as pd
import logging
import io
from datetime import datetime
from typing import Optional, Dict, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MasterDataCollector:
    """
    Master Data Collector - Fetches and merges Dhan symbol master with sector data.
    
    Sources:
    - DhanHQ: https://images.dhan.co/api-data/api-scrip-master.csv
    - Zerodha: https://zerodha.com/markets/sector/ (NIFTY sector indices)
    """

    def __init__(self, db_path: str = "ai-trading-system/data/masterdata.db"):
        self.db_path = db_path
        self.dhan_url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        self.zerodha_sector_url = "https://zerodha.com/markets/sector/"
        self._init_database()

    def _init_database(self):
        """Initialize SQLite database"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS symbols (
                symbol_id TEXT PRIMARY KEY,
                security_id TEXT,
                symbol_name TEXT,
                exchange TEXT,
                instrument_type TEXT,
                isin TEXT,
                lot_size INTEGER,
                tick_size REAL,
                freeze_quantity INTEGER,
                sector TEXT,
                industry TEXT,
                nse_symbol TEXT,
                bse_symbol TEXT,
                last_updated TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sectors (
                sector_name TEXT PRIMARY KEY,
                index_symbol TEXT,
                nse_sector_id TEXT,
                last_updated TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sector_constituents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sector_name TEXT,
                symbol_id TEXT,
                symbol_name TEXT,
                weight REAL,
                last_updated TEXT,
                FOREIGN KEY (sector_name) REFERENCES sectors(sector_name)
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbols_exchange ON symbols(exchange)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbols_sector ON symbols(sector)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_sector_constituents_sector ON sector_constituents(sector_name)
        """)

        conn.commit()
        conn.close()
        logger.info(f"Database initialized: {self.db_path}")

    def fetch_dhan_master(self) -> pd.DataFrame:
        """Fetch DhanHQ symbol master data"""
        try:
            logger.info("Fetching DhanHQ master data...")
            response = requests.get(self.dhan_url, timeout=60)
            response.raise_for_status()

            df = pd.read_csv(io.StringIO(response.text))
            
            logger.info(f"Downloaded {len(df)} symbols from DhanHQ")
            return df

        except Exception as e:
            logger.error(f"Error fetching Dhan master data: {e}")
            return pd.DataFrame()

    def fetch_zerodha_sectors(self) -> Dict[str, str]:
        """
        Fetch Zerodha sector data.
        Returns mapping of sector name to index symbol.
        """
        sectors = {
            "NIFTY 50": "NIFTY 50",
            "NIFTY NEXT 50": "NIFTY NEXT 50",
            "NIFTY 100": "NIFTY 100",
            "NIFTY 200": "NIFTY 200",
            "NIFTY 500": "NIFTY 500",
            "NIFTY MIDCAP 50": "NIFTY MIDCAP 50",
            "NIFTY MIDCAP 100": "NIFTY MIDCAP 100",
            "NIFTY SMALLCAP 100": "NIFTY SMALLCAP 100",
            "NIFTY BANK": "NIFTY BANK",
            "NIFTY IT": "NIFTY IT",
            "NIFTY PHARMA": "NIFTY PHARMA",
            "NIFTY AUTO": "NIFTY AUTO",
            "NIFTY METAL": "NIFTY METAL",
            "NIFTY FMCG": "NIFTY FMCG",
            "NIFTY ENERGY": "NIFTY ENERGY",
            "NIFTY REALTY": "NIFTY REALTY",
            "NIFTY INFRA": "NIFTY INFRA",
            "NIFTY COMMODITIES": "NIFTY COMMODITIES",
            "NIFTY CONSUMER DURABLES": "NIFTY CONSUMER DURABLES",
            "NIFTY FINANCIAL SERVICES": "NIFTY FINANCIAL SERVICES",
            "NIFTY MEDIA": "NIFTY MEDIA",
            "NIFTY PRIVATE BANK": "NIFTY PRIVATE BANK",
            "NIFTY PSU BANK": "NIFTY PSU BANK",
        }
        return sectors

    def _parse_zerodha_sector_page(self, html: str) -> Dict[str, List[Dict]]:
        """Parse Zerodha sector page HTML"""
        sector_data = {}
        
        import re
        
        sector_pattern = r'<a[^>]*href="/markets/sector/([^"]+)"[^>]*>([^<]+)</a>'
        matches = re.findall(sector_pattern, html)
        
        for slug, name in matches:
            sector_data[name.strip()] = []
        
        return sector_data

    def _map_sectors_from_dhan(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map sectors from Dhan data based on industry"""
        
        sector_mapping = {
            "Banks": "NIFTY BANK",
            "Private Banks": "NIFTY PRIVATE BANK",
            "Public Banks": "NIFTY PSU BANK",
            "IT Services": "NIFTY IT",
            "Pharmaceuticals": "NIFTY PHARMA",
            "Automobiles": "NIFTY AUTO",
            "Metals": "NIFTY METAL",
            "FMCG": "NIFTY FMCG",
            "Energy": "NIFTY ENERGY",
            "Real Estate": "NIFTY REALTY",
            "Infrastructure": "NIFTY INFRA",
            "Financial Services": "NIFTY FINANCIAL SERVICES",
            "Media": "NIFTY MEDIA",
            "Consumer Durables": "NIFTY CONSUMER DURABLES",
            "Telecommunications": "NIFTY IT",
            "Oil & Gas": "NIFTY ENERGY",
            "Chemicals": "NIFTY METAL",
            "Cement": "NIFTY REALTY",
            "Textiles": "NIFTY FMCG",
            "Engineering": "NIFTY INFRA",
        }
        
        df["mapped_sector"] = df["Industry"].map(sector_mapping)
        
        return df

    def process_dhan_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and clean Dhan master data"""
        if df.empty:
            return pd.DataFrame()

        column_mapping = {
            "SEM_EXM_EXCH_ID": "exchange",
            "SEM_TRADING_SYMBOL": "symbol_id", 
            "SEM_SMST_SECURITY_ID": "security_id",
            "SEM_SERIES": "series",
            "SEM_SEGMENT": "segment",
            "SEM_EXCH_INSTRUMENT_TYPE": "exch_type",
            "SEM_INSTRUMENT_NAME": "instrument_type",
            "SEM_LOT_UNITS": "lot_size",
            "SM_SYMBOL_NAME": "symbol_name",
            "SEM_TICK_SIZE": "tick_size",
        }
        
        df = df.rename(columns=column_mapping)

        eq_df = df[
            (df["segment"] == "E") & 
            (df["exch_type"] == "ES") &
            (df["series"] == "EQ")
        ].copy()
        
        logger.info(f"Found {len(eq_df)} E + ES + EQ equity symbols (excluding ETF/Index)")
        
        eq_df["sector"] = "Unknown"
        eq_df["industry"] = "Unknown"
        
        return eq_df

    def save_to_database(self, df: pd.DataFrame):
        """Save processed data to SQLite"""
        if df.empty:
            logger.warning("No data to save")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("DELETE FROM symbols")

        timestamp = datetime.now().isoformat()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO symbols (
                    symbol_id, security_id, symbol_name, exchange, instrument_type,
                    isin, lot_size, tick_size, freeze_quantity,
                    sector, industry, nse_symbol, bse_symbol, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(row.get("symbol_id", "")),
                str(row.get("security_id", "")),
                str(row.get("symbol_name", "")),
                str(row.get("exchange", "")),
                str(row.get("instrument_type", "")),
                "",
                int(row.get("lot_size", 0)) if pd.notna(row.get("lot_size")) else 0,
                float(row.get("tick_size", 0)) if pd.notna(row.get("tick_size")) else 0,
                0,
                str(row.get("sector", "Unknown")),
                str(row.get("industry", "Unknown")),
                str(row.get("symbol_id", "")),
                str(row.get("symbol_id", "")),
                timestamp
            ))

        zerodha_sectors = self.fetch_zerodha_sectors()
        for sector_name, index_symbol in zerodha_sectors.items():
            cursor.execute("""
                INSERT OR REPLACE INTO sectors (sector_name, index_symbol, last_updated)
                VALUES (?, ?, ?)
            """, (sector_name, index_symbol, timestamp))

        conn.commit()
        conn.close()
        
        logger.info(f"Saved {len(df)} symbols and {len(zerodha_sectors)} sectors to database")

    def get_symbols_by_sector(self, sector: str) -> pd.DataFrame:
        """Get all symbols in a sector"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql(f"""
            SELECT symbol_id, symbol_name, exchange, instrument_type, lot_size
            FROM symbols 
            WHERE sector = '{sector}'
            ORDER BY symbol_name
        """, conn)
        conn.close()
        return df

    def get_symbol_info(self, symbol_id: str) -> Optional[Dict]:
        """Get information for a specific symbol"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM symbols WHERE symbol_id = ?", (symbol_id,))
        row = cursor.fetchone()
        
        conn.close()
        
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return None

    def search_symbols(self, query: str) -> pd.DataFrame:
        """Search symbols by name or symbol ID"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql(f"""
            SELECT symbol_id, symbol_name, exchange, instrument_type, sector
            FROM symbols 
            WHERE symbol_name LIKE '%{query}%' OR symbol_id LIKE '%{query}%'
            ORDER BY symbol_name
            LIMIT 50
        """, conn)
        conn.close()
        return df

    def get_equity_symbols(self, exchange: str = "NSE") -> List[str]:
        """Get list of equity symbols"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT symbol_id FROM symbols 
            WHERE exchange = ? AND instrument_type = 'EQ'
            ORDER BY symbol_id
        """, (exchange,))
        
        symbols = [row[0] for row in cursor.fetchall()]
        conn.close()
        return symbols

    def get_all_sectors(self) -> List[str]:
        """Get list of all sectors"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT sector_name FROM sectors ORDER BY sector_name")
        sectors = [row[0] for row in cursor.fetchall()]
        conn.close()
        return sectors

    def update(self) -> bool:
        """Update master data from sources"""
        try:
            df = self.fetch_dhan_master()
            if df.empty:
                return False

            processed_df = self.process_dhan_data(df)
            self.save_to_database(processed_df)
            
            logger.info("Master data update complete")
            return True

        except Exception as e:
            logger.error(f"Error updating master data: {e}")
            return False

    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM symbols")
        total_symbols = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT sector) FROM symbols WHERE sector IS NOT NULL")
        sector_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM sectors")
        sectors = cursor.fetchone()[0]

        cursor.execute("SELECT exchange, COUNT(*) FROM symbols GROUP BY exchange")
        exchange_counts = dict(cursor.fetchall())

        cursor.execute("SELECT instrument_type, COUNT(*) FROM symbols GROUP BY instrument_type")
        instrument_counts = dict(cursor.fetchall())

        conn.close()

        return {
            "total_symbols": total_symbols,
            "sector_count": sector_count,
            "sectors": sectors,
            "exchange_counts": exchange_counts,
            "instrument_counts": instrument_counts
        }


def main():
    """Main entry point for master data collection"""
    collector = MasterDataCollector()
    
    logger.info("Starting master data collection...")
    
    success = collector.update()
    
    if success:
        stats = collector.get_database_stats()
        logger.info(f"Master data stats: {stats}")
        
        symbols = collector.get_equity_symbols("NSE")[:10]
        logger.info(f"Sample NSE symbols: {symbols}")
    else:
        logger.error("Failed to update master data")


if __name__ == "__main__":
    main()
