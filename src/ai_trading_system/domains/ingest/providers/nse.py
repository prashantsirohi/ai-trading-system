import os
import zipfile
import io
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import pandas as pd
import requests
from core.logging import logger


class NSECollector:
    """
    NSE Data Collector for fetching bhavcopy and equity data.
    """

    def __init__(self, data_dir: str = "data/raw/NSE_EQ"):
        self.data_dir = data_dir
        self.base_url = "https://www.nseindia.com"
        self.bhavcopy_url = "https://www.nseindia.com/api/reports?archives=cm"
        self.session = requests.Session()
        self._init_session()

    def _candidate_bhavcopy_urls(self, date: str) -> list[str]:
        date_compact = date.replace("-", "")
        dt = datetime.fromisoformat(date)
        ddmmyyyy = dt.strftime("%d%m%Y")
        return [
            f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv",
            f"https://www.nseindia.com/content/nsccl/CM{date_compact}bhav.csv.zip",
        ]

    def _local_bhavcopy_path(self, date: str) -> str:
        dt = datetime.fromisoformat(date)
        filename = f"nse_{dt.strftime('%d%b%Y').upper()}.csv"
        return os.path.join(self.data_dir, filename)

    def _read_bhavcopy_response(self, response: requests.Response) -> pd.DataFrame:
        content_type = response.headers.get("Content-Type", "").lower()
        if ".zip" in response.url.lower() or "zip" in content_type:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for name in z.namelist():
                    if name.endswith(".csv"):
                        with z.open(name) as f:
                            return pd.read_csv(f)
            return pd.DataFrame()
        return pd.read_csv(io.StringIO(response.text))

    def _init_session(self):
        """Initialize session with headers"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        }
        self.session.headers.update(headers)

    def get_bhavcopy(self, date: Optional[str] = None) -> pd.DataFrame:
        """
        Download NSE bhavcopy for a given date.
        
        Args:
            date: Date in YYYY-MM-DD format. If None, uses previous trading day.
        """
        if date is None:
            date = self._get_previous_trading_day()

        try:
            local_path = self._local_bhavcopy_path(date)
            if os.path.exists(local_path):
                df = pd.read_csv(local_path)
                if not df.empty:
                    logger.info(f"Loaded bhavcopy from local archive for {date}: {local_path}")
                    return df

            logger.info(f"Downloading bhavcopy for {date}")
            for url in self._candidate_bhavcopy_urls(date):
                response = self.session.get(url, timeout=30)
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                df = self._read_bhavcopy_response(response)
                if not df.empty:
                    os.makedirs(self.data_dir, exist_ok=True)
                    df.to_csv(local_path, index=False)
                    return df

            return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error downloading bhavcopy: {e}")
            return pd.DataFrame()

    def _get_previous_trading_day(self) -> str:
        """Get previous trading day (excluding weekends)"""
        date = datetime.now()
        for _ in range(7):
            date -= timedelta(days=1)
            if date.weekday() < 5:
                return date.strftime("%Y-%m-%d")
        return date.strftime("%Y-%m-%d")

    def fetch_equity_list(self) -> pd.DataFrame:
        """Fetch list of NSE equities"""
        try:
            url = f"{self.base_url}/api/equity-capitalisation?index=cap"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            return pd.DataFrame(data.get("data", []))
        except Exception as e:
            logger.error(f"Error fetching equity list: {e}")
            return pd.DataFrame()

    def fetch_index_constituents(self, index: str = "NIFTY 50") -> List[str]:
        """Fetch symbols in an index"""
        try:
            url = f"{self.base_url}/api/index-nifties?index={index}"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            return [item.get("symbol") for item in data.get("data", [])]
        except Exception as e:
            logger.error(f"Error fetching index constituents: {e}")
            return []

    def get_ohlc_data(
        self,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get OHLC data for a symbol.
        Uses NSE's historical data API.
        """
        if from_date is None:
            from_date = (datetime.now() - timedelta(365)).strftime("%Y-%m-%d")
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")

        try:
            url = f"{self.base_url}/api/historical/cm/equity/{symbol}"
            params = {
                "from": from_date,
                "to": to_date
            }
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "data" in data:
                df = pd.DataFrame(data["data"])
                df["timestamp"] = pd.to_datetime(df["CH_TIMESTAMP"])
                df = df.rename(columns={
                    "CH_OPEN": "open",
                    "CH_HIGH": "high",
                    "CH_LOW": "low",
                    "CH_CLOSE": "close",
                    "CH_VOLUME": "volume"
                })
                df = df[["timestamp", "open", "high", "low", "close", "volume"]]
                return df

            return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error fetching OHLC data for {symbol}: {e}")
            return pd.DataFrame()

    def save_to_parquet(self, df: pd.DataFrame, symbol: str) -> str:
        """Save dataframe to parquet file"""
        if df.empty:
            return ""

        os.makedirs(self.data_dir, exist_ok=True)
        filepath = os.path.join(self.data_dir, f"{symbol}.parquet")
        df.to_parquet(filepath, index=True)
        logger.info(f"Saved data to {filepath}")
        return filepath

    def load_from_parquet(self, symbol: str) -> pd.DataFrame:
        """Load dataframe from parquet file"""
        filepath = os.path.join(self.data_dir, f"{symbol}.parquet")
        if os.path.exists(filepath):
            return pd.read_parquet(filepath)
        return pd.DataFrame()

    def get_quote(self, symbol: str) -> Dict:
        """Get real-time quote for a symbol"""
        try:
            url = f"{self.base_url}/api/quote-advance?symbol={symbol}"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching quote for {symbol}: {e}")
            return {}

    def get_market_status(self) -> Dict:
        """Get market open/close status"""
        try:
            url = f"{self.base_url}/api/marketStatus"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching market status: {e}")
            return {}
