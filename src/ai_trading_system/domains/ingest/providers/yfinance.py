"""
Yahoo Finance data collector - alternative to Dhan API.
Fetches OHLCV data for NSE stocks.
"""

import time
import warnings
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
from ai_trading_system.platform.logging.logger import logger


def _download_yfinance(*args, **kwargs) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*ChainedAssignmentError.*",
            category=FutureWarning,
            module=r"yfinance(\.|$)",
        )
        return yf.download(*args, **kwargs)


class YFinanceCollector:
    """Collects OHLCV data from Yahoo Finance."""

    def __init__(self, batch_size: int = 100, delay_between_batches: float = 1.0):
        self.batch_size = batch_size
        self.delay = delay_between_batches

    def get_symbols(self, exchanges: List[str] = None) -> List[str]:
        """Get symbols from master DB."""
        import sqlite3

        conn = sqlite3.connect("data/masterdata.db")
        symbols = conn.execute(
            "SELECT symbol_id FROM symbols WHERE exchange = 'NSE'"
        ).fetchall()
        conn.close()
        return [s[0] for s in symbols]

    def fetch_batch(self, symbols: List[str], period: str = "1y") -> pd.DataFrame:
        """Fetch OHLCV for batch of symbols."""
        # Convert to yfinance format
        nse_symbols = [f"{s}.NS" for s in symbols]

        try:
            data = _download_yfinance(
                nse_symbols,
                period=period,
                progress=False,
                threads=True,
                auto_adjust=True,
            )

            if data.empty:
                return pd.DataFrame()

            # Reshape: multi-index columns -> close only
            if isinstance(data.columns, pd.MultiIndex):
                close = data["Close"].reset_index()
                close = close.melt(
                    id_vars="Date", var_name="symbol", value_name="close"
                )
                close["symbol"] = close["symbol"].str.replace(".NS", "", regex=False)

                # Add other columns from original data
                for col in ["Open", "High", "Low", "Volume"]:
                    df_col = data[col].reset_index()
                    df_col = df_col.melt(
                        id_vars="Date", var_name="symbol", value_name=col.lower()
                    )
                    df_col["symbol"] = df_col["symbol"].str.replace(
                        ".NS", "", regex=False
                    )
                    close = close.merge(df_col, on=["Date", "symbol"])

                close = close.rename(columns={"Date": "timestamp"})
                close["timestamp"] = pd.to_datetime(close["timestamp"]).dt.tz_localize(
                    None
                )

                return close

            return pd.DataFrame()

        except Exception as e:
            logger.warning(f"Error fetching batch: {e}")
            return pd.DataFrame()

    def fetch_all(
        self,
        exchanges: List[str] = None,
        period: str = "1y",
    ) -> pd.DataFrame:
        """Fetch data for all symbols in batches."""
        symbols = self.get_symbols(exchanges)
        logger.info(f"Fetching {len(symbols)} symbols from Yahoo Finance...")

        all_data = []
        n_batches = (len(symbols) + self.batch_size - 1) // self.batch_size

        for i in range(0, len(symbols), self.batch_size):
            batch = symbols[i : i + self.batch_size]
            batch_num = i // self.batch_size + 1

            logger.info(f"Batch {batch_num}/{n_batches}: {len(batch)} symbols")

            df = self.fetch_batch(batch, period=period)
            if not df.empty:
                all_data.append(df)

            if batch_num < n_batches:
                time.sleep(self.delay)

        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            logger.info(f"Fetched {len(result)} total rows")
            return result

        return pd.DataFrame()

    def get_latest_prices(self, symbols: List[str]) -> Dict[str, float]:
        """Get latest closing prices for given symbols."""
        nse_symbols = [f"{s}.NS" for s in symbols]

        try:
            data = _download_yfinance(
                nse_symbols,
                period="5d",
                progress=False,
                threads=True,
            )

            if data.empty:
                return {}

            # Get last close for each symbol
            prices = {}
            if isinstance(data.columns, pd.MultiIndex):
                closes = data["Close"].iloc[-1]
                for sym in symbols:
                    ns_sym = f"{sym}.NS"
                    if ns_sym in closes.index:
                        prices[sym] = float(closes[ns_sym])

            return prices

        except Exception as e:
            logger.warning(f"Error getting latest prices: {e}")
            return {}


if __name__ == "__main__":
    # Test
    collector = YFinanceCollector(batch_size=50)

    # Test fetching a few symbols
    test_symbols = ["TCS", "INFY", "BELRISE", "RELIANCE", "HDFCBANK"]
    prices = collector.get_latest_prices(test_symbols)
    print("Latest prices:")
    for sym, price in prices.items():
        print(f"  {sym}: ₹{price:.2f}")
