import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    import vectorbt as vbt
    VBT_AVAILABLE = True
except ImportError:
    VBT_AVAILABLE = False
    logger.warning("VectorBT not available, using basic backtester")


class BacktestEngine:
    """
    Backtesting Engine - Evaluates strategy performance.
    """

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.results = {}

    def load_data(self, symbol: str) -> pd.DataFrame:
        """Load OHLCV data for backtesting"""
        filepath = os.path.join(self.data_dir, "raw", "NSE_EQ", f"{symbol}.parquet")
        if os.path.exists(filepath):
            return pd.read_parquet(filepath)
        return pd.DataFrame()

    def run_backtest(
        self,
        symbol: str,
        strategy: str = "supertrend",
        initial_capital: float = 100000,
        commission: float = 0.1
    ) -> Dict:
        """
        Run backtest for a symbol with given strategy.
        
        Args:
            symbol: Trading symbol
            strategy: Strategy name (supertrend, rsi, macd)
            initial_capital: Starting capital
            commission: Commission percentage
        """
        df = self.load_data(symbol)
        if df.empty:
            return {}

        if "close" not in df.columns:
            return {}

        if VBT_AVAILABLE:
            return self._run_vectorbt_backtest(df, strategy, initial_capital, commission)
        else:
            return self._run_basic_backtest(df, strategy, initial_capital, commission)

    def _run_vectorbt_backtest(
        self,
        df: pd.DataFrame,
        strategy: str,
        initial_capital: float,
        commission: float
    ) -> Dict:
        """Run backtest using VectorBT"""
        close = df["close"].values
        high = df.get("high", pd.Series()).values
        low = df.get("low", pd.Series()).values

        if strategy == "supertrend":
            from numpy_indicators import Supertrend
            st = Supertrend(high=high, low=low, close=close, period=10, multiplier=3)
            entries = close > st
            exits = close < st
        elif strategy == "rsi":
            rsi = pd.Series(close).ta.rsi(length=14)
            entries = rsi < 30
            exits = rsi > 70
        elif strategy == "macd":
            macd = pd.Series(close).ta.macd(fast=12, slow=26, signal=9)
            entries = macd["MACD_12_26_9"] > macd["MACDh_12_26_9"]
            exits = macd["MACD_12_26_9"] < macd["MACDh_12_26_9"]
        else:
            entries = pd.Series(False, index=range(len(close)))
            exits = pd.Series(False, index=range(len(close)))

        try:
            pf = vbt.Portfolio.from_signals(
                close,
                entries=entries,
                exits=exits,
                init_cash=initial_capital,
                commission=commission / 100
            )

            stats = pf.stats()
            return {
                "symbol": symbol,
                "strategy": strategy,
                "total_return": stats.get("total_return", 0),
                "sharpe_ratio": stats.get("sharpe_ratio", 0),
                "max_drawdown": stats.get("max_drawdown", 0),
                "win_rate": stats.get("win_rate", 0),
                "profit_factor": stats.get("profit_factor", 0),
                "total_trades": stats.get("total_trades", 0),
                "avg_trade": stats.get("avg_trade", 0),
            }
        except Exception as e:
            logger.error(f"VectorBT backtest error: {e}")
            return self._run_basic_backtest(df, strategy, initial_capital, commission)

    def _run_basic_backtest(
        self,
        df: pd.DataFrame,
        strategy: str,
        initial_capital: float,
        commission: float
    ) -> Dict:
        """Run basic backtest without VectorBT"""
        close = df["close"]
        returns = close.pct_change()

        if strategy == "supertrend":
            if "SUPERT_10_3" in df.columns:
                entries = close > df["SUPERT_10_3"]
            else:
                return {}
        elif strategy == "rsi":
            if "RSI" in df.columns:
                entries = df["RSI"] < 30
            else:
                return {}
        elif strategy == "buy_hold":
            entries = pd.Series(True, index=close.index)
        else:
            return {}

        position = entries.shift(1).fillna(False)
        strategy_returns = position * returns

        strategy_returns = strategy_returns - (commission / 100)

        cumulative_returns = (1 + strategy_returns).cumprod()
        total_return = (cumulative_returns.iloc[-1] - 1) * 100 if len(cumulative_returns) > 0 else 0

        running_max = cumulative_returns.cummax()
        drawdown = (cumulative_returns - running_max) / running_max
        max_drawdown = drawdown.min() * 100

        sharpe_ratio = self._calculate_sharpe(strategy_returns)

        win_trades = (strategy_returns > 0).sum()
        total_trades = (strategy_returns != 0).sum()
        win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0

        profits = strategy_returns[strategy_returns > 0].sum()
        losses = abs(strategy_returns[strategy_returns < 0].sum())
        profit_factor = profits / losses if losses > 0 else 0

        return {
            "symbol": symbol,
            "strategy": strategy,
            "total_return": total_return,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "total_trades": int(total_trades),
            "avg_trade": total_return / total_trades if total_trades > 0 else 0,
        }

    def _calculate_sharpe(self, returns: pd.Series, risk_free_rate: float = 0.06) -> float:
        """Calculate Sharpe ratio"""
        if returns.std() == 0:
            return 0
        excess_returns = returns.mean() * 252 - risk_free_rate
        return excess_returns / (returns.std() * np.sqrt(252))

    def optimize_strategy(
        self,
        symbol: str,
        strategy: str = "supertrend",
        param_grid: Optional[Dict] = None
    ) -> pd.DataFrame:
        """Optimize strategy parameters"""
        if param_grid is None:
            param_grid = {
                "supertrend": {"period": [7, 10, 14], "multiplier": [2, 3, 4]},
                "rsi": {"length": [7, 14, 21], "oversold": [25, 30, 35]},
                "macd": {"fast": [8, 12, 16], "slow": [20, 26, 32]},
            }

        results = []
        params = param_grid.get(strategy, {})

        for period in params.get("period", [10]):
            for multiplier in params.get("multiplier", [3]):
                result = self.run_backtest(
                    symbol=symbol,
                    strategy=strategy,
                    initial_capital=100000
                )
                if result:
                    result["period"] = period
                    result["multiplier"] = multiplier
                    results.append(result)

        return pd.DataFrame(results)

    def compare_strategies(self, symbol: str, strategies: List[str]) -> pd.DataFrame:
        """Compare multiple strategies"""
        results = []
        for strategy in strategies:
            result = self.run_backtest(symbol, strategy)
            if result:
                results.append(result)
        return pd.DataFrame(results)

    def run_walk_forward(
        self,
        symbol: str,
        train_window: int = 252,
        test_window: int = 63,
        strategy: str = "supertrend"
    ) -> Dict:
        """Run walk-forward analysis"""
        df = self.load_data(symbol)
        if df.empty or len(df) < train_window + test_window:
            return {}

        results = []
        for i in range(train_window, len(df) - test_window, test_window):
            train_df = df.iloc[i - train_window:i]
            test_df = df.iloc[i:i + test_window]

            train_result = self._run_basic_backtest(train_df, strategy, 100000, 0.1)
            test_result = self._run_basic_backtest(test_df, strategy, 100000, 0.1)

            if train_result and test_result:
                results.append({
                    "train_period": f"{train_df.index[0]} to {train_df.index[-1]}",
                    "test_period": f"{test_df.index[0]} to {test_df.index[-1]}",
                    "train_return": train_result.get("total_return", 0),
                    "test_return": test_result.get("total_return", 0),
                    "train_sharpe": train_result.get("sharpe_ratio", 0),
                    "test_sharpe": test_result.get("sharpe_ratio", 0),
                })

        return {
            "walk_forward_results": pd.DataFrame(results),
            "avg_test_return": np.mean([r["test_return"] for r in results]) if results else 0,
        }

    def save_results(self, results: Dict, name: str) -> str:
        """Save backtest results"""
        os.makedirs(os.path.join(self.data_dir, "backtests"), exist_ok=True)
        filepath = os.path.join(self.data_dir, "backtests", f"{name}.parquet")

        df = pd.DataFrame([results])
        df.to_parquet(filepath, index=True)
        logger.info(f"Saved backtest results to {filepath}")
        return filepath
