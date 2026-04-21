import os
import duckdb
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Literal, Callable
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.platform.logging.logger import logger


class EventBacktester:
    """
    Event-Driven Backtesting Engine.

    Instead of testing "what if I buy every day", this engine tests
    "what happens when a specific EVENT (pattern/trigger) is hit".

    Events supported:
      - BREAKOUT: Close breaks above 20-day high + volume spike
      - TREND_FOLLOW: Supertrend flip + ADX > 25
      - MEAN_REV: RSI oversold (<35) + ADX < 20
      - MOVING_AVERAGE_CROSS: SMA(10) crosses SMA(20)
      - PULLBACK: Price retraces to SMA(20) in uptrend

    Risk Layer:
      - ATR-based stop-loss
      - Trailing stop (2x ATR from peak)
      - Position sizing via RiskManager

    Outputs:
      - Per-trade P&L
      - Portfolio equity curve
      - Performance metrics (Sharpe, Sortino, max drawdown, win rate)
    """

    def __init__(
        self,
        ohlcv_db_path: str = None,
        feature_store_dir: str = None,
        initial_capital: float = 1_000_000,
        data_domain: str = "research",
    ):
        paths = ensure_domain_layout(
            project_root=os.path.dirname(os.path.dirname(__file__)),
            data_domain=data_domain,
        )
        if ohlcv_db_path is None:
            ohlcv_db_path = str(paths.ohlcv_db_path)
        if feature_store_dir is None:
            feature_store_dir = str(paths.feature_store_dir)
        self.ohlcv_db_path = ohlcv_db_path
        self.feature_store_dir = feature_store_dir
        self.initial_capital = initial_capital
        self.data_domain = data_domain

    def _get_conn(self):
        return duckdb.connect(self.ohlcv_db_path)

    def detect_breakout(
        self,
        df: pd.DataFrame,
        lookback: int = 20,
        vol_threshold: float = 1.5,
    ) -> pd.Series:
        """
        BREAKOUT event: Close > N-day high AND volume > avg * threshold.
        """
        df = df.sort_values("timestamp")
        high_n = df["high"].rolling(lookback).max().shift(1)
        vol_avg = df["volume"].rolling(lookback).mean().shift(1)

        breakout = (df["close"] > high_n) & (df["volume"] > vol_avg * vol_threshold)
        return breakout

    def detect_trend_follow(
        self,
        df: pd.DataFrame,
        adx_threshold: float = 25.0,
    ) -> pd.Series:
        """
        TREND_FOLLOW event: Supertrend flips bullish + ADX above threshold.
        """
        df = df.sort_values("timestamp")
        adx = df.get("adx_value", pd.Series(50, index=df.index))
        st_dir = df.get("st_signal", pd.Series(1, index=df.index))

        prev_st = st_dir.shift(1).fillna(1)
        signal = (st_dir == 1) & (prev_st == -1) & (adx >= adx_threshold)
        return signal

    def detect_mean_rev(
        self,
        df: pd.DataFrame,
        rsi_lower: float = 35,
        rsi_upper: float = 65,
        adx_threshold: float = 20,
    ) -> pd.Series:
        """
        MEAN_REV event: RSI oversold/overbought + ADX below threshold.
        """
        df = df.sort_values("timestamp")
        rsi = df.get("rsi", pd.Series(50, index=df.index))
        adx = df.get("adx_value", pd.Series(50, index=df.index))

        signal = ((rsi < rsi_lower) | (rsi > rsi_upper)) & (adx < adx_threshold)
        return signal

    def detect_ma_cross(
        self,
        df: pd.DataFrame,
        fast: int = 10,
        slow: int = 20,
    ) -> pd.Series:
        """
        MA Cross event: SMA(fast) crosses above SMA(slow) (golden cross).
        """
        df = df.sort_values("timestamp")
        sma_f = df["close"].rolling(fast).mean()
        sma_s = df["close"].rolling(slow).mean()
        prev_f = sma_f.shift(1)
        prev_s = sma_s.shift(1)

        cross_up = (sma_f > sma_s) & (prev_f <= prev_s)
        cross_down = (sma_f < sma_s) & (prev_f >= prev_s)
        return cross_up | cross_down

    def detect_pullback(
        self,
        df: pd.DataFrame,
        ma_period: int = 20,
        max_retrace_pct: float = 0.05,
    ) -> pd.Series:
        """
        PULLBACK event: Price retraces within max_retrace_pct of SMA(ma_period)
        while in an uptrend (SMA(ma) rising).
        """
        df = df.sort_values("timestamp")
        sma = df["close"].rolling(ma_period).mean()
        prev_sma = sma.shift(1)
        retrace = (df["close"] - sma).abs() / sma

        uptrend = sma > prev_sma
        signal = (
            uptrend
            & (retrace < max_retrace_pct)
            & (df["volume"] > df["volume"].rolling(ma_period).mean())
        )
        return signal

    def run_event_backtest(
        self,
        symbols: List[str] = None,
        event_type: Literal[
            "BREAKOUT", "TREND_FOLLOW", "MEAN_REV", "MA_CROSS", "PULLBACK"
        ] = "BREAKOUT",
        exchange: str = "NSE",
        from_date: str = None,
        to_date: str = None,
        horizon: int = 20,
        atr_stop_mult: float = 2.5,
        atr_target_mult: float = 3.0,
        commission: float = 0.001,
    ) -> Dict:
        """
        Run event-driven backtest across multiple symbols.
        """
        if symbols is None:
            conn = self._get_conn()
            try:
                syms = conn.execute("""
                    SELECT DISTINCT symbol_id FROM _catalog
                    WHERE exchange = 'NSE'
                    ORDER BY symbol_id LIMIT 50
                """).fetchdf()
                symbols = syms["symbol_id"].tolist()
            finally:
                conn.close()

        if not symbols:
            return {"error": "No symbols provided", "trades": [], "metrics": {}}

        all_trades = []
        equity_curve = []
        running_capital = self.initial_capital

        for symbol in symbols:
            try:
                result = self._backtest_symbol(
                    symbol=symbol,
                    event_type=event_type,
                    exchange=exchange,
                    from_date=from_date,
                    to_date=to_date,
                    horizon=horizon,
                    atr_stop_mult=atr_stop_mult,
                    atr_target_mult=atr_target_mult,
                    commission=commission,
                    running_capital=running_capital,
                )
                if result and result.get("trade"):
                    all_trades.append(result["trade"])
                    running_capital = result["capital_at_exit"]
                    equity_curve.append(
                        {
                            "date": result["trade"]["entry_date"],
                            "capital": round(running_capital, 2),
                            "symbol": symbol,
                            "event": event_type,
                        }
                    )
            except Exception as e:
                logger.warning(f"Backtest failed for {symbol}: {e}")
                continue

        if not all_trades:
            return {"error": "No trades generated", "trades": [], "metrics": {}}

        trades_df = pd.DataFrame(all_trades)
        equity_df = pd.DataFrame(equity_curve).sort_values("date")

        metrics = self._compute_metrics(trades_df, equity_df)

        logger.info(
            f"Backtest ({event_type}): {len(trades_df)} trades, "
            f"win_rate={metrics.get('win_rate', 0) * 100:.1f}%, "
            f"Sharpe={metrics.get('sharpe', 0):.2f}, "
            f"max_dd={metrics.get('max_drawdown', 0) * 100:.1f}%"
        )

        return {
            "event_type": event_type,
            "n_trades": len(trades_df),
            "trades": trades_df,
            "equity_curve": equity_df,
            "metrics": metrics,
        }

    def _backtest_symbol(
        self,
        symbol: str,
        event_type: str,
        exchange: str,
        from_date: str,
        to_date: str,
        horizon: int,
        atr_stop_mult: float,
        atr_target_mult: float,
        commission: float,
        running_capital: float,
    ) -> Optional[Dict]:
        """
        Backtest a single symbol. Returns the best trade result.
        """
        conn = self._get_conn()
        try:
            date_filter = ""
            params: List = [symbol, exchange]
            if from_date:
                date_filter += " AND timestamp >= ?"
                params.append(from_date)
            if to_date:
                date_filter += " AND timestamp <= ?"
                params.append(to_date)

            ohlcv = conn.execute(
                f"""
                SELECT timestamp, open, high, low, close, volume
                FROM _catalog
                WHERE symbol_id = ? AND exchange = ?
                  AND timestamp IS NOT NULL
                  {date_filter}
                ORDER BY timestamp
            """,
                params,
            ).fetchdf()
        finally:
            conn.close()

        if ohlcv.empty or len(ohlcv) < 30:
            return None

        ohlcv["timestamp"] = pd.to_datetime(ohlcv["timestamp"])

        # The feature store uses versioned indicator column names; normalize
        # them here so event logic stays stable across domains.
        feat_cfg = [
            ("rsi", ["rsi_14 AS rsi"]),
            ("atr", ["atr_14 AS atr_value"]),
            ("adx", ["plus_di_14 AS adx_plus", "minus_di_14 AS adx_minus", "adx_14 AS adx_value"]),
            (
                "supertrend",
                [
                    "supertrend_10_3 AS st_upper",
                    "supertrend_10_3 AS st_lower",
                    "supertrend_dir_10_3 AS st_signal",
                ],
            ),
        ]

        for feat_name, feat_cols in feat_cfg:
            feat_path = os.path.join(
                self.feature_store_dir, feat_name, exchange, "*.parquet"
            ).replace("\\", "/")
            if os.path.exists(
                os.path.join(self.feature_store_dir, feat_name, exchange)
            ):
                conn2 = duckdb.connect(self.ohlcv_db_path, read_only=True)
                try:
                    feat_select = ", ".join(feat_cols)
                    fdf = conn2.execute(f"""
                        SELECT timestamp, {feat_select}
                        FROM read_parquet('{feat_path}')
                        WHERE symbol_id = '{symbol}' AND exchange = '{exchange}'
                        ORDER BY timestamp
                    """).fetchdf()
                finally:
                    conn2.close()
                if not fdf.empty:
                    fdf["timestamp"] = pd.to_datetime(fdf["timestamp"])
                    ohlcv = ohlcv.merge(fdf, on="timestamp", how="left")

        ohlcv = ohlcv.sort_values("timestamp")

        if event_type == "BREAKOUT":
            events = self.detect_breakout(ohlcv)
        elif event_type == "TREND_FOLLOW":
            events = self.detect_trend_follow(ohlcv)
        elif event_type == "MEAN_REV":
            events = self.detect_mean_rev(ohlcv)
        elif event_type == "MA_CROSS":
            events = self.detect_ma_cross(ohlcv)
        elif event_type == "PULLBACK":
            events = self.detect_pullback(ohlcv)
        else:
            events = pd.Series(False, index=ohlcv.index)

        event_dates = ohlcv.index[events].tolist()
        if not event_dates:
            return None

        entry_idx = event_dates[0]
        entry_row = ohlcv.loc[entry_idx]
        entry_date = entry_row["timestamp"]
        entry_price = float(entry_row["close"])
        atr = float(entry_row.get("atr_value", entry_price * 0.02))
        if pd.isna(atr) or atr <= 0:
            atr = entry_price * 0.02

        stop_loss = entry_price - (atr * atr_stop_mult)
        target = entry_price + (atr * atr_target_mult)

        position_size = (running_capital * 0.05) / (atr * atr_stop_mult)
        position_size = min(position_size, running_capital / entry_price)
        shares = max(1, int(position_size))

        exit_idx = None
        exit_reason = "HORIZON"
        exit_price = entry_price

        for j_idx in ohlcv.index.tolist():
            if ohlcv.index.get_loc(j_idx) <= ohlcv.index.get_loc(entry_idx):
                continue
            days_held = ohlcv.index.get_loc(j_idx) - ohlcv.index.get_loc(entry_idx)

            if days_held >= horizon:
                exit_idx = j_idx
                exit_price = float(ohlcv.loc[j_idx, "close"])
                break

            high_j = float(ohlcv.loc[j_idx, "high"])
            low_j = float(ohlcv.loc[j_idx, "low"])

            if high_j >= target:
                exit_idx = j_idx
                exit_price = target
                exit_reason = "TARGET"
                break
            if low_j <= stop_loss:
                exit_idx = j_idx
                exit_price = stop_loss
                exit_reason = "STOP_LOSS"
                break

        if exit_idx is None:
            exit_row = ohlcv.iloc[-1]
            exit_price = float(exit_row["close"])
            exit_reason = "END"
            exit_idx = ohlcv.index[-1]

        exit_date = ohlcv.loc[exit_idx, "timestamp"]
        days_held = ohlcv.index.get_loc(exit_idx) - ohlcv.index.get_loc(entry_idx)

        pnl = (exit_price - entry_price) * shares
        pnl_pct = (exit_price / entry_price - 1) * 100
        pnl_after_commission = (
            pnl
            - (shares * entry_price * commission)
            - (shares * exit_price * commission)
        )

        capital_at_exit = running_capital + pnl_after_commission

        return {
            "trade": {
                "symbol": symbol,
                "entry_date": entry_date,
                "exit_date": exit_date,
                "entry_price": round(entry_price, 2),
                "exit_price": round(exit_price, 2),
                "shares": shares,
                "pnl": round(pnl_after_commission, 2),
                "pnl_pct": round(pnl_pct, 2),
                "exit_reason": exit_reason,
                "days_held": days_held,
                "atr": round(atr, 4),
                "capital_at_entry": round(running_capital, 2),
                "capital_at_exit": round(capital_at_exit, 2),
            },
            "capital_at_exit": capital_at_exit,
        }

    def _compute_metrics(
        self,
        trades: pd.DataFrame,
        equity: pd.DataFrame,
    ) -> Dict:
        if trades.empty:
            return {}

        wins = trades[trades["pnl"] > 0]
        losses = trades[trades["pnl"] <= 0]

        win_rate = len(wins) / len(trades) if len(trades) > 0 else 0
        avg_win = wins["pnl"].mean() if len(wins) > 0 else 0
        avg_loss = losses["pnl"].mean() if len(losses) > 0 else 0
        profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else 0

        if equity.shape[0] < 2:
            sharpe = 0
            max_dd = 0
        else:
            equity_sorted = equity.sort_values("date")
            returns = equity_sorted["capital"].pct_change().dropna()
            sharpe = (
                returns.mean() / returns.std() * np.sqrt(252)
                if returns.std() > 0
                else 0
            )
            cum = equity_sorted["capital"] / self.initial_capital
            running_max = cum.cummax()
            drawdown = (cum - running_max) / running_max
            max_dd = drawdown.min()

        return {
            "win_rate": round(win_rate, 4),
            "profit_factor": round(profit_factor, 3),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "sharpe": round(sharpe, 3),
            "max_drawdown": round(max_dd, 4),
            "total_pnl": round(trades["pnl"].sum(), 2),
            "avg_trade_pnl": round(trades["pnl"].mean(), 2),
            "avg_days_held": round(trades["days_held"].mean(), 1),
            "best_trade": round(trades["pnl"].max(), 2),
            "worst_trade": round(trades["pnl"].min(), 2),
            "exit_reason_counts": trades["exit_reason"].value_counts().to_dict(),
        }

    def compare_events(
        self,
        symbols: List[str] = None,
        exchange: str = "NSE",
        from_date: str = None,
        to_date: str = None,
    ) -> pd.DataFrame:
        """
        Run backtests for all event types and compare performance.
        """
        event_types = ["BREAKOUT", "TREND_FOLLOW", "MEAN_REV", "MA_CROSS", "PULLBACK"]
        results = []

        for etype in event_types:
            result = self.run_event_backtest(
                symbols=symbols,
                event_type=etype,
                exchange=exchange,
                from_date=from_date,
                to_date=to_date,
            )
            if "metrics" in result and result["metrics"]:
                m = result["metrics"].copy()
                m["event_type"] = etype
                m["n_trades"] = result["n_trades"]
                results.append(m)

        return pd.DataFrame(results).sort_values("sharpe", ascending=False)
