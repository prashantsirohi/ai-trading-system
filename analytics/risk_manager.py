import os
import duckdb
import pandas as pd
import numpy as np
from typing import Optional, Dict, List
from core.paths import ensure_domain_layout
from core.logging import logger


def compute_atr_position_size(
    capital: float,
    risk_per_trade: float,
    entry_price: float,
    atr: float,
    atr_multiple: float = 2.0,
) -> int:
    """Return ATR-based share quantity for a single position."""
    if capital <= 0 or entry_price <= 0 or atr <= 0:
        return 0
    risk_amount = float(capital) * float(risk_per_trade)
    stop_distance = float(atr) * float(atr_multiple)
    if stop_distance <= 0:
        return 0
    qty = int(risk_amount / stop_distance)
    return max(qty, 0)


class RiskManager:
    """
    Risk Management Engine.

    Responsibilities:
      - Volatility-based position sizing (ATR)
      - Portfolio-level risk budgeting (max drawdown, max position)
      - Stop-loss computation per signal
      - Risk-adjusted return scoring
      - Meta-risk: regime-aware risk adjustments
    """

    DEFAULT_RISK = {
        "max_portfolio_risk_pct": 0.02,
        "max_position_pct": 0.05,
        "max_positions": 20,
        "atr_multiplier_stop": 2.5,
        "atr_multiplier_position": 1.5,
        "risk_per_trade_pct": 0.01,
    }

    def __init__(
        self,
        ohlcv_db_path: str = None,
        feature_store_dir: str = None,
        config: Dict = None,
        data_domain: str = "operational",
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
        self.config = {**self.DEFAULT_RISK, **(config or {})}
        self.data_domain = data_domain

    def _get_conn(self):
        return duckdb.connect(self.ohlcv_db_path)

    def compute_position_size(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        capital: float = 1_000_000,
        regime: str = "TREND",
        regime_multiplier: float = 1.0,
    ) -> Dict:
        """
        Compute position size for a single trade.

        Strategy:
          - Base risk = capital * risk_per_trade_pct
          - ATR-based stop = close - (atr * atr_multiplier_stop)
          - Position size = base_risk / (atr * atr_multiplier_stop)
          - Cap at max_position_pct of capital
          - In MEAN_REV regime: reduce position by 40% (use tighter stops)

        Returns:
            dict with: shares, position_value, risk_amount, stop_loss, atr
        """
        atr = self._get_atr(symbol_id, exchange)
        conn = self._get_conn()
        try:
            row = conn.execute(
                """
                SELECT close FROM _catalog
                WHERE symbol_id = ? AND exchange = ?
                ORDER BY timestamp DESC LIMIT 1
            """,
                (symbol_id, exchange),
            ).fetchone()
        finally:
            conn.close()

        close = float(row[0]) if row else 0
        if close == 0 or atr is None or atr == 0:
            return {
                "symbol_id": symbol_id,
                "shares": 0,
                "position_value": 0,
                "risk_amount": 0,
                "stop_loss": 0,
                "atr": 0,
                "regime_multiplier": regime_multiplier,
            }

        trend_like = {"TREND", "STRONG_TREND", "STRONG_BULL_TREND", "BULLISH_MIXED"}
        bear_like = {"STRONG_BEAR_TREND", "BEARISH_MIXED"}
        if regime in trend_like:
            regime_mult = regime_multiplier
        elif regime in bear_like:
            regime_mult = regime_multiplier * 0.5
        else:
            regime_mult = regime_multiplier * 0.6

        stop_distance = atr * self.config["atr_multiplier_stop"]
        stop_loss = close - stop_distance

        shares_raw = compute_atr_position_size(
            capital=capital * regime_mult,
            risk_per_trade=self.config["risk_per_trade_pct"],
            entry_price=close,
            atr=atr,
            atr_multiple=self.config["atr_multiplier_stop"],
        )
        max_shares = (capital * self.config["max_position_pct"]) / close
        shares = min(shares_raw, max_shares)
        shares = max(0, int(shares))

        position_value = shares * close
        risk_amount = shares * stop_distance

        return {
            "symbol_id": symbol_id,
            "shares": shares,
            "position_value": round(position_value, 2),
            "risk_amount": round(risk_amount, 2),
            "stop_loss": round(stop_loss, 2),
            "atr": round(atr, 4),
            "close": round(close, 2),
            "regime": regime,
            "regime_multiplier": round(regime_mult, 3),
        }

    def _get_atr(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        period: int = 14,
    ) -> Optional[float]:
        atr_path = os.path.join(
            self.feature_store_dir, "atr", exchange, f"{symbol_id}.parquet"
        )
        if os.path.exists(atr_path):
            df = pd.read_parquet(atr_path)
            col = f"atr_{period}"
            if col in df.columns:
                return float(df[col].dropna().iloc[-1])
            return None

        conn = self._get_conn()
        try:
            result = conn.execute(f"""
                WITH ohlc AS (
                    SELECT
                        symbol_id, exchange, timestamp,
                        high, low, close,
                        LAG(close) OVER w AS prev_close
                    FROM _catalog
                    WHERE symbol_id = '{symbol_id}' AND exchange = '{exchange}'
                    WINDOW w AS (ORDER BY timestamp)
                ),
                tr_calc AS (
                    SELECT
                        GREATEST(
                            high - low,
                            ABS(high - prev_close),
                            ABS(low - prev_close)
                        ) AS tr
                    FROM ohlc
                ),
                atr_calc AS (
                    SELECT AVG(tr) OVER w AS raw_atr
                    FROM tr_calc
                    WINDOW w AS (ORDER BY ROW_NUMBER() OVER ()
                                 ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
                )
                SELECT raw_atr FROM atr_calc QUALIFY ROW_NUMBER() OVER (
                    ORDER BY raw_atr DESC
                ) = 1
            """).fetchone()
            conn.close()
            return float(result[0]) if result and result[0] else None
        except Exception as e:
            logger.warning(f"ATR fetch failed for {symbol_id}: {e}")
            return None

    def build_portfolio(
        self,
        signals: pd.DataFrame,
        capital: float = 1_000_000,
        regime: str = "TREND",
        regime_multiplier: float = 1.0,
    ) -> pd.DataFrame:
        """
        Build a portfolio from ranked signals.

        Args:
            signals: DataFrame with symbol_id, exchange, composite_score
            capital: Total portfolio capital
            regime: Current market regime
            regime_multiplier: Risk multiplier (0-1)

        Returns:
            DataFrame with position details per symbol
        """
        if signals.empty:
            return pd.DataFrame()

        max_pos = min(self.config["max_positions"], len(signals))
        signals = signals.sort_values("composite_score", ascending=False).head(max_pos)

        per_risk = capital * self.config["risk_per_trade_pct"]
        capital_per_pos = capital / max_pos

        positions = []
        for _, row in signals.iterrows():
            pos = self.compute_position_size(
                row["symbol_id"],
                row.get("exchange", "NSE"),
                capital=capital_per_pos,
                regime=regime,
                regime_multiplier=regime_multiplier,
            )
            if pos["shares"] > 0:
                positions.append(pos)

        if not positions:
            return pd.DataFrame()

        df = pd.DataFrame(positions)
        df["weight"] = df["position_value"] / df["position_value"].sum()
        df["risk_pct"] = df["risk_amount"] / capital

        total_risk = df["risk_amount"].sum()
        if total_risk > capital * self.config["max_portfolio_risk_pct"]:
            scale = (capital * self.config["max_portfolio_risk_pct"]) / total_risk
            df["position_value"] *= scale
            df["shares"] = (df["position_value"] / df["close"]).astype(int)
            df["risk_amount"] = df["shares"] * (df["close"] - df["stop_loss"]).abs()
            df["weight"] = df["position_value"] / df["position_value"].sum()
            df["risk_pct"] = df["risk_amount"] / capital

        logger.info(
            f"Portfolio built: {len(df)} positions, "
            f"total risk: {df['risk_amount'].sum():,.0f} "
            f"({df['risk_pct'].sum() * 100:.1f}% of capital)"
        )

        return df

    def compute_portfolio_metrics(
        self,
        positions: pd.DataFrame,
        returns_df: pd.DataFrame,
    ) -> Dict:
        """
        Compute portfolio-level risk metrics.
        """
        if positions.empty or returns_df.empty:
            return {}

        portfolio_returns = returns_df.set_index("timestamp").sort_index()

        cumulative = (1 + portfolio_returns["return"]).cumprod()
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        max_dd = drawdown.min()

        volatility = portfolio_returns["return"].std() * np.sqrt(252)
        sharpe = (
            portfolio_returns["return"].mean() * 252 / volatility
            if volatility > 0
            else 0
        )

        return {
            "max_drawdown": round(max_dd * 100, 2),
            "annualized_volatility": round(volatility * 100, 2),
            "sharpe_ratio": round(sharpe, 3),
            "num_positions": len(positions),
            "total_exposure_pct": round(positions["weight"].sum() * 100, 2),
        }

    def risk_score(self, symbol_id: str, exchange: str = "NSE") -> float:
        """
        Return a risk score (0-100) for a symbol.
        Higher = riskier (more volatile).
        Used to filter high-risk stocks from ranking.
        """
        atr = self._get_atr(symbol_id, exchange)
        conn = self._get_conn()
        try:
            row = conn.execute(
                """
                SELECT close FROM _catalog
                WHERE symbol_id = ? AND exchange = ?
                ORDER BY timestamp DESC LIMIT 1
            """,
                (symbol_id, exchange),
            ).fetchone()
        finally:
            conn.close()

        close = float(row[0]) if row else 0
        if close == 0 or atr is None or atr == 0:
            return 50.0

        cv = atr / close
        risk_score = min(cv * 100, 100)
        return round(risk_score, 2)
