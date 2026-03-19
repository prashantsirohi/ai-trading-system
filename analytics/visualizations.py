import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Dict, List

try:
    import quantstats as qs

    HAS_QUANTSTATS = True
except ImportError:
    HAS_QUANTSTATS = False

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import plotly.express as px

    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Visualizer:
    """
    Visualization & Reporting Engine.

    Capabilities:
      - QuantStats Tear Sheets: Returns, drawdowns, rolling Sharpe
      - Interactive Plotly charts: OHLCV + indicators + signals
      - Portfolio equity curves with drawdown overlay
      - Signal distribution heatmaps
      - Top signals ranking table
    """

    def __init__(
        self,
        ohlcv_db_path: str = None,
        feature_store_dir: str = None,
        output_dir: str = None,
    ):
        if ohlcv_db_path is None:
            ohlcv_db_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data",
                "ohlcv.duckdb",
            )
        if feature_store_dir is None:
            feature_store_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data",
                "feature_store",
            )
        if output_dir is None:
            output_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "reports",
            )
        self.ohlcv_db_path = ohlcv_db_path
        self.feature_store_dir = feature_store_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def _get_ohlcv(self, symbol_id: str, exchange: str = "NSE") -> pd.DataFrame:
        import duckdb

        conn = duckdb.connect(self.ohlcv_db_path)
        try:
            df = conn.execute(
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
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    def _get_features(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        feature_names: List[str] = None,
    ) -> pd.DataFrame:
        if feature_names is None:
            feature_names = ["rsi", "adx", "macd", "atr", "bb", "supertrend"]

        merged = None
        for feat in feature_names:
            path = os.path.join(
                self.feature_store_dir, feat, exchange, f"{symbol_id}.parquet"
            )
            if not os.path.exists(path):
                continue
            fdf = pd.read_parquet(path)
            fdf["timestamp"] = pd.to_datetime(fdf["timestamp"])
            cols = ["timestamp"] + [
                c
                for c in fdf.columns
                if c
                not in (
                    "symbol_id",
                    "exchange",
                    "timestamp",
                    "close",
                    "open",
                    "high",
                    "low",
                    "volume",
                )
            ]
            if merged is None:
                merged = fdf[cols]
            else:
                merged = merged.merge(fdf[cols], on="timestamp", how="left")
        return merged if merged is not None else pd.DataFrame()

    def plot_technical_chart(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        from_date: str = None,
        to_date: str = None,
        show_supertrend: bool = True,
        show_bollinger: bool = True,
        show_macd: bool = True,
        show_rsi: bool = True,
        output_path: str = None,
    ) -> go.Figure:
        """
        Interactive OHLCV chart with overlaid technical indicators.
        """
        if not HAS_PLOTLY:
            logger.warning("Plotly not available")
            return None

        ohlcv = self._get_ohlcv(symbol_id, exchange)
        if ohlcv.empty:
            logger.warning(f"No OHLCV data for {symbol_id}")
            return None

        if from_date:
            ohlcv = ohlcv[ohlcv["timestamp"] >= from_date]
        if to_date:
            ohlcv = ohlcv[ohlcv["timestamp"] <= to_date]

        features = self._get_features(symbol_id, exchange)
        if not features.empty:
            ohlcv = ohlcv.merge(features, on="timestamp", how="left")

        ohlcv = ohlcv.sort_values("timestamp")

        n_rows = 1 + (1 if show_rsi else 0) + (1 if show_macd else 0)
        row_heights = [0.6] + [0.2] * (n_rows - 1)

        fig = make_subplots(
            rows=n_rows,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=row_heights,
            subplot_titles=(
                [f"{symbol_id} ({exchange})"]
                + (["RSI"] if show_rsi else [])
                + (["MACD"] if show_macd else [])
            ),
        )

        fig.add_trace(
            go.Candlestick(
                x=ohlcv["timestamp"],
                open=ohlcv["open"],
                high=ohlcv["high"],
                low=ohlcv["low"],
                close=ohlcv["close"],
                name="OHLCV",
                increasing_line_color="#26a69a",
                decreasing_line_color="#ef5350",
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Scatter(
                x=ohlcv["timestamp"],
                y=ohlcv["volume"],
                name="Volume",
                marker_color="rgba(100,100,200,0.3)",
                yaxis="y2",
            ),
            row=1,
            col=1,
        )

        if show_bollinger and "bb_middle_20" in ohlcv.columns:
            fig.add_trace(
                go.Scatter(
                    x=ohlcv["timestamp"],
                    y=ohlcv["bb_middle_20"],
                    name="BB Middle",
                    line=dict(color="#9b59b6", width=1),
                    legendgroup="bb",
                ),
                row=1,
                col=1,
            )
            if "bb_upper_20_2sd" in ohlcv.columns:
                fig.add_trace(
                    go.Scatter(
                        x=ohlcv["timestamp"],
                        y=ohlcv["bb_upper_20_2sd"],
                        name="BB Upper",
                        line=dict(color="#9b59b6", width=1, dash="dash"),
                        legendgroup="bb",
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )
            if "bb_lower_20_2sd" in ohlcv.columns:
                fig.add_trace(
                    go.Scatter(
                        x=ohlcv["timestamp"],
                        y=ohlcv["bb_lower_20_2sd"],
                        name="BB Lower",
                        line=dict(color="#9b59b6", width=1, dash="dash"),
                        legendgroup="bb",
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )

        if show_supertrend:
            st_col = "supertrend_10_3"
            if st_col in ohlcv.columns:
                bull = ohlcv[ohlcv["supertrend_dir_10_3"] == 1]
                bear = ohlcv[ohlcv["supertrend_dir_10_3"] == -1]
                fig.add_trace(
                    go.Scatter(
                        x=bull["timestamp"],
                        y=bull[st_col],
                        name="ST Bullish",
                        line=dict(color="#00b300", width=2),
                        legendgroup="st",
                    ),
                    row=1,
                    col=1,
                )
                fig.add_trace(
                    go.Scatter(
                        x=bear["timestamp"],
                        y=bear[st_col],
                        name="ST Bearish",
                        line=dict(color="#ff0000", width=2),
                        legendgroup="st",
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )

        row = 2
        if show_rsi and "rsi_14" in ohlcv.columns:
            fig.add_trace(
                go.Scatter(
                    x=ohlcv["timestamp"],
                    y=ohlcv["rsi_14"],
                    name="RSI 14",
                    line=dict(color="#ff9800", width=1.5),
                    fill="tozeroy",
                    fillcolor="rgba(255,152,0,0.1)",
                ),
                row=row,
                col=1,
            )
            fig.add_hline(
                y=70,
                line_dash="dash",
                line_color="red",
                annotation_text="Overbought",
                row=row,
                col=1,
            )
            fig.add_hline(
                y=30,
                line_dash="dash",
                line_color="green",
                annotation_text="Oversold",
                row=row,
                col=1,
            )
            row += 1

        if show_macd and "macd_line" in ohlcv.columns:
            fig.add_trace(
                go.Scatter(
                    x=ohlcv["timestamp"],
                    y=ohlcv["macd_line"],
                    name="MACD",
                    line=dict(color="#2196F3", width=1.5),
                ),
                row=row,
                col=1,
            )
            if "macd_signal_9" in ohlcv.columns:
                fig.add_trace(
                    go.Scatter(
                        x=ohlcv["timestamp"],
                        y=ohlcv["macd_signal_9"],
                        name="Signal",
                        line=dict(color="#FF5722", width=1),
                    ),
                    row=row,
                    col=1,
                )
            if "macd_histogram" in ohlcv.columns:
                colors = [
                    "#26a69a" if v >= 0 else "#ef5350" for v in ohlcv["macd_histogram"]
                ]
                fig.add_trace(
                    go.Bar(
                        x=ohlcv["timestamp"],
                        y=ohlcv["macd_histogram"],
                        name="Histogram",
                        marker_color=colors,
                        opacity=0.5,
                    ),
                    row=row,
                    col=1,
                )

        fig.update_layout(
            title=dict(text=f"{symbol_id} — Technical Analysis", font_size=20),
            template="plotly_dark",
            height=300 + 300 * (n_rows - 1),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            xaxis_rangeslider_visible=False,
        )
        fig.update_xaxes(title_text="Date", row=n_rows, col=1)
        fig.update_yaxes(title_text="Price", row=1, col=1)

        if output_path:
            fig.write_html(output_path)
            logger.info(f"Chart saved to {output_path}")

        return fig

    def plot_equity_curve(
        self,
        equity_curve: pd.DataFrame,
        title: str = "Portfolio Equity Curve",
        output_path: str = None,
    ) -> go.Figure:
        """
        Equity curve with drawdown overlay.
        """
        if not HAS_PLOTLY:
            return None

        equity = equity_curve.sort_values("date").copy()
        equity["date"] = pd.to_datetime(equity["date"])

        cum = equity["capital"] / equity["capital"].iloc[0]
        running_max = cum.cummax()
        drawdown = (cum - running_max) / running_max * 100

        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            row_heights=[0.7, 0.3],
            subplot_titles=["Equity Curve", "Drawdown (%)"],
        )

        fig.add_trace(
            go.Scatter(
                x=equity["date"],
                y=round(cum, 4),
                name="Portfolio",
                line=dict(color="#26a69a", width=2),
                fill="tozeroy",
                fillcolor="rgba(38,166,154,0.2)",
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Scatter(
                x=equity["date"],
                y=round(drawdown, 2),
                name="Drawdown",
                line=dict(color="#ef5350", width=1),
                fill="tozeroy",
                fillcolor="rgba(239,83,80,0.3)",
            ),
            row=2,
            col=1,
        )

        fig.update_layout(
            title=dict(text=title),
            template="plotly_dark",
            height=500,
            showlegend=True,
        )

        if output_path:
            fig.write_html(output_path)
            logger.info(f"Equity chart saved to {output_path}")

        return fig

    def quantstats_tear_sheet(
        self,
        equity_curve: pd.DataFrame,
        benchmark: pd.Series = None,
        output_path: str = None,
    ) -> Optional[str]:
        """
        Generate QuantStats HTML tear sheet.
        """
        if not HAS_QUANTSTATS:
            logger.warning("QuantStats not available")
            return None

        equity = equity_curve.sort_values("date").copy()
        equity["date"] = pd.to_datetime(equity["date"])
        equity = equity.set_index("date")

        returns = equity["capital"].pct_change().dropna()

        if output_path is None:
            output_path = os.path.join(self.output_dir, "tear_sheet.html")

        qs.reports.html(
            returns,
            benchmark=benchmark,
            title="AI Trading System — Strategy Report",
            output=output_path,
        )

        logger.info(f"QuantStats tear sheet saved to {output_path}")
        return output_path

    def plot_signals(
        self,
        signals_df: pd.DataFrame,
        output_path: str = None,
    ) -> go.Figure:
        """
        Bar chart of top signals by composite score.
        """
        if not HAS_PLOTLY:
            return None

        df = signals_df.head(20).sort_values("composite_score")
        fig = px.bar(
            df,
            x="composite_score",
            y="symbol_id",
            orientation="h",
            color="composite_score",
            color_continuous_scale="Viridis",
            title="Top 20 Signals by Composite Score",
            labels={
                "symbol_id": "Symbol",
                "composite_score": "Composite Score",
                "rel_strength_score": "Rel Strength",
                "vol_intensity_score": "Vol Intensity",
            },
        )

        fig.update_layout(
            template="plotly_dark",
            height=600,
            showlegend=False,
        )

        if output_path:
            fig.write_html(output_path)
            logger.info(f"Signals chart saved to {output_path}")

        return fig

    def plot_factor_breakdown(
        self,
        signal: Dict,
        output_path: str = None,
    ) -> go.Figure:
        """
        Radar/spider chart of factor scores for a single signal.
        """
        if not HAS_PLOTLY:
            return None

        categories = [
            "Relative Strength",
            "Volume Intensity",
            "Trend Persistence",
            "Proximity Highs",
        ]
        values = [
            signal.get("rel_strength_score", 0),
            signal.get("vol_intensity_score", 0),
            signal.get("trend_persistence_score", 0),
            signal.get("proximity_highs_score", 0),
        ]

        fig = go.Figure()

        fig.add_trace(
            go.Scatterpolar(
                r=values + [values[0]],
                theta=categories + [categories[0]],
                fill="toself",
                fillcolor="rgba(38,166,154,0.3)",
                line_color="#26a69a",
                name=signal.get("symbol_id", "Stock"),
            )
        )

        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            title=dict(
                text=f"Factor Profile: {signal.get('symbol_id', '')} "
                f"(Score: {signal.get('composite_score', 0):.1f})"
            ),
            template="plotly_dark",
            height=500,
        )

        if output_path:
            fig.write_html(output_path)
            logger.info(f"Factor chart saved to {output_path}")

        return fig

    def generate_daily_report(
        self,
        ranked_signals: pd.DataFrame,
        equity_curve: pd.DataFrame,
        regime: str,
        output_dir: str = None,
    ) -> Dict[str, str]:
        """
        Generate complete daily report with all visualizations.
        """
        if output_dir is None:
            ts = datetime.now().strftime("%Y%m%d")
            output_dir = os.path.join(self.output_dir, f"report_{ts}")
        os.makedirs(output_dir, exist_ok=True)

        paths = {}

        top_sym = (
            ranked_signals.iloc[0]["symbol_id"] if not ranked_signals.empty else None
        )
        if top_sym:
            chart_path = os.path.join(output_dir, f"{top_sym}_chart.html")
            self.plot_technical_chart(top_sym, output_path=chart_path)
            paths["top_chart"] = chart_path

        signals_path = os.path.join(output_dir, "top_signals.html")
        self.plot_signals(ranked_signals, output_path=signals_path)
        paths["signals"] = signals_path

        if not equity_curve.empty:
            equity_path = os.path.join(output_dir, "equity_curve.html")
            self.plot_equity_curve(equity_curve, output_path=equity_path)
            paths["equity"] = equity_path

            tear_path = os.path.join(output_dir, "tear_sheet.html")
            self.quantstats_tear_sheet(equity_curve, output_path=tear_path)
            paths["tear_sheet"] = tear_path

        summary_path = os.path.join(output_dir, "summary.txt")
        with open(summary_path, "w") as f:
            f.write(f"AI Trading System — Daily Report\n")
            f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
            f.write(f"Market Regime: {regime}\n")
            f.write(f"Top Signals: {len(ranked_signals)}\n")
            if not ranked_signals.empty:
                f.write(f"\nTop 10:\n")
                f.write(
                    ranked_signals.head(10)[
                        [
                            "symbol_id",
                            "composite_score",
                            "rel_strength_score",
                            "vol_intensity_score",
                        ]
                    ].to_string(index=False)
                )
        paths["summary"] = summary_path

        logger.info(f"Daily report generated: {output_dir}")
        return paths
