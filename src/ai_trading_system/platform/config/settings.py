import os
from typing import Dict
from dataclasses import dataclass, field

from ai_trading_system.platform.utils.env import load_project_env

load_project_env(__file__)


@dataclass
class DataConfig:
    """Data collection and storage configuration"""
    data_dir: str = "ai-trading-system/data"
    raw_dir: str = "ai-trading-system/data/raw/NSE_EQ"
    features_dir: str = "ai-trading-system/data/features"
    signals_dir: str = "ai-trading-system/data/signals"
    backtests_dir: str = "ai-trading-system/data/backtests"
    parquet_compression: str = "snappy"


@dataclass
class DhanConfig:
    """DhanHQ API configuration"""
    api_key: str = os.getenv("DHAN_API_KEY", "")
    client_id: str = os.getenv("DHAN_CLIENT_ID", "")
    access_token: str = os.getenv("DHAN_ACCESS_TOKEN", "")
    rate_limit_per_second: int = 5
    rate_limit_bulk: int = 1000
    daily_limit: int = 1000


@dataclass
class CollectorConfig:
    """Data collector configuration"""
    enabled_providers: list = field(default_factory=lambda: ["dhan", "nse"])
    batch_size: int = 50
    retry_attempts: int = 3
    retry_delay: int = 5
    fetch_interval_minutes: int = 5


@dataclass
class FeatureConfig:
    """Feature engineering configuration"""
    indicators: list = field(default_factory=lambda: [
        "RSI", "MACD", "Supertrend", "ATR", "EMA", "VWAP"
    ])
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    supertrend_period: int = 10
    supertrend_multiplier: float = 3.0
    ema_periods: list = field(default_factory=lambda: [20, 50, 200])
    atr_period: int = 14


@dataclass
class SignalConfig:
    """Signal detection configuration"""
    min_signal_strength: int = 1
    rsi_oversold: float = 30
    rsi_overbought: float = 70
    volume_threshold: float = 2.0
    trend_threshold: float = 2.0


@dataclass
class BacktestConfig:
    """Backtesting configuration"""
    initial_capital: float = 100000
    commission: float = 0.1
    slippage: float = 0.05
    train_window: int = 252
    test_window: int = 63


@dataclass
class RiskConfig:
    """Risk management configuration"""
    max_risk_per_trade: float = 0.01
    max_portfolio_exposure: float = 0.20
    max_drawdown: float = 0.10
    max_positions: int = 10
    max_loss_per_day: float = 0.02


@dataclass
class AIConfig:
    """AI ranking configuration"""
    model_type: str = "random_forest"
    model_path: str = "ai-trading-system/models"
    min_prediction_score: float = 0.5
    top_n_signals: int = 10
    retrain_frequency_days: int = 30


@dataclass
class ExecutionConfig:
    """Execution engine configuration"""
    broker: str = "dhan"
    order_type: str = "MARKET"
    product_type: str = "INTRADAY"
    validity: str = "DAY"
    max_retries: int = 3


@dataclass
class AppConfig:
    """Main application configuration"""
    debug: bool = False
    log_level: str = "INFO"

    data: DataConfig = field(default_factory=DataConfig)
    dhan: DhanConfig = field(default_factory=DhanConfig)
    collector: CollectorConfig = field(default_factory=CollectorConfig)
    features: FeatureConfig = field(default_factory=FeatureConfig)
    signals: SignalConfig = field(default_factory=SignalConfig)
    backtest: BacktestConfig = field(default_factory=BacktestConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    ai: AIConfig = field(default_factory=AIConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Load configuration from environment variables"""
        return cls(
            dhan=DhanConfig(
                api_key=os.getenv("DHAN_API_KEY", ""),
                client_id=os.getenv("DHAN_CLIENT_ID", ""),
                access_token=os.getenv("DHAN_ACCESS_TOKEN", "")
            )
        )


CONFIG = AppConfig()
