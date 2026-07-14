"""One immutable, cutoff-aware input surface for a ranking decision."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd

from ai_trading_system.domains.ranking.input_loader import RankerInputLoader
from ai_trading_system.domains.ranking.stage_store import read_latest_snapshot


@dataclass(frozen=True)
class RankInputSnapshot:
    """Cache every dated ranking input under one inclusive as-of boundary."""

    loader: RankerInputLoader
    as_of: str
    exchanges: tuple[str, ...]
    _cache: dict[tuple[Any, ...], Any] = field(default_factory=dict, init=False, repr=False)

    def _cached(self, key: tuple[Any, ...], load: Callable[[], Any]) -> Any:
        if key not in self._cache:
            self._cache[key] = load()
        value = self._cache[key]
        if isinstance(value, pd.DataFrame):
            return value.copy()
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, tuple):
            return tuple(item.copy() if hasattr(item, "copy") else item for item in value)
        return value

    def market(self) -> pd.DataFrame:
        return self._cached(
            ("market",),
            lambda: self.loader.load_latest_market_data(
                as_of=self.as_of,
                exchanges=list(self.exchanges),
            ),
        )

    def returns(self, periods: list[int]) -> pd.DataFrame:
        normalized = tuple(int(period) for period in periods)
        return self._cached(
            ("returns", normalized),
            lambda: self.loader.load_return_frame_multi(
                as_of=self.as_of,
                periods=list(normalized),
                exchanges=list(self.exchanges),
            ),
        )

    def volume(self) -> pd.DataFrame:
        return self._cached(
            ("volume",),
            lambda: self.loader.load_volume_frame(
                as_of=self.as_of,
                exchanges=list(self.exchanges),
            ),
        )

    def benchmark_returns(self, symbol: str, periods: list[int]) -> dict[int, float]:
        normalized_periods = tuple(int(period) for period in periods)

        def load() -> dict[int, float]:
            conn = self.loader.get_conn()
            try:
                history = conn.execute(
                    """
                    SELECT timestamp, close
                    FROM _catalog
                    WHERE symbol_id = ?
                      AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    [symbol, self.as_of, max(normalized_periods, default=0) + 1],
                ).fetchdf()
            finally:
                conn.close()
            if history.empty:
                return {}
            history = history.sort_values("timestamp").reset_index(drop=True)
            latest = float(history["close"].iloc[-1])
            return {
                period: (latest - float(history["close"].iloc[-period - 1]))
                / float(history["close"].iloc[-period - 1])
                * 100.0
                for period in normalized_periods
                if len(history) > period
                and float(history["close"].iloc[-period - 1]) > 0
            }

        return self._cached(("benchmark", str(symbol), normalized_periods), load)

    def adx(self) -> pd.DataFrame:
        return self._cached(("adx",), lambda: self.loader.load_latest_adx(date=self.as_of))

    def sma(self) -> pd.DataFrame:
        return self._cached(("sma",), lambda: self.loader.load_latest_sma(date=self.as_of))

    def highs(self, window: int) -> pd.DataFrame:
        normalized = int(window)
        return self._cached(
            ("highs", normalized),
            lambda: self.loader.load_latest_highs(date=self.as_of, window=normalized),
        )

    def delivery(self) -> pd.DataFrame:
        return self._cached(
            ("delivery",),
            lambda: self.loader.load_latest_delivery(date=self.as_of),
        )

    def stage2(self, rel_strength_frame: pd.DataFrame) -> pd.DataFrame:
        return self.loader.load_latest_stage2(
            date=self.as_of,
            exchanges=list(self.exchanges),
            rel_strength_frame=rel_strength_frame,
        )

    def phase1_symbol_features(self, exchange: str) -> pd.DataFrame:
        normalized = str(exchange).strip().upper()
        return self._cached(
            ("phase1", normalized),
            lambda: self.loader.load_latest_phase1_symbol_features(
                date=self.as_of,
                exchange=normalized,
            ),
        )

    def sector_inputs(self) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
        def load() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
            sector_rs, stock_vs_sector, sector_map = self.loader.load_sector_inputs()
            cutoff = pd.Timestamp(self.as_of).normalize()
            if isinstance(sector_rs.index, pd.DatetimeIndex):
                sector_rs = sector_rs.loc[sector_rs.index <= cutoff]
            if isinstance(stock_vs_sector.index, pd.DatetimeIndex):
                stock_vs_sector = stock_vs_sector.loc[stock_vs_sector.index <= cutoff]
            return sector_rs, stock_vs_sector, dict(sector_map)

        return self._cached(("sector",), load)

    def weekly_stage(self, db_path: str) -> pd.DataFrame:
        return self._cached(
            ("weekly_stage", db_path),
            lambda: read_latest_snapshot(db_path, asof=self.as_of),
        )
