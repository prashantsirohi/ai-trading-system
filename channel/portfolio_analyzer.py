import os
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

import pandas as pd
import numpy as np

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)


class PositionType(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    CASH = "CASH"


@dataclass
class Position:
    symbol: str
    quantity: float
    avg_price: float
    current_price: Optional[float] = None
    position_type: PositionType = PositionType.LONG
    sector: Optional[str] = None
    entry_date: Optional[str] = None

    @property
    def market_value(self) -> float:
        if self.current_price:
            return self.quantity * self.current_price
        return 0.0

    @property
    def cost_basis(self) -> float:
        return self.quantity * self.avg_price

    @property
    def pnl(self) -> float:
        return self.market_value - self.cost_basis

    @property
    def pnl_pct(self) -> float:
        if self.cost_basis == 0:
            return 0.0
        return (self.pnl / self.cost_basis) * 100

    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "quantity": self.quantity,
            "avg_price": self.avg_price,
            "current_price": self.current_price,
            "market_value": self.market_value,
            "cost_basis": self.cost_basis,
            "pnl": self.pnl,
            "pnl_pct": self.pnl_pct,
            "position_type": self.position_type.value,
            "sector": self.sector,
            "entry_date": self.entry_date,
        }


@dataclass
class SWOTAnalysis:
    symbol: str
    sector: str

    sector_strength: str = ""
    sector_rs: float = 0.0
    sector_rank: str = ""

    stock_strength: str = ""
    stock_rs: float = 0.0
    stock_rs_rank: float = 0.0

    momentum: str = ""
    momentum_20d: float = 0.0
    momentum_50d: float = 0.0

    volatility: str = ""
    volatility_level: float = 0.0

    vs_sector: float = 0.0

    strength: List[str] = field(default_factory=list)
    weakness: List[str] = field(default_factory=list)
    opportunity: List[str] = field(default_factory=list)
    threat: List[str] = field(default_factory=list)

    overall_signal: str = "HOLD"
    confidence: float = 0.5

    def to_list(self) -> List:
        return [
            self.symbol,
            self.sector,
            self.sector_strength,
            f"{self.sector_rs:.3f}",
            self.stock_strength,
            f"{self.stock_rs:.3f}",
            f"{self.stock_rs_rank:.0%}",
            self.momentum,
            f"{self.momentum_20d:+.1%}",
            f"{self.momentum_50d:+.1%}",
            self.volatility,
            f"{self.volatility_level:.1%}",
            f"{self.vs_sector:+.3f}",
            " | ".join(self.strength[:3]) if self.strength else "",
            " | ".join(self.weakness[:3]) if self.weakness else "",
            " | ".join(self.opportunity[:3]) if self.opportunity else "",
            " | ".join(self.threat[:3]) if self.threat else "",
            self.overall_signal,
            f"{self.confidence:.0%}",
        ]


def load_sector_map() -> Dict[str, str]:
    try:
        import sqlite3
        from pathlib import Path

        base_dir = Path(__file__).parent.parent
        conn = sqlite3.connect(base_dir / "data/masterdata.db")

        INDUSTRY_TO_SECTOR = {
            "Banks": "Banks",
            "Finance": "Finance",
            "Capital Markets": "Finance",
            "IT - Software": "IT",
            "IT - Services": "IT",
            "Pharmaceuticals & Biotechnology": "Pharma",
            "Power": "Power",
            "Ferrous Metals": "Metals",
            "Automobiles": "Automobiles",
            "Auto Components": "Auto Components",
            "Realty": "Realty",
            "Chemicals & Petrochemicals": "Chemicals",
            "Consumer Durables": "Consumer",
            "Retailing": "Consumer",
            "Industrial Products": "Industrial",
            "FMCG": "FMCG",
            "Diversified FMCG": "FMCG",
            "Aerospace & Defense": "Aerospace",
            "Healthcare Services": "Healthcare",
            "Insurance": "Finance",
            "Financial Technology (Fintech)": "Finance",
            "Petroleum Products": "Energy",
            "Gas": "Energy",
            "Oil": "Energy",
            "Non - Ferrous Metals": "Metals",
            "Minerals & Mining": "Mining",
            "Textiles & Apparels": "Consumer",
            "Construction": "Industrial",
            "Cement & Cement Products": "Industrial",
            "Transport Infrastructure": "Infrastructure",
            "Telecom - Services": "Services",
            "Telecom -  Equipment & Accessories": "Services",
            "Commercial Services & Supplies": "Services",
            "Personal Products": "FMCG",
            "Food Products": "FMCG",
            "Beverages": "FMCG",
            "Fertilizers & Agrochemicals": "Chemicals",
            "Electrical Equipment": "Industrial",
            "Industrial Manufacturing": "Industrial",
            "Leisure Services": "Consumer",
            "Diversified": "Diversified",
            "Aerospace & Defence": "Aerospace",
        }

        rows = conn.execute(
            "SELECT Symbol, [Industry Group] FROM stock_details"
        ).fetchall()
        sector_map = {sym: INDUSTRY_TO_SECTOR.get(ind, "Other") for sym, ind in rows}
        conn.close()
        return sector_map
    except Exception as e:
        logger.warning(f"Could not load sector map: {e}")
        return {}


class Portfolio:
    def __init__(self, name: str = "Portfolio", initial_cash: float = 100000.0):
        self.name = name
        self.initial_cash = initial_cash
        self.positions: Dict[str, Position] = {}
        self.cash = initial_cash
        self.trades: List[Dict] = []
        self.returns_history: List[Dict] = []
        self._sector_rs: Optional[pd.DataFrame] = None
        self._stock_rs: Optional[pd.DataFrame] = None
        self._sector_map: Dict[str, str] = {}
        self._base_dir = Path(__file__).parent.parent

    def load_rs_data(self) -> bool:
        try:
            self._sector_rs = pd.read_parquet(
                self._base_dir / "data/feature_store/all_symbols/sector_rs.parquet"
            )
            self._stock_rs = pd.read_parquet(
                self._base_dir
                / "data/feature_store/all_symbols/stock_vs_sector.parquet"
            )
            self._sector_map = load_sector_map()
            logger.info("RS data loaded successfully")
            return True
        except Exception as e:
            logger.warning(f"Could not load RS data: {e}")
            return False

    def add_position(
        self,
        symbol: str,
        quantity: float,
        avg_price: float,
        position_type: PositionType = PositionType.LONG,
        sector: Optional[str] = None,
        entry_date: Optional[str] = None,
    ) -> bool:
        if symbol in self.positions:
            return False

        if not sector and self._sector_map:
            sector = self._sector_map.get(symbol, "Unknown")

        self.positions[symbol] = Position(
            symbol=symbol,
            quantity=quantity,
            avg_price=avg_price,
            position_type=position_type,
            sector=sector,
            entry_date=entry_date or datetime.now().strftime("%Y-%m-%d"),
        )

        self.trades.append(
            {
                "timestamp": datetime.now().isoformat(),
                "action": "BUY",
                "symbol": symbol,
                "quantity": quantity,
                "price": avg_price,
                "value": quantity * avg_price,
            }
        )

        logger.info(f"Added position: {symbol} x {quantity} @ {avg_price}")
        return True

    def update_position_price(self, symbol: str, current_price: float) -> bool:
        if symbol not in self.positions:
            return False
        self.positions[symbol].current_price = current_price
        return True

    def update_prices(self, prices: Dict[str, float]) -> None:
        for symbol, price in prices.items():
            self.update_position_price(symbol, price)

    @property
    def total_market_value(self) -> float:
        return self.cash + sum(p.market_value for p in self.positions.values())

    @property
    def total_cost_basis(self) -> float:
        return sum(p.cost_basis for p in self.positions.values())

    @property
    def total_pnl(self) -> float:
        return sum(p.pnl for p in self.positions.values())

    @property
    def total_pnl_pct(self) -> float:
        if self.total_cost_basis == 0:
            return 0.0
        return (self.total_pnl / self.total_cost_basis) * 100

    def get_holdings_dataframe(self) -> pd.DataFrame:
        data = [p.to_dict() for p in self.positions.values()]
        df = pd.DataFrame(data)

        if not df.empty:
            df["weight"] = df["market_value"] / self.total_market_value * 100
            df = df.sort_values("market_value", ascending=False)

        return df

    def perform_swot_analysis(self, symbol: str) -> SWOTAnalysis:
        swot = SWOTAnalysis(
            symbol=symbol, sector=self.positions[symbol].sector or "Unknown"
        )

        if self._sector_rs is None or self._stock_rs is None:
            return swot

        try:
            latest_sector_rs = self._sector_rs.iloc[-1]
            latest_stock_rs = self._stock_rs.iloc[-1]

            sector = swot.sector

            if sector in latest_sector_rs.index:
                swot.sector_rs = float(latest_sector_rs[sector])
                if swot.sector_rs >= 0.7:
                    swot.sector_strength = "Strong"
                    swot.sector_rank = "Top 30%"
                elif swot.sector_rs >= 0.5:
                    swot.sector_strength = "Neutral"
                    swot.sector_rank = "Middle"
                else:
                    swot.sector_strength = "Weak"
                    swot.sector_rank = "Bottom 50%"

            if symbol in latest_stock_rs.index:
                stock_vs = float(latest_stock_rs[symbol])
                swot.vs_sector = stock_vs
                swot.stock_rs = (
                    swot.sector_rs + stock_vs if swot.sector_rs else stock_vs
                )
                swot.stock_rs_rank = swot.stock_rs

                if swot.stock_rs >= 0.6:
                    swot.stock_strength = "Strong"
                elif swot.stock_rs >= 0.4:
                    swot.stock_strength = "Neutral"
                else:
                    swot.stock_strength = "Weak"

            if sector in self._sector_rs.columns:
                sector_series = self._sector_rs[sector].iloc[-20:]
                swot.momentum_20d = (
                    float(sector_series.sum()) if not sector_series.empty else 0
                )
                sector_50 = self._sector_rs[sector].iloc[-50:]
                swot.momentum_50d = (
                    float(sector_50.mean() * 50) if not sector_50.empty else 0
                )

            if swot.momentum_20d > 0.05:
                swot.momentum = "Bullish"
            elif swot.momentum_20d < -0.05:
                swot.momentum = "Bearish"
            else:
                swot.momentum = "Sideways"

            swot.volatility_level = abs(swot.momentum_20d) * 2

            if swot.volatility_level < 0.05:
                swot.volatility = "Low"
            elif swot.volatility_level < 0.15:
                swot.volatility = "Medium"
            else:
                swot.volatility = "High"

            self._generate_swot_signals(swot)

        except Exception as e:
            logger.warning(f"Error in SWOT analysis for {symbol}: {e}")

        return swot

    def _generate_swot_signals(self, swot: SWOTAnalysis):
        if swot.sector_strength == "Strong":
            swot.strength.append(f"Sector outperforming ({swot.sector_rank})")
            swot.opportunity.append("Sector momentum positive")
        else:
            swot.weakness.append("Sector underperforming")
            swot.threat.append("Sector headwinds")

        if swot.stock_strength == "Strong":
            swot.strength.append(f"Stock RS rank {swot.stock_rs_rank:.0%}")
            swot.opportunity.append("Stock outperforming peers")

        if swot.vs_sector > 0.1:
            swot.strength.append(f"+{swot.vs_sector:.1%} vs sector")
            swot.opportunity.append("Continued outperformance")
        elif swot.vs_sector < -0.1:
            swot.weakness.append(f"{swot.vs_sector:.1%} vs sector")
            swot.threat.append("Sector rotation risk")

        if swot.momentum == "Bullish":
            swot.strength.append("Positive momentum")
            swot.opportunity.append("Trend continuation")
        elif swot.momentum == "Bearish":
            swot.weakness.append("Negative momentum")
            swot.threat.append("Trend reversal risk")

        if swot.volatility == "High":
            swot.threat.append("High volatility")
        elif swot.volatility == "Low":
            swot.strength.append("Stable price action")

        if (
            swot.sector_strength == "Strong"
            and swot.stock_strength == "Strong"
            and swot.vs_sector > 0
        ):
            swot.overall_signal = "STRONG BUY"
            swot.confidence = 0.9
        elif swot.sector_strength == "Strong" and swot.stock_strength == "Strong":
            swot.overall_signal = "BUY"
            swot.confidence = 0.75
        elif swot.sector_strength == "Strong" and swot.vs_sector > 0:
            swot.overall_signal = "BUY"
            swot.confidence = 0.65
        elif swot.stock_strength == "Strong" and swot.momentum == "Bullish":
            swot.overall_signal = "BUY"
            swot.confidence = 0.7
        elif swot.sector_strength == "Weak" and swot.weakness:
            swot.overall_signal = "SELL"
            swot.confidence = 0.7
        elif swot.momentum == "Bearish":
            swot.overall_signal = "REDUCE"
            swot.confidence = 0.6
        else:
            swot.overall_signal = "HOLD"
            swot.confidence = 0.5

    def perform_full_swot_analysis(self) -> List[SWOTAnalysis]:
        results = []
        for symbol in self.positions:
            swot = self.perform_swot_analysis(symbol)
            results.append(swot)
        return results

    def to_google_sheet_data(self) -> List[List]:
        data = [
            [
                "Symbol",
                "Qty",
                "Avg Price",
                "Current",
                "Market Value",
                "Cost",
                "PnL",
                "PnL%",
                "Weight%",
                "Sector",
            ]
        ]

        holdings_df = self.get_holdings_dataframe()
        for _, row in holdings_df.iterrows():
            data.append(
                [
                    row.get("symbol", ""),
                    f"{row.get('quantity', 0):.0f}",
                    f"{row.get('avg_price', 0):.2f}",
                    f"{row.get('current_price', 0):.2f}",
                    f"{row.get('market_value', 0):.2f}",
                    f"{row.get('cost_basis', 0):.2f}",
                    f"{row.get('pnl', 0):.2f}",
                    f"{row.get('pnl_pct', 0):.2f}%",
                    f"{row.get('weight', 0):.2f}%",
                    row.get("sector", ""),
                ]
            )

        data.append([])
        data.append(["SUMMARY"])
        data.append(["Total Value", f"{self.total_market_value:.2f}"])
        data.append(["Cash", f"{self.cash:.2f}"])
        data.append(
            ["Total P&L", f"{self.total_pnl:.2f}", f"({self.total_pnl_pct:.2f}%)"]
        )
        data.append(["Positions", f"{len(self.positions)}"])

        return data

    def to_swot_google_sheet_data(self) -> List[List]:
        headers = [
            "Symbol",
            "Sector",
            "Sector Strength",
            "Sector RS",
            "Stock Strength",
            "Stock RS",
            "RS Rank",
            "Momentum",
            "20d %",
            "50d %",
            "Volatility",
            "Vol Level",
            "vs Sector",
            "Strengths",
            "Weaknesses",
            "Opportunities",
            "Threats",
            "Signal",
            "Confidence",
        ]

        data = [headers]

        swot_results = self.perform_full_swot_analysis()
        swot_results.sort(
            key=lambda x: (
                x.confidence
                if x.overall_signal in ["BUY", "STRONG BUY"]
                else -x.confidence
            ),
            reverse=True,
        )

        for swot in swot_results:
            data.append(swot.to_list())

        return data

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "total_value": self.total_market_value,
            "total_pnl": self.total_pnl,
            "total_pnl_pct": self.total_pnl_pct,
            "positions": len(self.positions),
        }


class PortfolioManager:
    def __init__(self, spreadsheet_id: Optional[str] = None):
        self._base_dir = Path(__file__).parent.parent
        self.spreadsheet_id = spreadsheet_id or os.getenv("GOOGLE_SPREADSHEET_ID")
        self.sheets_client = None
        self._authenticate()

    def _authenticate(self):
        try:
            from google_sheets_manager import GoogleSheetsManager

            manager = GoogleSheetsManager()
            if manager.client:
                manager.open_spreadsheet(self.spreadsheet_id)
                self.sheets_client = manager
                logger.info("Authenticated with Google Sheets")
        except Exception as e:
            logger.warning(f"Could not authenticate: {e}")

    def save_portfolio_to_sheet(
        self, portfolio: Portfolio, sheet_name: str = "PORTFOLIO"
    ) -> bool:
        if not self.sheets_client:
            return False

        try:
            ws = self.sheets_client.get_or_create_sheet(sheet_name, rows=100, cols=12)
            if not ws:
                return False

            ws.clear()
            data = portfolio.to_google_sheet_data()
            ws.update(data, "A1")

            logger.info(f"Saved portfolio to '{sheet_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to save portfolio: {e}")
            return False

    def save_swot_analysis(
        self, portfolio: Portfolio, sheet_name: str = "Portfolio Analysis"
    ) -> bool:
        if not self.sheets_client:
            return False

        try:
            ws = self.sheets_client.get_or_create_sheet(sheet_name, rows=100, cols=20)
            if not ws:
                return False

            ws.clear()
            data = portfolio.to_swot_google_sheet_data()
            ws.update(data, "A1")

            logger.info(f"Saved SWOT analysis to '{sheet_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to save SWOT analysis: {e}")
            return False

    def create_sample_portfolio(self) -> Portfolio:
        portfolio = Portfolio(name="Sample Portfolio", initial_cash=100000)
        portfolio.load_rs_data()

        sample_holdings = [
            {
                "symbol": "RELIANCE",
                "quantity": 50,
                "avg_price": 2500,
                "sector": "Energy",
            },
            {"symbol": "TCS", "quantity": 30, "avg_price": 3500, "sector": "IT"},
            {
                "symbol": "HDFCBANK",
                "quantity": 40,
                "avg_price": 1600,
                "sector": "Banks",
            },
            {"symbol": "INFY", "quantity": 25, "avg_price": 1500, "sector": "IT"},
            {"symbol": "SBIN", "quantity": 100, "avg_price": 600, "sector": "Banks"},
        ]

        for h in sample_holdings:
            portfolio.add_position(**h)

        sample_prices = {
            "RELIANCE": 3544,
            "TCS": 3355,
            "HDFCBANK": 1700,
            "INFY": 1550,
            "SBIN": 650,
        }
        portfolio.update_prices(sample_prices)

        return portfolio


def load_portfolio_manager(spreadsheet_id: Optional[str] = None) -> PortfolioManager:
    return PortfolioManager(spreadsheet_id)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("Portfolio SWOT Analysis")
    print("=" * 60)

    pm = load_portfolio_manager()

    if pm.sheets_client:
        print("\nCreating sample portfolio...")
        portfolio = pm.create_sample_portfolio()

        print("\nHoldings:")
        df = portfolio.get_holdings_dataframe()
        print(
            df[["symbol", "sector", "market_value", "pnl_pct"]].to_string(index=False)
        )

        print("\nPerforming SWOT analysis...")
        swot_results = portfolio.perform_full_swot_analysis()

        print("\n" + "=" * 60)
        print("SWOT ANALYSIS RESULTS")
        print("=" * 60)

        for swot in swot_results:
            print(f"\n{swot.symbol} ({swot.sector})")
            print(f"  Signal: {swot.overall_signal} ({swot.confidence:.0%})")
            print(f"  Sector: {swot.sector_strength} (RS={swot.sector_rs:.3f})")
            print(
                f"  Stock: {swot.stock_strength} (RS={swot.stock_rs:.3f}, vs sector={swot.vs_sector:+.3f})"
            )
            print(f"  Momentum: {swot.momentum} ({swot.momentum_20d:+.1%} 20d)")

        print("\nSaving to Google Sheets...")
        if pm.save_portfolio_to_sheet(portfolio, "PORTFOLIO"):
            print("Portfolio saved!")

        if pm.save_swot_analysis(portfolio, "Portfolio Analysis"):
            print("SWOT Analysis saved!")
    else:
        print("Not connected to Google Sheets")
