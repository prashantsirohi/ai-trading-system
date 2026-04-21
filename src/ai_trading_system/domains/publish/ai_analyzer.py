import os
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime
from ai_trading_system.platform.utils.env import load_project_env

load_project_env(__file__)

from ai_trading_system.platform.logging.logger import logger


@dataclass
class StockAnalysis:
    symbol: str
    sector: str
    summary: str
    strengths: List[str]
    risks: List[str]
    recommendation: str
    confidence: float
    technical_outlook: str
    catalysts: List[str]
    data_source: str = "AI Analysis"


class AIAnalyzer:
    def __init__(self, model: str = "openrouter/free"):
        self.api_key = os.getenv("OPENROUTER_KEY")
        self.model = model
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        if not self.api_key:
            logger.warning("OPENROUTER_KEY not found in environment")

    def _call_llm(
        self, messages: List[Dict], temperature: float = 0.7
    ) -> Optional[str]:
        if not self.api_key:
            return None

        try:
            import requests

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/anomalyco/opencode",
                "X-Title": "AI Trading System",
            }

            payload = {
                "model": self.model,
                "messages": messages,
                "temperature": temperature,
            }

            response = requests.post(
                self.base_url, headers=headers, json=payload, timeout=60
            )
            response.raise_for_status()

            result = response.json()
            if "choices" not in result or not result["choices"]:
                logger.error(f"Invalid response format: {result}")
                return None
            return result["choices"][0]["message"]["content"]

        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return None

    def analyze_stock(
        self,
        symbol: str,
        sector: str,
        sector_rs: float,
        stock_rs: float,
        vs_sector: float,
        momentum: str,
        price: Optional[float] = None,
        avg_price: Optional[float] = None,
    ) -> StockAnalysis:
        pnl_pct = ((price - avg_price) / avg_price * 100) if price and avg_price else 0

        system_prompt = """You are an expert stock analyst with deep knowledge of Indian markets (NSE).
Analyze the stock based on the provided RS (Relative Strength) metrics and provide actionable insights.
Return JSON with: summary, strengths[], risks[], recommendation, confidence (0-1), technical_outlook, catalysts[]"""

        user_prompt = f"""Analyze {symbol} ({sector} sector):

RS Metrics:
- Sector RS: {sector_rs:.3f} (0-1 scale, higher = sector outperforming)
- Stock RS: {stock_rs:.3f} (0-1 scale, higher = stock outperforming)
- vs Sector: {vs_sector:+.3f} (stock outperformance vs sector)
- Momentum: {momentum}

Position:
- Current Price: {price if price else "N/A"}
- Avg Cost: {avg_price if avg_price else "N/A"}
- P&L: {pnl_pct:+.1f}%

Provide a brief investment thesis (2-3 sentences) and list 3 key strengths, 3 key risks, 3 potential catalysts.
Then give your recommendation (STRONG BUY/BUY/HOLD/REDUCE/SELL) with confidence score and technical outlook (BULLISH/NEUTRAL/BEARISH)."""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        response = self._call_llm(messages)

        if not response:
            return StockAnalysis(
                symbol=symbol,
                sector=sector,
                summary="AI analysis unavailable",
                strengths=[],
                risks=[],
                recommendation="HOLD",
                confidence=0.5,
                technical_outlook="UNKNOWN",
                catalysts=[],
            )

        try:
            response_clean = response.strip()
            if response_clean.startswith("```"):
                response_clean = response_clean.split("```")[1]
                if response_clean.startswith("json"):
                    response_clean = response_clean[4:]
                response_clean = response_clean.strip()

            data = json.loads(response_clean)
            return StockAnalysis(
                symbol=symbol,
                sector=sector,
                summary=data.get("summary", ""),
                strengths=data.get("strengths", []),
                risks=data.get("risks", []),
                recommendation=data.get("recommendation", "HOLD"),
                confidence=float(data.get("confidence", 0.5)),
                technical_outlook=data.get("technical_outlook", "NEUTRAL"),
                catalysts=data.get("catalysts", []),
            )
        except json.JSONDecodeError:
            return StockAnalysis(
                symbol=symbol,
                sector=sector,
                summary=response[:200] if response else "Parse failed",
                strengths=[],
                risks=[],
                recommendation="HOLD",
                confidence=0.5,
                technical_outlook="UNKNOWN",
                catalysts=[],
            )

    def analyze_portfolio(self, portfolio) -> Dict[str, Any]:
        swot_results = portfolio.perform_full_swot_analysis()

        holdings = portfolio.get_holdings_dataframe()
        total_value = portfolio.total_market_value
        total_pnl = portfolio.total_pnl

        sector_allocation = {}
        for _, row in holdings.iterrows():
            sector = row.get("sector", "Other")
            sector_allocation[sector] = sector_allocation.get(sector, 0) + row.get(
                "market_value", 0
            )

        for sector in sector_allocation:
            sector_allocation[sector] = sector_allocation[sector] / total_value * 100

        system_prompt = """You are a senior portfolio analyst specializing in Indian equities.
Analyze the portfolio and provide strategic recommendations.
Return JSON with: portfolio_summary, sector_view, key_themes[], risk_factors[], recommendation, allocation_suggestions[]"""

        holdings_text = "\n".join(
            [
                f"- {row.get('symbol')}: {row.get('sector')} | P&L: {row.get('pnl_pct', 0):+.1f}% | Weight: {row.get('weight', 0):.1f}%"
                for _, row in holdings.iterrows()
            ]
        )

        sector_text = "\n".join(
            [
                f"- {sector}: {weight:.1f}%"
                for sector, weight in sector_allocation.items()
            ]
        )

        swot_text = "\n".join(
            [
                f"- {s.symbol}: {s.overall_signal} (conf {s.confidence:.0%}) | Sector RS: {s.sector_rs:.2f} | Stock RS: {s.stock_rs:.2f}"
                for s in swot_results
            ]
        )

        user_prompt = f"""Portfolio Analysis:

Total Value: ₹{total_value:,.0f}
Total P&L: ₹{total_pnl:,.0f}

Holdings:
{holdings_text}

Sector Allocation:
{sector_text}

SWOT Signals:
{swot_text}

Provide a comprehensive portfolio review with strategic recommendations."""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        response = self._call_llm(messages, temperature=0.5)

        result = {
            "total_value": total_value,
            "total_pnl": total_pnl,
            "positions": len(portfolio.positions),
            "sector_allocation": sector_allocation,
            "swot_summary": {
                "buy_signals": sum(
                    1 for s in swot_results if s.overall_signal in ["BUY", "STRONG BUY"]
                ),
                "sell_signals": sum(
                    1 for s in swot_results if s.overall_signal in ["SELL", "REDUCE"]
                ),
                "hold_signals": sum(
                    1 for s in swot_results if s.overall_signal == "HOLD"
                ),
            },
            "ai_analysis": response if response else "AI analysis unavailable",
        }

        return result


def get_ai_analyzer() -> Optional[AIAnalyzer]:
    api_key = os.getenv("OPENROUTER_KEY")
    if not api_key:
        logger.warning("No OPENROUTER_KEY found")
        return None
    return AIAnalyzer()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("AI Stock Analyzer Test")
    print("=" * 60)

    analyzer = get_ai_analyzer()

    if analyzer:
        print("\nTesting single stock analysis...")

        result = analyzer.analyze_stock(
            symbol="RELIANCE",
            sector="Energy",
            sector_rs=0.602,
            stock_rs=0.389,
            vs_sector=-0.214,
            momentum="Bullish",
            price=3544,
            avg_price=2500,
        )

        print(f"\n{result.symbol} ({result.sector})")
        print(f"  Summary: {result.summary}")
        print(f"  Recommendation: {result.recommendation} ({result.confidence:.0%})")
        print(f"  Technical: {result.technical_outlook}")
        print(f"  Strengths: {result.strengths[:2]}")
        print(f"  Risks: {result.risks[:2]}")
    else:
        print("AI Analyzer not available - check OPENROUTER_KEY")
