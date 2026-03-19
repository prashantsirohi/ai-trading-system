import os
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RiskManager:
    """
    Risk Management - Controls exposure and position sizing.
    """

    def __init__(
        self,
        max_risk_per_trade: float = 0.01,
        max_portfolio_exposure: float = 0.20,
        max_drawdown: float = 0.10,
        max_positions: int = 10
    ):
        self.max_risk_per_trade = max_risk_per_trade
        self.max_portfolio_exposure = max_portfolio_exposure
        self.max_drawdown = max_drawdown
        self.max_positions = max_positions
        self.portfolio_value = 100000
        self.current_positions = {}
        self.daily_pnl = 0
        self.peak_portfolio_value = 100000
        self.total_trades = 0
        self.winning_trades = 0

    def calculate_position_size(
        self,
        symbol: str,
        entry_price: float,
        stop_loss: float,
        risk_amount: Optional[float] = None
    ) -> int:
        """
        Calculate position size based on risk parameters.
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            stop_loss: Stop loss price
            risk_amount: Optional custom risk amount
            
        Returns:
            Number of shares to buy
        """
        if entry_price <= 0 or stop_loss <= 0:
            return 0

        risk_per_share = abs(entry_price - stop_loss)
        if risk_per_share == 0:
            return 0

        if risk_amount is None:
            risk_amount = self.portfolio_value * self.max_risk_per_trade

        position_size = int(risk_amount / risk_per_share)

        max_shares_by_exposure = int(
            (self.portfolio_value * self.max_portfolio_exposure) / entry_price
        )

        position_size = min(position_size, max_shares_by_exposure)

        return max(0, position_size)

    def check_risk_limits(
        self,
        symbol: str,
        position_size: int,
        entry_price: float,
        stop_loss: float
    ) -> Dict:
        """
        Check if trade passes risk management rules.
        
        Returns:
            Dict with 'approved' (bool) and 'reason' (str)
        """
        if self.get_current_drawdown() > self.max_drawdown:
            return {
                "approved": False,
                "reason": f"Max drawdown limit exceeded: {self.get_current_drawdown():.2%}"
            }

        portfolio_exposure = self.get_portfolio_exposure()
        trade_value = position_size * entry_price
        new_exposure = portfolio_exposure + trade_value

        if new_exposure > self.portfolio_value * self.max_portfolio_exposure:
            return {
                "approved": False,
                "reason": f"Max portfolio exposure exceeded: {new_exposure/self.portfolio_value:.2%}"
            }

        if len(self.current_positions) >= self.max_positions:
            return {
                "approved": False,
                "reason": f"Max positions limit reached: {len(self.current_positions)}"
            }

        risk_per_trade = abs(entry_price - stop_loss) / entry_price
        if risk_per_trade > self.max_risk_per_trade * 2:
            return {
                "approved": False,
                "reason": f"Risk per trade too high: {risk_per_trade:.2%}"
            }

        return {"approved": True, "reason": "All checks passed"}

    def get_portfolio_exposure(self) -> float:
        """Calculate total portfolio exposure"""
        total = 0
        for symbol, position in self.current_positions.items():
            total += position.get("value", 0)
        return total

    def get_current_drawdown(self) -> float:
        """Calculate current drawdown percentage"""
        if self.peak_portfolio_value == 0:
            return 0
        return (self.peak_portfolio_value - self.portfolio_value) / self.peak_portfolio_value

    def update_position(
        self,
        symbol: str,
        action: str,
        quantity: int,
        price: float
    ):
        """
        Update position after trade execution.
        
        Args:
            action: 'buy' or 'sell'
            quantity: Number of shares
            price: Execution price
        """
        if action == "buy":
            cost = quantity * price
            if symbol in self.current_positions:
                existing = self.current_positions[symbol]
                new_qty = existing["quantity"] + quantity
                avg_price = (
                    (existing["quantity"] * existing["avg_price"] + cost) / new_qty
                )
                self.current_positions[symbol] = {
                    "quantity": new_qty,
                    "avg_price": avg_price,
                    "value": new_qty * price,
                    "entry_time": existing.get("entry_time", datetime.now())
                }
            else:
                self.current_positions[symbol] = {
                    "quantity": quantity,
                    "avg_price": price,
                    "value": quantity * price,
                    "entry_time": datetime.now()
                }

        elif action == "sell" or action == "close":
            if symbol in self.current_positions:
                position = self.current_positions[symbol]
                if quantity >= position["quantity"]:
                    self.current_positions.pop(symbol)
                else:
                    remaining = position["quantity"] - quantity
                    self.current_positions[symbol] = {
                        "quantity": remaining,
                        "avg_price": position["avg_price"],
                        "value": remaining * price,
                        "entry_time": position.get("entry_time", datetime.now())
                    }

        self._update_portfolio_value()

    def _update_portfolio_value(self):
        """Update total portfolio value"""
        total_value = sum(pos.get("value", 0) for pos in self.current_positions.values())
        total_value += self.portfolio_value * 0.3
        self.portfolio_value = total_value

        if total_value > self.peak_portfolio_value:
            self.peak_portfolio_value = total_value

    def record_trade_result(self, pnl: float):
        """Record trade P&L for statistics"""
        self.daily_pnl += pnl
        self.total_trades += 1
        if pnl > 0:
            self.winning_trades += 1

    def get_win_rate(self) -> float:
        """Calculate win rate"""
        if self.total_trades == 0:
            return 0
        return self.winning_trades / self.total_trades

    def get_risk_metrics(self) -> Dict:
        """Get current risk metrics"""
        return {
            "portfolio_value": self.portfolio_value,
            "peak_value": self.peak_portfolio_value,
            "current_drawdown": self.get_current_drawdown(),
            "portfolio_exposure": self.get_portfolio_exposure(),
            "max_exposure": self.portfolio_value * self.max_portfolio_exposure,
            "positions_count": len(self.current_positions),
            "max_positions": self.max_positions,
            "win_rate": self.get_win_rate(),
            "daily_pnl": self.daily_pnl
        }

    def calculate_kelly_criterion(self, win_rate: float, avg_win: float, avg_loss: float) -> float:
        """Calculate Kelly Criterion for position sizing"""
        if avg_loss == 0:
            return 0
        win_loss_ratio = avg_win / avg_loss
        kelly = (win_rate * win_loss_ratio - (1 - win_rate)) / win_loss_ratio
        return max(0, min(kelly, 0.25))

    def apply_kelly_sizing(
        self,
        win_rate: float,
        avg_win: float,
        avg_loss: float,
        base_size: int
    ) -> int:
        """Apply Kelly Criterion for position sizing"""
        kelly = self.calculate_kelly_criterion(win_rate, avg_win, avg_loss)
        return int(base_size * kelly * 2)
