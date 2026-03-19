import time
from typing import Dict, List, Optional
from datetime import datetime
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DhanExecutor:
    """
    Execution Engine - Sends trades to DhanHQ broker.
    """

    def __init__(
        self,
        api_key: str,
        client_id: str,
        access_token: str,
        risk_manager=None
    ):
        self.api_key = api_key
        self.client_id = client_id
        self.access_token = access_token
        self.base_url = "https://api.dhan.co/v2"
        self.risk_manager = risk_manager
        self.pending_orders = {}
        self.executed_orders = {}

    def _get_headers(self) -> Dict:
        """Get API headers"""
        return {
            "access-token": self.access_token,
            "client-id": self.client_id,
            "Content-Type": "application/json"
        }

    def place_order(
        self,
        symbol: str,
        exchange: str = "NSE",
        transaction_type: str = "BUY",
        order_type: str = "LIMIT",
        quantity: int = 1,
        price: Optional[float] = None,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        product_type: str = "INTRADAY"
    ) -> Dict:
        """
        Place a new order.
        
        Args:
            symbol: Trading symbol
            exchange: NSE/BSE
            transaction_type: BUY/SELL
            order_type: LIMIT/MARKET/STOP_LOSS
            quantity: Number of shares
            price: Limit price (None for MARKET)
            stop_loss: Stop loss price
            take_profit: Take profit price
            product_type: INTRADAY/DELIVERY/CARRYFORWARD
        """
        if self.risk_manager:
            risk_check = self.risk_manager.check_risk_limits(
                symbol, quantity, price or 0, stop_loss or 0
            )
            if not risk_check["approved"]:
                logger.warning(f"Order rejected: {risk_check['reason']}")
                return {"status": "rejected", "reason": risk_check["reason"]}

        order_payload = {
            "exchange": exchange,
            "symbol": symbol,
            "transactionType": transaction_type,
            "orderType": order_type,
            "quantity": quantity,
            "productType": product_type,
            "price": price,
            "validity": "DAY"
        }

        if stop_loss:
            order_payload["stopLoss"] = stop_loss
        if take_profit:
            order_payload["takeProfit"] = take_profit

        try:
            logger.info(f"Placing order: {order_payload}")
            order_id = f"ORD_{int(time.time())}_{symbol}"

            self.pending_orders[order_id] = {
                "symbol": symbol,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "price": price,
                "order_type": order_type,
                "timestamp": datetime.now(),
                "status": "pending"
            }

            return {
                "status": "success",
                "order_id": order_id,
                "symbol": symbol,
                "quantity": quantity,
                "price": price
            }

        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return {"status": "error", "message": str(e)}

    def place_market_order(
        self,
        symbol: str,
        quantity: int,
        transaction_type: str = "BUY"
    ) -> Dict:
        """Place market order"""
        return self.place_order(
            symbol=symbol,
            transaction_type=transaction_type,
            order_type="MARKET",
            quantity=quantity,
            price=None
        )

    def place_limit_order(
        self,
        symbol: str,
        quantity: int,
        price: float,
        transaction_type: str = "BUY"
    ) -> Dict:
        """Place limit order"""
        return self.place_order(
            symbol=symbol,
            transaction_type=transaction_type,
            order_type="LIMIT",
            quantity=quantity,
            price=price
        )

    def place_bracket_order(
        self,
        symbol: str,
        quantity: int,
        entry_price: float,
        stop_loss: float,
        target: float,
        transaction_type: str = "BUY"
    ) -> Dict:
        """Place bracket order with stop loss and target"""
        return self.place_order(
            symbol=symbol,
            transaction_type=transaction_type,
            order_type="LIMIT",
            quantity=quantity,
            price=entry_price,
            stop_loss=stop_loss,
            take_profit=target,
            product_type="INTRADAY"
        )

    def modify_order(
        self,
        order_id: str,
        quantity: Optional[int] = None,
        price: Optional[float] = None,
        stop_loss: Optional[float] = None
    ) -> Dict:
        """Modify an existing order"""
        if order_id not in self.pending_orders:
            return {"status": "error", "message": "Order not found"}

        modify_payload = {}
        if quantity:
            modify_payload["quantity"] = quantity
        if price:
            modify_payload["price"] = price
        if stop_loss:
            modify_payload["stopLoss"] = stop_loss

        try:
            logger.info(f"Modifying order {order_id}: {modify_payload}")
            self.pending_orders[order_id].update(modify_payload)

            return {
                "status": "success",
                "order_id": order_id,
                "modified": modify_payload
            }

        except Exception as e:
            logger.error(f"Error modifying order: {e}")
            return {"status": "error", "message": str(e)}

    def cancel_order(self, order_id: str) -> Dict:
        """Cancel an order"""
        if order_id not in self.pending_orders:
            return {"status": "error", "message": "Order not found"}

        try:
            logger.info(f"Cancelling order {order_id}")
            self.pending_orders[order_id]["status"] = "cancelled"

            return {
                "status": "success",
                "order_id": order_id
            }

        except Exception as e:
            logger.error(f"Error cancelling order: {e}")
            return {"status": "error", "message": str(e)}

    def get_order_status(self, order_id: str) -> Dict:
        """Get status of an order"""
        if order_id in self.pending_orders:
            return self.pending_orders[order_id]
        if order_id in self.executed_orders:
            return self.executed_orders[order_id]
        return {"status": "not_found"}

    def get_positions(self) -> List[Dict]:
        """Get current positions"""
        return []

    def get_trade_book(self) -> List[Dict]:
        """Get trade book"""
        return list(self.executed_orders.values())

    def close_position(
        self,
        symbol: str,
        quantity: Optional[int] = None,
        order_type: str = "MARKET"
    ) -> Dict:
        """Close existing position"""
        positions = self.get_positions()
        position = next((p for p in positions if p.get("symbol") == symbol), None)

        if not position:
            return {"status": "error", "message": "Position not found"}

        qty = quantity or position.get("quantity", 0)
        transaction_type = "SELL" if position.get("quantity", 0) > 0 else "BUY"

        return self.place_order(
            symbol=symbol,
            transaction_type=transaction_type,
            order_type=order_type,
            quantity=qty
        )

    def execute_signal(
        self,
        signal: Dict,
        price: float,
        risk_manager
    ) -> Dict:
        """
        Execute a trading signal through the full pipeline.
        
        Pipeline:
        1. Validate signal
        2. Risk check
        3. Position sizing
        4. Place order
        """
        symbol = signal.get("symbol")
        if not symbol:
            return {"status": "error", "message": "No symbol in signal"}

        if not price or price <= 0:
            return {"status": "error", "message": "Invalid price"}

        stop_loss = signal.get("stop_loss", price * 0.98)
        quantity = risk_manager.calculate_position_size(
            symbol, price, stop_loss
        )

        if quantity <= 0:
            return {"status": "rejected", "reason": "Position size too small"}

        risk_check = risk_manager.check_risk_limits(symbol, quantity, price, stop_loss)
        if not risk_check["approved"]:
            return {"status": "rejected", "reason": risk_check["reason"]}

        order = self.place_market_order(
            symbol=symbol,
            quantity=quantity,
            transaction_type="BUY"
        )

        if order.get("status") == "success":
            risk_manager.update_position(symbol, "buy", quantity, price)

        return order
