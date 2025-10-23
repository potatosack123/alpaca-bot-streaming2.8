"""
Alpaca adapter - TRADING ONLY
All market data methods removed - use Polygon instead
"""
from __future__ import annotations
import logging
from typing import Optional
from datetime import datetime, timezone

log = logging.getLogger(__name__)

try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import MarketOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce
    ALPACA_PY = True
except Exception:
    ALPACA_PY = False

try:
    import alpaca_trade_api as tradeapi
    TRADE_API = True
except Exception:
    TRADE_API = False


class AlpacaAdapter:
    """
    Alpaca adapter for ORDER EXECUTION ONLY
    All market data comes from Polygon
    """

    def __init__(self, api_key: str, api_secret: str, force_mode: str = "auto"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.force_mode = force_mode
        self.connection_mode: Optional[str] = None
        self._trading_client = None
        
        log.info("AlpacaAdapter initialized (trading only)")

    def connect(self, context: Optional[str] = None, quiet: bool = False) -> str:
        """Connect to Alpaca for trading operations"""
        def _log_connected(mode: str) -> None:
            msg = f"Connected to Alpaca {mode.upper()} for trading."
            (log.debug if quiet else log.info)(msg)
            
        if self.force_mode == "paper":
            self._connect_paper()
            self.connection_mode = "paper"
            _log_connected("paper")
        elif self.force_mode == "live":
            self._connect_live()
            self.connection_mode = "live"
            _log_connected("live")
        else:
            # Auto: try paper first
            try:
                self._connect_paper()
                self.connection_mode = "paper"
                _log_connected("paper")
            except Exception as e:
                log.warning(f"Paper connection failed; trying live. {e}")
                self._connect_live()
                self.connection_mode = "live"
                _log_connected("live")
        
        return self.connection_mode or "paper"

    def _connect_paper(self) -> None:
        """Connect to paper trading"""
        if ALPACA_PY:
            self._trading_client = TradingClient(self.api_key, self.api_secret, paper=True)
            return
        if TRADE_API:
            self._trading_client = tradeapi.REST(
                self.api_key, self.api_secret, 
                base_url="https://paper-api.alpaca.markets"
            )
            return
        raise RuntimeError("Alpaca SDK not installed.")

    def _connect_live(self) -> None:
        """Connect to live trading"""
        if ALPACA_PY:
            self._trading_client = TradingClient(self.api_key, self.api_secret, paper=False)
            return
        if TRADE_API:
            self._trading_client = tradeapi.REST(
                self.api_key, self.api_secret, 
                base_url="https://api.alpaca.markets"
            )
            return
        raise RuntimeError("Alpaca SDK not installed.")

    # ========== CLOCK & MARKET STATUS ==========
    
    def is_market_open_now(self) -> bool:
        """Check if market is currently open"""
        try:
            c = self._trading_client.get_clock()
            return bool(getattr(c, "is_open", False))
        except Exception:
            return False

    def get_clock_info(self):
        """Returns (is_open, next_open_time, next_close_time)"""
        try:
            clock = self._trading_client.get_clock()
            is_open = bool(getattr(clock, "is_open", False))
            next_open = getattr(clock, "next_open", None)
            next_close = getattr(clock, "next_close", None)
            return is_open, next_open, next_close
        except Exception as e:
            log.warning(f"get_clock_info failed: {e}")
            return False, None, None

    # ========== ACCOUNT INFO ==========
    
    def get_account_equity(self) -> float:
        """Get account equity"""
        try:
            a = self._trading_client.get_account()
            return float(getattr(a, "equity", 0.0))
        except Exception:
            return 0.0

    def get_today_pnl(self) -> float:
        """Get today's realized P&L"""
        try:
            a = self._trading_client.get_account()
            # Try different attribute names
            for attr in ("todays_pnl", "equity", "cash"):
                val = getattr(a, attr, None)
                if val is not None:
                    return float(val)
            return 0.0
        except Exception:
            return 0.0

    def get_unrealized_pl_sum(self) -> float:
        """Get total unrealized P&L from all positions"""
        try:
            positions = self._trading_client.get_all_positions()
            total = 0.0
            for p in positions:
                upl = getattr(p, "unrealized_pl", None) or getattr(p, "unrealized_plpc", 0)
                total += float(upl)
            return total
        except Exception:
            return 0.0

    # ========== TRADING OPERATIONS ==========
    
    def flatten_all(self) -> None:
        """Close all open positions"""
        if ALPACA_PY:
            try:
                poss = self._trading_client.get_all_positions()
                for p in poss:
                    qtyf = float(getattr(p, "qty", 0))
                    if qtyf == 0:
                        continue
                    side = OrderSide.SELL if qtyf > 0 else OrderSide.BUY
                    req = MarketOrderRequest(
                        symbol=getattr(p, "symbol", ""),
                        qty=abs(int(qtyf)),
                        side=side,
                        time_in_force=TimeInForce.DAY
                    )
                    self._trading_client.submit_order(order_data=req)
                    log.info(f"Flattened position: {getattr(p, 'symbol', '')}")
            except Exception as e:
                log.warning(f"flatten_all failed: {e}")
        elif TRADE_API:
            try:
                poss = self._trading_client.list_positions()
                for p in poss:
                    qty = abs(int(float(getattr(p, "qty", 0))))
                    if qty <= 0:
                        continue
                    side = "sell" if float(getattr(p, "qty", 0)) > 0 else "buy"
                    self._trading_client.submit_order(
                        symbol=getattr(p, "symbol", ""),
                        qty=qty,
                        side=side,
                        type="market",
                        time_in_force="day"
                    )
                    log.info(f"Flattened position: {getattr(p, 'symbol', '')}")
            except Exception as e:
                log.warning(f"flatten_all failed: {e}")

    def submit_market_order(self, symbol: str, qty: int, side: str) -> None:
        """
        Submit market order
        
        Args:
            symbol: Stock ticker
            qty: Number of shares
            side: 'buy' or 'sell'
        """
        if ALPACA_PY:
            req = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL,
                time_in_force=TimeInForce.DAY
            )
            self._trading_client.submit_order(order_data=req)
            log.info(f"Order submitted: {side.upper()} {qty} {symbol}")
        elif TRADE_API:
            self._trading_client.submit_order(
                symbol=symbol,
                qty=qty,
                side=side,
                type="market",
                time_in_force="day"
            )
            log.info(f"Order submitted: {side.upper()} {qty} {symbol}")
