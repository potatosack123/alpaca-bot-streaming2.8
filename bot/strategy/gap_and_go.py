# bot/strategy/gap_and_go_v2.py
"""
Gap-and-Go v2 - Dynamic, Scanner-Ready Strategy
Strategy manages its own exits (ATR/VWAP/breakeven/trailing)
UI SL/TP are hard guardrails only

TUNED FOR: Volatile small-cap stocks with big gap movements
"""
from __future__ import annotations
from typing import Optional, Dict, Literal
from datetime import time, datetime, timezone
from collections import deque
import pytz
import logging

from ..state import SessionState, Signal, SignalType, Bar
from .base import StrategyBase

log = logging.getLogger(__name__)

class GapAndGo(StrategyBase):
    name = "GapAndGo"
    default_timeframe = "1m"
    supported_timeframes = {"1m"}

    def __init__(self,
                 # Entry filters (pre-scanner)
                 min_gap_pct: float = 2.0,  # Lower for testing
                 max_gap_pct: float = 35.0,
                 min_price: float = 2.0,
                 max_price: float = 20.0,
                 min_premarket_vol: int = 50000,  # Lower for testing
                 max_spread: float = 0.08,
                 confirm_bars: int = 2,  # 2-bar confirmation
                 volume_surge_factor: float = 1.2,  # Less aggressive
                 allow_multiple_entries: bool = False,
                 trade_cutoff_minute: int = 30,  # Stop new entries at 10:00 ET
                 
                 # Exit management - TUNED FOR VOLATILE SMALL CAPS
                 atr_len: int = 10,  # Longer lookback
                 atr_stop_mult: float = 1.8,  # WIDER stops for volatility
                 trail_floor_mult: float = 0.8,
                 vwap_crack_bars: int = 2,  # Require 2 bars to confirm VWAP crack
                 exit_time_hour: int = 10,  # Force exit at 10:00 ET
                 exit_time_minute: int = 0,
                 
                 # Direction
                 trade_direction: Literal["long_only", "short_only", "both"] = "both",
                 
                 debug: bool = True
    ):
        # Entry filters
        self.min_gap_pct = min_gap_pct
        self.max_gap_pct = max_gap_pct
        self.min_price = min_price
        self.max_price = max_price
        self.min_premarket_vol = min_premarket_vol
        self.max_spread = max_spread
        self.confirm_bars = confirm_bars
        self.volume_surge_factor = volume_surge_factor
        self.allow_multiple_entries = allow_multiple_entries
        self.trade_cutoff_minute = trade_cutoff_minute
        
        # Exit parameters
        self.atr_len = atr_len
        self.atr_stop_mult = atr_stop_mult
        self.trail_floor_mult = trail_floor_mult
        self.vwap_crack_bars = vwap_crack_bars
        self.exit_time_hour = exit_time_hour
        self.exit_time_minute = exit_time_minute
        
        self.trade_direction = trade_direction
        self.debug = debug
        
        # State tracking (per symbol)
        self.prev_close: Dict[str, float] = {}
        self.premarket_high: Dict[str, float] = {}
        self.premarket_low: Dict[str, float] = {}
        self.premarket_volume: Dict[str, int] = {}
        self.first_break_done: Dict[str, bool] = {}
        self.in_position: Dict[str, bool] = {}
        self.position_direction: Dict[str, str] = {}  # 'long' or 'short'
        
        # Entry tracking
        self.entry_time: Dict[str, datetime] = {}
        self.entry_price: Dict[str, float] = {}
        self.initial_stop: Dict[str, float] = {}
        self.r_value: Dict[str, float] = {}
        self.breakeven_locked: Dict[str, bool] = {}
        self.breakeven_lock_time: Dict[str, Optional[datetime]] = {}
        
        # Breakout confirmation tracking
        self.breakout_confirm_count: Dict[str, int] = {}
        
        # Gap tracking - FIXED: Store session gap once
        self.session_gap_pct: Dict[str, float] = {}
        self.session_open: Dict[str, float] = {}
        
        # VWAP crack confirmation tracking
        self.vwap_crack_count: Dict[str, int] = {}
        
        # ATR tracking
        self.atr_buffer: Dict[str, deque] = {}  # Rolling true range values
        self.atr: Dict[str, float] = {}
        self.last_close: Dict[str, float] = {}
        
        # VWAP tracking
        self.vwap_sum_pv: Dict[str, float] = {}  # sum(price * volume)
        self.vwap_sum_v: Dict[str, float] = {}   # sum(volume)
        self.vwap: Dict[str, float] = {}
        
        # Volume tracking
        self.volume_samples: Dict[str, deque] = {}
        self.avg_volume: Dict[str, float] = {}
        
        # Daily state
        self.current_date: Dict[str, str] = {}
        self.session_start_logged: Dict[str, bool] = {}
        
        # Scanner integration (future)
        self.scanner_data: Dict[str, Dict] = {}  # Optional scanner metadata
        
        self.east = pytz.timezone("America/New_York")

    def on_start(self, session_state: SessionState) -> None:
        """Initialize strategy state"""
        self.prev_close.clear()
        self.premarket_high.clear()
        self.premarket_low.clear()
        self.premarket_volume.clear()
        self.first_break_done.clear()
        self.in_position.clear()
        self.position_direction.clear()
        
        self.entry_time.clear()
        self.entry_price.clear()
        self.initial_stop.clear()
        self.r_value.clear()
        self.breakeven_locked.clear()
        self.breakeven_lock_time.clear()
        
        self.breakout_confirm_count.clear()
        self.session_gap_pct.clear()
        self.session_open.clear()
        self.vwap_crack_count.clear()
        
        self.atr_buffer.clear()
        self.atr.clear()
        self.last_close.clear()
        
        self.vwap_sum_pv.clear()
        self.vwap_sum_v.clear()
        self.vwap.clear()
        
        self.volume_samples.clear()
        self.avg_volume.clear()
        
        self.current_date.clear()
        self.session_start_logged.clear()
        self.scanner_data.clear()
        
        if self.debug:
            log.info(f"GapAndGoV2 initialized: gap {self.min_gap_pct}-{self.max_gap_pct}%, "
                    f"price ${self.min_price}-${self.max_price}, "
                    f"direction={self.trade_direction}, ATR={self.atr_len}bars×{self.atr_stop_mult}, "
                    f"VWAP_confirm={self.vwap_crack_bars}bars")

    def _get_eastern_time(self, bar: Bar) -> Optional[datetime]:
        """Convert bar timestamp to Eastern time"""
        if not bar.timestamp:
            return None
        if bar.timestamp.tzinfo is None:
            utc_time = bar.timestamp.replace(tzinfo=pytz.UTC)
        else:
            utc_time = bar.timestamp
        return utc_time.astimezone(self.east)

    def _is_premarket(self, eastern_time: datetime) -> bool:
        """Check if time is premarket (4am-9:30am ET)"""
        t = eastern_time.time()
        return time(4, 0) <= t < time(9, 30)

    def _is_market_hours(self, eastern_time: datetime) -> bool:
        """Check if time is during market hours (9:30am-4pm ET)"""
        t = eastern_time.time()
        return time(9, 30) <= t <= time(16, 0)

    def _should_exit_time(self, eastern_time: datetime) -> bool:
        """Check if we should force exit (at or after exit time)"""
        t = eastern_time.time()
        exit_t = time(self.exit_time_hour, self.exit_time_minute)
        return t >= exit_t

    def _reset_daily_state(self, symbol: str, date_str: str):
        """Reset state for new trading day"""
        if self.current_date.get(symbol) != date_str:
            self.current_date[symbol] = date_str
            
            # Reset premarket tracking
            self.premarket_high[symbol] = float('-inf')
            self.premarket_low[symbol] = float('inf')
            self.premarket_volume[symbol] = 0
            
            # Reset entry flags
            self.first_break_done[symbol] = False
            self.in_position[symbol] = False
            self.position_direction.pop(symbol, None)
            
            # Reset position tracking
            self.entry_time.pop(symbol, None)
            self.entry_price.pop(symbol, None)
            self.initial_stop.pop(symbol, None)
            self.r_value.pop(symbol, None)
            self.breakeven_locked[symbol] = False
            self.breakeven_lock_time[symbol] = None
            
            # Reset breakout confirmation
            self.breakout_confirm_count[symbol] = 0
            
            # Reset gap tracking
            self.session_gap_pct.pop(symbol, None)
            self.session_open.pop(symbol, None)
            
            # Reset VWAP crack tracking
            self.vwap_crack_count[symbol] = 0
            
            # Reset ATR
            self.atr_buffer[symbol] = deque(maxlen=self.atr_len)
            self.atr[symbol] = 0.0
            self.last_close.pop(symbol, None)
            
            # Reset VWAP
            self.vwap_sum_pv[symbol] = 0.0
            self.vwap_sum_v[symbol] = 0.0
            self.vwap[symbol] = 0.0
            
            # Reset volume tracking
            self.volume_samples[symbol] = deque(maxlen=100)
            self.avg_volume[symbol] = 0.0
            
            self.session_start_logged[symbol] = False

    def _update_atr(self, symbol: str, bar: Bar):
        """Update ATR calculation (lightweight rolling)"""
        high = bar.high
        low = bar.low
        prev_close = self.last_close.get(symbol, bar.close)
        
        # True range = max(high-low, |high-prev_close|, |low-prev_close|)
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        
        if symbol not in self.atr_buffer:
            self.atr_buffer[symbol] = deque(maxlen=self.atr_len)
        
        self.atr_buffer[symbol].append(tr)
        self.last_close[symbol] = bar.close
        
        # Calculate ATR as average of true ranges
        if len(self.atr_buffer[symbol]) > 0:
            self.atr[symbol] = sum(self.atr_buffer[symbol]) / len(self.atr_buffer[symbol])
        else:
            # Fallback if no ATR yet (use 0.5% of price)
            self.atr[symbol] = bar.close * 0.005

    def _update_vwap(self, symbol: str, bar: Bar):
        """Update VWAP calculation"""
        typical_price = (bar.high + bar.low + bar.close) / 3.0
        
        self.vwap_sum_pv[symbol] = self.vwap_sum_pv.get(symbol, 0.0) + (typical_price * bar.volume)
        self.vwap_sum_v[symbol] = self.vwap_sum_v.get(symbol, 0.0) + bar.volume
        
        if self.vwap_sum_v[symbol] > 0:
            self.vwap[symbol] = self.vwap_sum_pv[symbol] / self.vwap_sum_v[symbol]

    def _update_volume_tracking(self, symbol: str, bar: Bar, eastern_time: datetime):
        """Track average volume (after first 5 minutes)"""
        minutes_since_open = (eastern_time.hour - 9) * 60 + (eastern_time.minute - 30)
        
        if minutes_since_open > 5:
            if symbol not in self.volume_samples:
                self.volume_samples[symbol] = deque(maxlen=100)
            
            self.volume_samples[symbol].append(bar.volume)
            
            if len(self.volume_samples[symbol]) > 0:
                self.avg_volume[symbol] = sum(self.volume_samples[symbol]) / len(self.volume_samples[symbol])

    def _check_entry_eligibility(self, symbol: str, bar: Bar, eastern_time: datetime) -> tuple[bool, str]:
        """
        Check if symbol is eligible for entry
        Returns: (eligible, reason)
        Scanner can override these checks in the future
        """
        # Check if scanner data exists (future feature)
        scanner = self.scanner_data.get(symbol)
        
        # Filter 1: Price range
        if bar.close < self.min_price or bar.close > self.max_price:
            return False, f"Price ${bar.close:.2f} outside range ${self.min_price}-${self.max_price}"
        
        # Filter 2: Premarket volume
        pm_vol = self.premarket_volume.get(symbol, 0)
        if pm_vol < self.min_premarket_vol:
            return False, f"Premarket volume {pm_vol:,} < {self.min_premarket_vol:,}"
        
        # Filter 3: Spread check (requires bid/ask data)
        if hasattr(bar, 'bid') and hasattr(bar, 'ask') and bar.bid > 0:
            spread = bar.ask - bar.bid
            spread_pct = spread / bar.close
            max_allowed_spread = max(self.max_spread, 0.015)  # At least 1.5% or user setting
            if spread_pct > max_allowed_spread:
                return False, f"Spread {spread_pct*100:.2f}% > {max_allowed_spread*100:.2f}%"
        
        # Filter 4: Volume surge
        avg_vol = self.avg_volume.get(symbol, 0)
        if avg_vol > 0 and bar.volume < (avg_vol * self.volume_surge_factor):
            return False, f"Volume {bar.volume:,} < {self.volume_surge_factor}x avg ({avg_vol:,.0f})"
        
        return True, "Eligible"

    def _calculate_initial_stop(self, symbol: str, entry_price: float, direction: str) -> float:
        """Calculate initial stop loss using ATR"""
        atr = self.atr.get(symbol, entry_price * 0.005)  # Fallback to 0.5% if no ATR
        
        if direction == "long":
            return entry_price - (self.atr_stop_mult * atr)
        else:  # short
            return entry_price + (self.atr_stop_mult * atr)

    def _update_trailing_stop(self, symbol: str, bar: Bar, direction: str) -> Optional[float]:
        """
        Update trailing stop logic
        Returns new stop level if should exit, None otherwise
        """
        if not self.in_position.get(symbol):
            return None
        
        entry = self.entry_price.get(symbol)
        current_stop = self.initial_stop.get(symbol)
        vwap = self.vwap.get(symbol)
        atr = self.atr.get(symbol, entry * 0.005)
        r = self.r_value.get(symbol, atr)
        
        if not all([entry, current_stop, vwap]):
            return None
        
        # === Breakeven Lock ===
        if not self.breakeven_locked.get(symbol, False):
            # DEBUG LOGGING
            if self.debug:
                log.debug(f"[{symbol}] BE check: close=${bar.close:.4f}, "
                         f"entry=${entry:.4f}, r=${r:.4f}, "
                         f"entry+r=${entry+r:.4f}, "
                         f"triggers={bar.close >= entry + r}")
            
            if direction == "long":
                if bar.close >= entry + r:
                    # WIDENED: Larger buffers for volatile small caps
                    if entry < 10:
                        be_buffer = max(0.03, 0.30 * r, 0.5 * atr)  # WIDER for <$10
                    else:
                        be_buffer = max(0.02, 0.20 * r, 0.4 * atr)  # WIDER for >$10
                    
                    self.initial_stop[symbol] = entry + be_buffer
                    self.breakeven_locked[symbol] = True
                    self.breakeven_lock_time[symbol] = bar.timestamp
                    if self.debug:
                        log.info(f"[{symbol}] Breakeven locked at ${self.initial_stop[symbol]:.2f} "
                                f"(entry=${entry:.2f}, r=${r:.4f}, buffer=${be_buffer:.4f})")
            else:  # short
                if bar.close <= entry - r:
                    # WIDENED: Larger buffers for volatile small caps
                    if entry < 10:
                        be_buffer = max(0.03, 0.30 * r, 0.5 * atr)
                    else:
                        be_buffer = max(0.02, 0.20 * r, 0.4 * atr)
                    
                    self.initial_stop[symbol] = entry - be_buffer
                    self.breakeven_locked[symbol] = True
                    self.breakeven_lock_time[symbol] = bar.timestamp
                    if self.debug:
                        log.info(f"[{symbol}] Breakeven locked at ${self.initial_stop[symbol]:.2f} "
                                f"(entry=${entry:.2f}, r=${r:.4f}, buffer=${be_buffer:.4f})")
        
        # === Trailing Stop (VWAP + swing) ===
        if direction == "long":
            # Trail under VWAP - trail_floor_mult * ATR
            new_stop = vwap - (self.trail_floor_mult * atr)
            
            # Only ratchet up, never down
            if new_stop > current_stop:
                self.initial_stop[symbol] = new_stop
                if self.debug:
                    log.debug(f"[{symbol}] Trailing stop updated: ${new_stop:.2f} (VWAP: ${vwap:.2f})")
        else:  # short
            # Trail over VWAP + trail_floor_mult * ATR
            new_stop = vwap + (self.trail_floor_mult * atr)
            
            # Only ratchet down, never up
            if new_stop < current_stop:
                self.initial_stop[symbol] = new_stop
                if self.debug:
                    log.debug(f"[{symbol}] Trailing stop updated: ${new_stop:.2f} (VWAP: ${vwap:.2f})")
        
        # === VWAP Hard Exit with 2-BAR CONFIRMATION ===
        # WIDENED: Buffer is now 0.5×ATR or 0.5% of price, whichever is larger
        vwap_buffer = max(0.5 * atr, 0.005 * bar.close)
        
        if direction == "long":
            vwap_crack = bar.close < (vwap - vwap_buffer)
        else:  # short
            vwap_crack = bar.close > (vwap + vwap_buffer)
        
        # Track consecutive VWAP crack bars
        if vwap_crack:
            self.vwap_crack_count[symbol] = self.vwap_crack_count.get(symbol, 0) + 1
            if self.debug:
                log.debug(f"[{symbol}] VWAP crack bar {self.vwap_crack_count[symbol]}/{self.vwap_crack_bars}")
        else:
            self.vwap_crack_count[symbol] = 0  # Reset if price comes back
        
        # Only exit after N consecutive bars
        if self.vwap_crack_count.get(symbol, 0) >= self.vwap_crack_bars:
            if self.debug:
                log.info(f"[{symbol}] VWAP crack exit ({direction}): "
                        f"${bar.close:.2f} vs VWAP ${vwap:.2f} "
                        f"(confirmed {self.vwap_crack_bars} bars)")
            return bar.close  # Exit now
        
        return None

    def on_bar(self, symbol: str, bar: Bar, state: SessionState) -> Optional[Signal]:
        """
        Main strategy logic
        Handles both entry and dynamic exit management
        """
        eastern_time = self._get_eastern_time(bar)
        if not eastern_time:
            return None
        
        date_str = eastern_time.strftime("%Y-%m-%d")
        self._reset_daily_state(symbol, date_str)
        
        # === HANDLE MISSING PREV_CLOSE ===
        if symbol not in self.prev_close or self.prev_close[symbol] == 0:
            if self._is_market_hours(eastern_time):
                # Try to get actual prior close from state if available
                if hasattr(state, 'get_prior_close'):
                    try:
                        prior_close = state.get_prior_close(symbol, eastern_time.date())
                        if prior_close and prior_close > 0:
                            self.prev_close[symbol] = prior_close
                            if self.debug:
                                log.info(f"[{symbol}] Using prior RTH close: ${prior_close:.2f}")
                    except:
                        pass
                
                # Fallback to bar.open if still missing
                if symbol not in self.prev_close or self.prev_close[symbol] == 0:
                    self.prev_close[symbol] = bar.open
                    if self.debug:
                        log.warning(f"[{symbol}] Missing prev_close, using bar.open: ${bar.open:.2f} "
                                  f"(gap calculation will be 0% for first session)")
        
        # === PREMARKET: Track metrics ===
        if self._is_premarket(eastern_time):
            pm_high = self.premarket_high.get(symbol, float('-inf'))
            pm_low = self.premarket_low.get(symbol, float('inf'))
            
            self.premarket_high[symbol] = max(pm_high, bar.high)
            self.premarket_low[symbol] = min(pm_low, bar.low)
            self.premarket_volume[symbol] = self.premarket_volume.get(symbol, 0) + bar.volume
            
            return None
        
        # === AFTER HOURS: Track prev_close ===
        if not self._is_market_hours(eastern_time):
            self.prev_close[symbol] = bar.close
            return None
        
        # === MARKET HOURS ===
        
        # Update indicators every bar
        self._update_atr(symbol, bar)
        self._update_vwap(symbol, bar)
        self._update_volume_tracking(symbol, bar, eastern_time)
        
        # Log session start once at 09:30:00
        if not self.session_start_logged.get(symbol, False):
            minutes_since_open = (eastern_time.hour - 9) * 60 + (eastern_time.minute - 30)
            if minutes_since_open == 0:
                prev_close = self.prev_close.get(symbol, bar.open)
                pm_high = self.premarket_high.get(symbol, bar.open)
                pm_low = self.premarket_low.get(symbol, bar.open)
                
                # FIXED: Calculate and store session gap ONCE
                if prev_close > 0:
                    self.session_gap_pct[symbol] = ((bar.open - prev_close) / prev_close) * 100
                    self.session_open[symbol] = bar.open
                else:
                    self.session_gap_pct[symbol] = 0.0
                    self.session_open[symbol] = bar.open
                
                gap_pct = self.session_gap_pct[symbol]
                
                log.info(f"[{symbol}] Session start - "
                        f"Open: ${bar.open:.2f}, Prev: ${prev_close:.2f}, "
                        f"PM High: ${pm_high:.2f}, PM Low: ${pm_low:.2f}, "
                        f"Gap(open): {gap_pct:+.2f}%, PM Vol: {self.premarket_volume.get(symbol, 0):,}")
                
                self.session_start_logged[symbol] = True
        
        # === POSITION MANAGEMENT (if in position) ===
        if self.in_position.get(symbol, False):
            direction = self.position_direction.get(symbol, "long")
            
            # Time-based exit
            if self._should_exit_time(eastern_time):
                if self.debug:
                    log.info(f"[{symbol}] Time exit ({direction}): {self.exit_time_hour}:{self.exit_time_minute:02d}")
                self.in_position[symbol] = False
                self.position_direction.pop(symbol, None)
                return Signal(SignalType.SELL if direction == "long" else SignalType.BUY)
            
            # Dynamic trailing stop and VWAP exit
            exit_price = self._update_trailing_stop(symbol, bar, direction)
            if exit_price is not None:
                self.in_position[symbol] = False
                self.position_direction.pop(symbol, None)
                return Signal(SignalType.SELL if direction == "long" else SignalType.BUY)
            
            # Check if stop hit
            current_stop = self.initial_stop.get(symbol)
            if current_stop:
                if direction == "long":
                    if bar.low <= current_stop:
                        if self.debug:
                            log.info(f"[{symbol}] Stop hit (long): ${bar.low:.2f} <= ${current_stop:.2f}")
                        self.in_position[symbol] = False
                        self.position_direction.pop(symbol, None)
                        return Signal(SignalType.SELL)
                else:  # short
                    if bar.high >= current_stop:
                        if self.debug:
                            log.info(f"[{symbol}] Stop hit (short): ${bar.high:.2f} >= ${current_stop:.2f}")
                        self.in_position[symbol] = False
                        self.position_direction.pop(symbol, None)
                        return Signal(SignalType.BUY)
            
            return None
        
        # === ENTRY LOGIC ===
        
        # Only trade in first N minutes
        minutes_since_open = (eastern_time.hour - 9) * 60 + (eastern_time.minute - 30)
        
        # FIXED: Log cutoff message EXACTLY at cutoff time
        if minutes_since_open == self.trade_cutoff_minute:
            if not self.first_break_done.get(symbol, False):
                gap_pct = self.session_gap_pct.get(symbol, 0.0)
                pm_high = self.premarket_high.get(symbol, 0.0)
                pm_low = self.premarket_low.get(symbol, 0.0)
                # FIXED: Only log if gap was in valid range
                if self.min_gap_pct <= abs(gap_pct) <= self.max_gap_pct and self.debug:
                    log.info(f"[{symbol}] No entry: never broke PM "
                            f"{'high' if gap_pct > 0 else 'low'} "
                            f"(${pm_high if gap_pct > 0 else pm_low:.2f}) before cutoff")

        # Block new entries after cutoff
        if minutes_since_open > self.trade_cutoff_minute:
            return None
        
        # Skip if already traded today (unless multiple entries allowed)
        if self.first_break_done.get(symbol, False) and not self.allow_multiple_entries:
            return None
        
        # Check eligibility
        eligible, reason = self._check_entry_eligibility(symbol, bar, eastern_time)
        if not eligible:
            # Log rejection reason
            if self.debug:
                log.debug(f"[{symbol}] Not eligible: {reason}")
            return None
        
        # Get reference price (premarket high for gap-up, premarket low for gap-down)
        prev_close = self.prev_close.get(symbol, bar.open)
        pm_high = self.premarket_high.get(symbol, bar.open)
        pm_low = self.premarket_low.get(symbol, bar.open)
        
        if prev_close <= 0:
            return None
        
        # FIXED: Use stored session gap, not current bar gap
        gap_pct = self.session_gap_pct.get(symbol, 0.0)
        
        # Determine direction
        is_gap_up = gap_pct >= self.min_gap_pct and gap_pct <= self.max_gap_pct
        is_gap_down = gap_pct <= -self.min_gap_pct and gap_pct >= -self.max_gap_pct
        
        should_trade_long = is_gap_up and self.trade_direction in ["long_only", "both"]
        should_trade_short = is_gap_down and self.trade_direction in ["short_only", "both"]
        
        if not (should_trade_long or should_trade_short):
            # ADDED: Log why gap was rejected (too large/small)
            if self.debug and abs(gap_pct) >= 1.0:  # Only if gap is somewhat significant
                if abs(gap_pct) > self.max_gap_pct:
                    log.debug(f"[{symbol}] Gap {gap_pct:.2f}% exceeds max {self.max_gap_pct}%")
                elif abs(gap_pct) < self.min_gap_pct:
                    log.debug(f"[{symbol}] Gap {gap_pct:.2f}% below min {self.min_gap_pct}%")
            return None
        
        direction = "long" if should_trade_long else "short"
        ref_price = pm_high if direction == "long" else pm_low
        
        # Dynamic offset: minimum 3 cents or 0.05% of price
        offset = max(0.03, 0.0005 * ref_price)
        
        # Breakout condition
        if direction == "long":
            broke = bar.high >= (ref_price + offset) and bar.close >= ref_price
        else:
            broke = bar.low <= (ref_price - offset) and bar.close <= ref_price
        
        if broke:
            # Increment confirmation counter
            self.breakout_confirm_count[symbol] = self.breakout_confirm_count.get(symbol, 0) + 1
            
            # Require N consecutive bars confirming breakout
            if self.breakout_confirm_count[symbol] < self.confirm_bars:
                if self.debug:
                    log.debug(f"[{symbol}] Breakout bar {self.breakout_confirm_count[symbol]}/{self.confirm_bars}")
                return None
            
            # Calculate initial stop and R
            atr = self.atr.get(symbol, bar.close * 0.005)
            
            # Ensure we have enough bars for valid ATR
            atr_buffer_len = len(self.atr_buffer.get(symbol, []))
            if atr_buffer_len < 3:
                if self.debug:
                    log.warning(f"[{symbol}] Insufficient ATR data ({atr_buffer_len} bars), using 1% fallback")
                atr = bar.close * 0.01  # Use 1% as fallback
            
            initial_stop = self._calculate_initial_stop(symbol, bar.close, direction)
            r_value = abs(bar.close - initial_stop)
            
            # Validate R is not absurdly small
            min_r = bar.close * 0.005  # At least 0.5% of price
            if r_value < min_r:
                if self.debug:
                    log.warning(f"[{symbol}] R too small (${r_value:.4f} < ${min_r:.4f}), skipping entry")
                return None
            
            # Calculate entry gap % for logging
            entry_gap_pct = ((bar.close - prev_close) / prev_close) * 100 if prev_close > 0 else 0.0
            
            if self.debug:
                log.info(f"[{symbol}] ENTRY ({direction.upper()}): "
                        f"Gap(open)={gap_pct:.2f}%, Gap(entry)={entry_gap_pct:.2f}%, "
                        f"Entry=${bar.close:.2f}, Ref=${ref_price:.2f}, "
                        f"ATR=${atr:.4f} ({atr_buffer_len}bars), "
                        f"Stop=${initial_stop:.2f}, R=${r_value:.2f}")
                log.info(f"[{symbol}] ✅ Signal: {SignalType.BUY if direction == 'long' else SignalType.SELL}")
            
            # Store position state
            self.first_break_done[symbol] = True
            self.in_position[symbol] = True
            self.position_direction[symbol] = direction
            self.entry_time[symbol] = eastern_time
            self.entry_price[symbol] = bar.close
            self.initial_stop[symbol] = initial_stop
            self.r_value[symbol] = r_value
            self.breakeven_locked[symbol] = False
            self.breakeven_lock_time[symbol] = None
            
            return Signal(SignalType.BUY if direction == "long" else SignalType.SELL)
        
        return None

    def on_stop(self, session_state: SessionState) -> None:
        """Cleanup when strategy stops"""
        if self.debug:
            log.info("GapAndGoV2 stopped")
    
    def set_scanner_data(self, symbol: str, data: Dict):
        """
        Future: Accept scanner metadata
        Scanner can provide: gap_pct, premarket_volume, rvol_10d, float_m, 
                            spread_now, news_score, eligibility, etc.
        """
        self.scanner_data[symbol] = data
