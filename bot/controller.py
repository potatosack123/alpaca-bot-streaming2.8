from __future__ import annotations
import threading
import logging
import time as _time
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pytz
import queue

from .state import AppSettings, SessionState, RunMode, SignalType, ForceMode
from .strategy import STRATEGIES, load_external_strategies
from .strategy.base import StrategyBase
from .broker.alpaca_adapter import AlpacaAdapter
from .data.polygon_adapter import PolygonAdapter
from .data.polygon_stream import PolygonStream
from .backtest.engine import run_backtest
from .backtest.data import load_bars, register_polygon_adapter

# --- helpers for slot time windows ---
from datetime import time as dtime
import pytz
from .state import StrategySlot

_east = pytz.timezone("America/New_York")

def _parse_hhmm(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        return dtime(9,30)

def _in_window_east(bar_ts_utc: datetime, start_hhmm: str, end_hhmm: str) -> bool:
    t = bar_ts_utc.astimezone(_east).time()
    a = _parse_hhmm(start_hhmm); b = _parse_hhmm(end_hhmm)
    if a <= b:
        return a <= t <= b
    return (t >= a) or (t <= b)
# -------------------------------------

log = logging.getLogger(__name__)
trades_log = logging.getLogger("trades")

class Controller:
    def __init__(self, settings: AppSettings):
        # UPGRADED: Per-strategy lot tracking
        # positions[symbol][strategy_id] = {qty, entry_price, sl, tp, ...}
        self.positions: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.recent_trades: List[Dict[str, Any]] = []
        self.settings = settings
        self.state = SessionState()
        self._worker: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.clear()
        self._adapter: Optional[AlpacaAdapter] = None
        self._polygon: Optional[PolygonAdapter] = None
        self._polygon_stream: Optional[PolygonStream] = None
        self._live_confirmed = False
        load_external_strategies(self.settings.extra_strategy_paths)

    def _log_trade_entry(self, symbol: str, side: str, qty: int, price: float, 
                         sl: float, tp: float, strategy_name: str, 
                         slot_info: dict = None, settings: dict = None):
        """Comprehensive trade entry logging - COMPACT format"""
        log_parts = [
            f"ENTRY {side.upper()} {symbol}",
            f"x{qty}@${price:.2f}",
            f"SL${sl:.2f}",
            f"TP${tp:.2f}",
        ]
        
        # Strategy info
        if slot_info:
            log_parts.append(f"{slot_info.get('name', strategy_name)}[P{slot_info.get('priority', 'N/A')}]")
            log_parts.append(f"{slot_info.get('timeframe', 'N/A')}")
        else:
            log_parts.append(f"{strategy_name}")
        
        # Risk parameters (compact)
        if settings:
            log_parts.append(f"R{settings.get('risk_percent', 'N/A')}%")
        
        trades_log.info(" | ".join(log_parts))

    def _log_trade_exit(self, symbol: str, side: str, qty: int, entry_price: float,
                        exit_price: float, reason: str, strategy_name: str = None,
                        slot_info: dict = None):
        """Comprehensive trade exit logging - COMPACT format"""
        if side == "BUY":
            pnl = (exit_price - entry_price) * qty
            pnl_pct = ((exit_price / entry_price) - 1) * 100
        else:
            pnl = (entry_price - exit_price) * qty
            pnl_pct = ((entry_price / exit_price) - 1) * 100
        
        log_parts = [
            f"EXIT {reason} {symbol}",
            f"x{qty}",
            f"${entry_price:.2f}→${exit_price:.2f}",
            f"P&L ${pnl:+.2f}({pnl_pct:+.2f}%)",
        ]
        
        # Strategy info (compact)
        if slot_info:
            log_parts.append(f"{slot_info.get('name', strategy_name or 'N/A')}[P{slot_info.get('priority', 'N/A')}]")
        elif strategy_name:
            log_parts.append(f"{strategy_name}")
        
        trades_log.info(" | ".join(log_parts))

    def connect(self, api_key: str, api_secret: str, polygon_key: str) -> str:
        """Connect to Alpaca for trading and Polygon for market data"""
        # Alpaca for trading only
        self._adapter = AlpacaAdapter(
            api_key, 
            api_secret, 
            force_mode=self.settings.force_mode.value if hasattr(self.settings.force_mode, "value") else str(self.settings.force_mode)
        )
        mode = self._adapter.connect()
        self.state.connection_mode = mode
        log.info("Alpaca connected for TRADING: %s", mode.upper())
        
        # Polygon for market data
        self._polygon = PolygonAdapter(polygon_key)
        register_polygon_adapter(self._polygon)
        log.info("Polygon connected for MARKET DATA")
        
        return mode

    def start(self, run_mode: RunMode, run_folder: Optional[Path]=None, live_confirmed: bool=False) -> None:
        if self._worker and self._worker.is_alive():
            log.warning("Worker already running.")
            return
        
        self.positions.clear()
        self.recent_trades.clear()
        
        self.state.run_mode = run_mode
        self.state.started = True
        self.state.paused = False
        self.state.should_stop = False
        self.state.flatten_on_stop = self.settings.flatten_on_stop
        self.state.run_folder = run_folder
        self._stop_event.clear()
        self._pause_event.clear()
        self._live_confirmed = live_confirmed

        self._worker = threading.Thread(target=self._run_loop, name=f"{run_mode}-worker", daemon=True)
        self._worker.start()

    def pause(self) -> None:
        self.state.paused = True
        self._pause_event.set()
        log.info("Paused: new entries halted; managing exits continues.")

    def resume(self) -> None:
        self.state.paused = False
        self._pause_event.clear()
        log.info("Resumed: new entries allowed.")

    def stop(self, flatten: bool=False) -> None:
        if flatten or self.settings.flatten_on_stop:
            log.info("Flatten & Stop requested.")
            if self._adapter and self.state.run_mode == RunMode.LIVE:
                try:
                    self._adapter.flatten_all()
                except Exception as e:
                    log.exception("Flatten failed: %s", e)
        self.state.should_stop = True
        self._stop_event.set()

    def _run_loop(self) -> None:
        try:
            if self.state.run_mode == RunMode.LIVE:
                self._run_live()
            else:
                self._run_backtest()
        except Exception as e:
            log.exception("Worker crashed: %s", e)
        finally:
            self.state.started = False
            self.state.paused = False
            self.state.should_stop = False
            self.positions.clear()
            log.info("Run ended.")

    def _wait_for_market_open(self) -> bool:
        if not self._adapter:
            return False
        log.info("Market appears closed. Waiting for next open...")
        while not self._stop_event.is_set():
            is_open, next_open, _ = self._adapter.get_clock_info()
            if is_open:
                log.info("Market opened. Starting live loop.")
                return True
            sleep_s = 30
            if next_open:
                now = datetime.now(timezone.utc)
                delta = (next_open - now).total_seconds()
                if delta <= 60:
                    sleep_s = 5
                elif delta <= 300:
                    sleep_s = 15
            _time.sleep(sleep_s)
        return False

    def _run_live(self) -> None:
        if not self._adapter:
            log.error("No connection. Provide API keys and connect first.")
            return

        if not self._adapter.is_market_open_now():
            if not self._wait_for_market_open():
                return

        if self.state.connection_mode == "live" and not self._live_confirmed:
            log.warning("Live mode requires user confirmation. Aborting start.")
            return

        symbols = [s.strip().upper() for s in self.settings.symbols.split(",") if s.strip()]
        tf = self.settings.timeframe
        # global defaults (fractions)
        risk_pct = self.settings.risk_percent / 100.0
        sl_pct = self.settings.stop_loss_percent / 100.0
        tp_pct = self.settings.take_profit_percent / 100.0
        lunch_skip = self.settings.lunch_skip
        east = pytz.timezone("America/New_York")

        # --------- Multi-slot init (priority order) ----------
        slots: List[StrategySlot] = getattr(self.settings, "strategy_slots", []) or []
        enabled_slots = [s for s in slots if s.enabled and s.name in STRATEGIES]
        enabled_slots.sort(key=lambda s: s.priority)  # 1 = highest

        slot_strats: List[tuple[StrategySlot, StrategyBase]] = []
        for s in enabled_slots:
            try:
                strat_cls = STRATEGIES[s.name]
                slot_strats.append((s, strat_cls()))
            except Exception as e:
                log.warning("Slot strategy init failed (%s): %s", s.name, e)
        multi_mode = len(slot_strats) > 0
        # -----------------------------------------------------

        # single-strategy fallback
        strategy: Optional[StrategyBase] = None
        if not multi_mode:
            strat_cls = STRATEGIES.get(self.settings.selected_strategy)
            if not strat_cls:
                log.error("Strategy not found: %s", self.settings.selected_strategy)
                return
            strategy = strat_cls()
            strategy.on_start(self.state)
        else:
            for s, strat in slot_strats:
                try:
                    strat.on_start(self.state)
                except Exception as e:
                    log.debug("on_start %s failed: %s", s.name, e)

        # UPGRADED: Per-strategy position tracking
        # positions[symbol][strategy_id] = {qty, entry_price, sl, tp, strategy_obj, ...}
        positions: Dict[str, Dict[str, Dict[str, Any]]] = {}

        md_queue: "queue.Queue" = queue.Queue()

        # Polygon WebSocket for live bars
        def on_polygon_bar(symbol: str, bar_data: dict):
            """Handle incoming Polygon bar"""
            try:
                md_queue.put(("bar", symbol, bar_data))
            except Exception as e:
                log.debug(f"Bar queue error: {e}")

        if not self._polygon:
            log.error("Polygon not initialized. Cannot stream live data.")
            return

        try:
            self._polygon_stream = PolygonStream(
                api_key=self._polygon.api_key,
                symbols=symbols,
                on_bar=on_polygon_bar
            )
            self._polygon_stream.start()
            log.info("Polygon WebSocket started for live data")
        except Exception as e:
            log.error(f"Failed to start Polygon stream: {e}")
            return

        log.info("Live loop started for %s | TF=%s | mode=%s", symbols, tf, self.state.connection_mode)

        last_pl_update_ts = 0.0
        while not self._stop_event.is_set():
            paused = self._pause_event.is_set()

            try:
                evt = md_queue.get(timeout=1.0)
            except queue.Empty:
                evt = None

            now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)

            if evt:
                etype, sym, bar = evt
                if etype == "bar" and sym in symbols:
                    o = float(bar["o"]); h = float(bar["h"]); l = float(bar["l"]); c = float(bar["c"])
                    # bar timestamp normalization
                    bar_ts = now_utc
                    try:
                        tval = bar.get("t")
                        if tval is not None:
                            if isinstance(tval, (int, float)):
                                if tval > 1e12:
                                    bar_ts = datetime.fromtimestamp(tval / 1e9, tz=timezone.utc)
                                elif tval > 1e10:
                                    bar_ts = datetime.fromtimestamp(tval / 1e3, tz=timezone.utc)
                                else:
                                    bar_ts = datetime.fromtimestamp(tval, tz=timezone.utc)
                            elif isinstance(tval, str):
                                iso = tval.replace("Z", "+00:00")
                                dtp = datetime.fromisoformat(iso)
                                bar_ts = dtp if dtp.tzinfo else dtp.replace(tzinfo=timezone.utc)
                    except Exception:
                        bar_ts = now_utc

                    bar_obj = type("BarObj", (), dict(timestamp=bar_ts, open=o, high=h, low=l, close=c, volume=bar.get("v", 0)))

                    # === STRATEGY-MANAGED EXIT CHECKING (NEW) ===
                    # Check all open positions for strategy exit signals FIRST
                    strategy_lots = positions.get(sym, {})
                    for strategy_id, pos in list(strategy_lots.items()):
                        # Get the strategy object that owns this position
                        strat_obj = pos.get("strategy_obj")
                        if not strat_obj:
                            continue

                        try:
                            # Call strategy.on_bar() for exit signal
                            exit_signal = strat_obj.on_bar(sym, bar_obj, self.state)
                            
                            if exit_signal and exit_signal.type in (SignalType.BUY, SignalType.SELL):
                                pos_side = pos["side"]
                                # Check if signal is opposite side (exit signal)
                                is_exit = (pos_side == "BUY" and exit_signal.type == SignalType.SELL) or \
                                         (pos_side == "SELL" and exit_signal.type == SignalType.BUY)
                                
                                if is_exit:
                                    # STRATEGY EXIT - Priority over SL/TP
                                    qty = pos["qty"]
                                    side_close = "sell" if pos_side == "BUY" else "buy"
                                    
                                    try:
                                        self._adapter.submit_market_order(sym, qty, side_close)
                                        
                                        slot_info = {
                                            "name": pos.get("strategy_name", strategy_id),
                                            "priority": pos.get("priority"),
                                        }
                                        self._log_trade_exit(
                                            sym, pos_side, qty, pos["entry_price"], c,
                                            "STRATEGY_EXIT", pos.get("strategy_name"), slot_info
                                        )
                                        
                                        # Remove from tracking
                                        strategy_lots.pop(strategy_id, None)
                                        if not strategy_lots:
                                            positions.pop(sym, None)
                                        
                                        # Update UI
                                        self.recent_trades.insert(0, {
                                            "time": bar_ts.strftime("%H:%M:%S"),
                                            "action": "CLOSE",
                                            "symbol": sym,
                                            "price": f"${c:.2f}",
                                            "qty": qty,
                                            "reason": f"Strategy Exit [{strategy_id}]"
                                        })
                                        if len(self.recent_trades) > 15:
                                            self.recent_trades.pop()
                                        
                                        # Update positions display
                                        if sym in self.positions and strategy_id in self.positions[sym]:
                                            self.positions[sym].pop(strategy_id, None)
                                            if not self.positions[sym]:
                                                self.positions.pop(sym, None)
                                        
                                    except Exception as e:
                                        log.exception(f"Strategy exit order failed for {sym}: {e}")
                        
                        except Exception as e:
                            log.debug(f"Strategy on_bar error for {sym}/{strategy_id}: {e}")

                    # === BROKER SL/TP GUARDRAIL CHECKING ===
                    # Only after strategy exits have been checked
                    for strategy_id, pos in list(positions.get(sym, {}).items()):
                        side = pos["side"]; sl = pos["sl"]; tp = pos["tp"]
                        exit_now = False; exit_px = None; exit_reason = ""
                        
                        if side == "BUY":
                            if l <= sl: exit_now = True; exit_px = sl; exit_reason = "Stop Loss (Guardrail)"
                            elif h >= tp: exit_now = True; exit_px = tp; exit_reason = "Take Profit (Guardrail)"
                        else:
                            if h >= sl: exit_now = True; exit_px = sl; exit_reason = "Stop Loss (Guardrail)"
                            elif l <= tp: exit_now = True; exit_px = tp; exit_reason = "Take Profit (Guardrail)"
                        
                        if exit_now and exit_px is not None:
                            try:
                                qty = pos["qty"]
                                side_close = "sell" if side == "BUY" else "buy"
                                self._adapter.submit_market_order(sym, qty, side_close)
                                
                                slot_info = {
                                    "name": pos.get("strategy_name", strategy_id),
                                    "priority": pos.get("priority"),
                                }
                                self._log_trade_exit(
                                    sym, side, qty, pos["entry_price"], exit_px,
                                    exit_reason.upper().replace(" ", "_"),
                                    pos.get("strategy_name"), slot_info
                                )
                                
                                positions[sym].pop(strategy_id, None)
                                if not positions[sym]:
                                    positions.pop(sym, None)
                                
                                self.recent_trades.insert(0, {
                                    "time": bar_ts.strftime("%H:%M:%S"),
                                    "action": "CLOSE",
                                    "symbol": sym,
                                    "price": f"${exit_px:.2f}",
                                    "qty": qty,
                                    "reason": exit_reason
                                })
                                if len(self.recent_trades) > 15:
                                    self.recent_trades.pop()
                                
                                if sym in self.positions and strategy_id in self.positions[sym]:
                                    self.positions[sym].pop(strategy_id, None)
                                    if not self.positions[sym]:
                                        self.positions.pop(sym, None)
                            
                            except Exception as e:
                                log.exception(f"Guardrail exit order failed: {e}")
                    
                    # Update position prices for display
                    if sym in positions:
                        if sym not in self.positions:
                            self.positions[sym] = {}
                        
                        for strategy_id, pos in positions[sym].items():
                            if strategy_id not in self.positions[sym]:
                                self.positions[sym][strategy_id] = {}
                            
                            self.positions[sym][strategy_id]["current_price"] = c
                            entry_px = pos["entry_price"]
                            qty = pos["qty"]
                            pnl = (c - entry_px) * qty if pos["side"] == "BUY" else (entry_px - c) * qty
                            pnl_pct = ((c / entry_px) - 1) * 100 if pos["side"] == "BUY" else ((entry_px / c) - 1) * 100
                            self.positions[sym][strategy_id]["pnl"] = pnl
                            self.positions[sym][strategy_id]["pnl_pct"] = pnl_pct

                    # === ENTRY LOGIC (NEW POSITIONS) ===
                    allow_entry = True
                    
                    if not multi_mode and lunch_skip:
                        ts_east = bar_ts.astimezone(east)
                        if ts_east.hour == 12:
                            allow_entry = False

                    if allow_entry and not paused:
                        if multi_mode:
                            # Multi-strategy mode: check each slot
                            for s, strat in slot_strats:
                                strategy_id = f"{s.name}_P{s.priority}"
                                
                                # Skip if this strategy already has a position on this symbol
                                if sym in positions and strategy_id in positions[sym]:
                                    continue
                                
                                if not _in_window_east(bar_ts, s.start_hhmm, s.end_hhmm):
                                    continue
                                
                                try:
                                    sig = strat.on_bar(sym, bar_obj, self.state)
                                except Exception as e:
                                    log.debug(f"on_bar {s.name} error: {e}")
                                    sig = None
                                
                                if sig and sig.type in (SignalType.BUY, SignalType.SELL):
                                    # Per-slot lunch skip check
                                    eff_lunch = s.lunch_skip if s.lunch_skip is not None else False
                                    if eff_lunch:
                                        ts_east = bar_ts.astimezone(east)
                                        if ts_east.hour == 12:
                                            continue
                                    
                                    # Use slot's parameters
                                    eff_risk = s.risk_percent if s.risk_percent is not None else 1.0
                                    eff_sl = s.sl_percent if s.sl_percent is not None else 1.0
                                    eff_tp = s.tp_percent if s.tp_percent is not None else 2.0

                                    risk_fraction = float(eff_risk) / 100.0
                                    sl_fraction = float(eff_sl) / 100.0
                                    tp_fraction = float(eff_tp) / 100.0

                                    if sl_fraction > 0:
                                        try:
                                            equity = self._adapter.get_account_equity()
                                            qty = max(1, int((risk_fraction * equity) / (c * sl_fraction)))
                                            side = "buy" if sig.type == SignalType.BUY else "sell"
                                            self._adapter.submit_market_order(sym, qty, side)
                                            
                                            if side == "buy":
                                                sl = c * (1 - sl_fraction); tp = c * (1 + tp_fraction)
                                            else:
                                                sl = c * (1 + sl_fraction); tp = c * (1 - tp_fraction)
                                            
                                            # Store position with strategy tracking
                                            if sym not in positions:
                                                positions[sym] = {}
                                            positions[sym][strategy_id] = {
                                                "side": side.upper(),
                                                "qty": qty,
                                                "entry_price": c,
                                                "sl": sl,
                                                "tp": tp,
                                                "strategy_name": s.name,
                                                "priority": s.priority,
                                                "strategy_obj": strat  # Store strategy instance for exit calls
                                            }
                                            
                                            # UI positions tracking
                                            if sym not in self.positions:
                                                self.positions[sym] = {}
                                            self.positions[sym][strategy_id] = {
                                                "symbol": sym,
                                                "side": side.upper(),
                                                "entry_time": bar_ts.strftime("%H:%M:%S"),
                                                "entry_price": c,
                                                "current_price": c,
                                                "qty": qty,
                                                "pnl": 0.0,
                                                "pnl_pct": 0.0,
                                                "stop_loss": sl,
                                                "take_profit": tp,
                                                "slot": s.name,
                                                "priority": s.priority,
                                                "strategy_id": strategy_id
                                            }
                                            
                                            self.recent_trades.insert(0, {
                                                "time": bar_ts.strftime("%H:%M:%S"),
                                                "action": "OPEN",
                                                "symbol": sym,
                                                "price": f"${c:.2f}",
                                                "qty": qty,
                                                "reason": f"{s.name}[P{s.priority}]"
                                            })
                                            if len(self.recent_trades) > 15:
                                                self.recent_trades.pop()
                                            
                                            # Enhanced logging
                                            slot_info = {
                                                "name": s.name,
                                                "priority": s.priority,
                                                "timeframe": s.timeframe,
                                                "start_hhmm": s.start_hhmm,
                                                "end_hhmm": s.end_hhmm,
                                                "lunch_skip": s.lunch_skip if s.lunch_skip is not None else False,
                                            }
                                            settings_info = {
                                                "risk_percent": eff_risk,
                                                "sl_percent": eff_sl,
                                                "tp_percent": eff_tp,
                                            }
                                            self._log_trade_entry(sym, side, qty, c, sl, tp, 
                                                                 s.name, slot_info, settings_info)
                                            
                                            break  # Only one entry per symbol per priority pass
                                        
                                        except Exception as e:
                                            log.exception(f"Entry order failed: {e}")
                        
                        else:
                            # Single-strategy mode
                            strategy_id = self.settings.selected_strategy
                            
                            # Skip if already in position
                            if sym in positions and strategy_id in positions[sym]:
                                pass
                            else:
                                sig = strategy.on_bar(sym, bar_obj, self.state)
                                if sig and sig.type in (SignalType.BUY, SignalType.SELL):
                                    if sl_pct > 0:
                                        try:
                                            equity = self._adapter.get_account_equity()
                                            qty = max(1, int((risk_pct * equity) / (c * sl_pct)))
                                            side = "buy" if sig.type == SignalType.BUY else "sell"
                                            self._adapter.submit_market_order(sym, qty, side)
                                            
                                            if side == "buy":
                                                sl = c * (1 - sl_pct); tp = c * (1 + tp_pct)
                                            else:
                                                sl = c * (1 + sl_pct); tp = c * (1 - tp_pct)
                                            
                                            if sym not in positions:
                                                positions[sym] = {}
                                            positions[sym][strategy_id] = {
                                                "side": side.upper(),
                                                "qty": qty,
                                                "entry_price": c,
                                                "sl": sl,
                                                "tp": tp,
                                                "strategy_name": strategy_id,
                                                "strategy_obj": strategy
                                            }
                                            
                                            if sym not in self.positions:
                                                self.positions[sym] = {}
                                            self.positions[sym][strategy_id] = {
                                                "symbol": sym,
                                                "side": side.upper(),
                                                "entry_time": bar_ts.strftime("%H:%M:%S"),
                                                "entry_price": c,
                                                "current_price": c,
                                                "qty": qty,
                                                "pnl": 0.0,
                                                "pnl_pct": 0.0,
                                                "stop_loss": sl,
                                                "take_profit": tp,
                                                "strategy_id": strategy_id
                                            }
                                            
                                            self.recent_trades.insert(0, {
                                                "time": bar_ts.strftime("%H:%M:%S"),
                                                "action": "OPEN",
                                                "symbol": sym,
                                                "price": f"${c:.2f}",
                                                "qty": qty,
                                                "reason": "Strategy Signal"
                                            })
                                            if len(self.recent_trades) > 15:
                                                self.recent_trades.pop()
                                            
                                            settings_info = {
                                                "risk_percent": self.settings.risk_percent,
                                                "sl_percent": self.settings.stop_loss_percent,
                                                "tp_percent": self.settings.take_profit_percent,
                                            }
                                            self._log_trade_entry(sym, side, qty, c, sl, tp,
                                                                 self.settings.selected_strategy, None, settings_info)
                                        
                                        except Exception as e:
                                            log.exception(f"Entry order failed: {e}")

            # Update P&L periodically
            now_ts = _time.time()
            if now_ts - last_pl_update_ts >= 2.0:
                try:
                    self.state.realized_pnl = self._adapter.get_today_pnl()
                except Exception:
                    pass
                try:
                    self.state.unrealized_pnl = self._adapter.get_unrealized_pl_sum()
                except Exception:
                    pass
                self.state.last_pl_update = datetime.utcnow()
                last_pl_update_ts = now_ts

        try:
            if self._polygon_stream:
                self._polygon_stream.stop()
        except Exception:
            pass

        if not multi_mode and strategy:
            strategy.on_stop(self.state)
        else:
            for _, strat in slot_strats:
                try:
                    strat.on_stop(self.state)
                except Exception:
                    pass

    def _run_backtest(self) -> None:
        symbols = [s.strip().upper() for s in self.settings.symbols.split(",") if s.strip()]
        tf = self.settings.timeframe
        strat_cls = STRATEGIES.get(self.settings.selected_strategy)
        if not strat_cls:
            log.error("Strategy not found: %s", self.settings.selected_strategy)
            return
        strategy: StrategyBase = strat_cls()

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = Path("backtests") / ts
        run_dir.mkdir(parents=True, exist_ok=True)
        self.state.run_folder = run_dir

        backtest_logger = logging.getLogger("backtest")
        fh = logging.FileHandler(run_dir / "backtest.log", encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S"))
        backtest_logger.addHandler(fh)
        backtest_logger.setLevel(logging.INFO)

        from .config_store import load_polygon_key
        polygon_key = load_polygon_key()

        if not polygon_key:
            log.error("Polygon API key not found. Please set it in Settings.")
            return

        polygon_adapter = PolygonAdapter(polygon_key)
        register_polygon_adapter(polygon_adapter)
        log.info("Polygon adapter initialized for backtest data")

        end = self.settings.backtest_end_date
        start = self.settings.backtest_start_date
        
        if end and end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        if start and start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        
        if not end:
            end = datetime.now(timezone.utc)
        if not start:
            start = end - timedelta(days=730)
        
        log.info(f"Backtest date range: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')} ({(end-start).days} days)")
        
        if start >= end:
            log.error("Invalid date range: start date must be before end date")
            return
        
        days_diff = (end - start).days
        if days_diff > 730 and self.settings.backtest_source.value == "polygon":
            log.warning("Date range exceeds 730 days (%d days). Polygon free tier is limited to 2 years of data.", days_diff)

        bars_cache = {}
        def loader(sym: str):
            key = (sym, tf, start, end)
            if key not in bars_cache:
                bars_cache[key] = load_bars(sym, tf, start, end)
            return bars_cache[key]

        try:
            stats = run_backtest(symbols, tf, strategy, vars(self.settings), loader, run_dir, self._adapter)
            self.state.stats = stats
            self.state.realized_pnl = stats.get("total_pnl", 0.0)
            self.state.unrealized_pnl = 0.0
            self.state.last_pl_update = datetime.utcnow()
        finally:
            backtest_logger.removeHandler(fh)
            fh.close()
