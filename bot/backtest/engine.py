from __future__ import annotations
import logging
from typing import List, Dict, Any, Callable, Optional
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass

import pytz
import pandas as pd

from ..state import Bar, SessionState, SignalType, RunMode

log = logging.getLogger(__name__)

@dataclass
class Position:
    """Active position tracker"""
    symbol: str
    side: str  # ADDED: 'long' or 'short'
    entry_time: datetime
    entry_price: float
    shares: int
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None

@dataclass  
class Trade:
    """Completed trade record"""
    symbol: str
    side: str  # ADDED: 'long' or 'short'
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    shares: int
    pnl: float
    pnl_pct: float
    exit_reason: str  # 'signal', 'stop_loss', 'take_profit'


def extract_gag_analytics(strategy, symbol: str, position: Position, pnl: float) -> Dict[str, Any]:
    """
    Extract Gap-and-Go v2 analytics from strategy state
    Returns dict with extended fields (empty strings if not available)
    Backwards compatible - returns empty dict for non-GAG strategies
    """
    analytics = {
        'prev_close': '',
        'gap_pct': '',
        'premarket_high': '',
        'premarket_low': '',
        'premarket_volume': '',
        'atr_on_entry': '',
        'initial_stop': '',
        'r_value': '',
        'breakeven_lock_time': '',
        'r_multiple': '',
        'vwap_exit': False,
        'time_exit': False,
        'strategy_exit': False,
    }
    
    # Check if this is a Gap-and-Go v2 strategy
    if not hasattr(strategy, 'prev_close'):
        return analytics
    
    try:
        # Extract data from strategy state
        prev_close = strategy.prev_close.get(symbol)
        pm_high = strategy.premarket_high.get(symbol)
        pm_low = strategy.premarket_low.get(symbol)
        pm_vol = strategy.premarket_volume.get(symbol, 0)
        atr = strategy.atr.get(symbol)
        initial_stop = strategy.initial_stop.get(symbol)
        r_value = strategy.r_value.get(symbol)
        be_lock_time = strategy.breakeven_lock_time.get(symbol)
        
        # Populate analytics
        if prev_close:
            analytics['prev_close'] = round(prev_close, 2)
            
            # Calculate gap %
            if prev_close > 0:
                gap_pct = ((position.entry_price - prev_close) / prev_close) * 100
                analytics['gap_pct'] = round(gap_pct, 2)
        
        if pm_high and pm_high != float('-inf'):
            analytics['premarket_high'] = round(pm_high, 2)
        
        if pm_low and pm_low != float('inf'):
            analytics['premarket_low'] = round(pm_low, 2)
        
        if pm_vol > 0:
            analytics['premarket_volume'] = pm_vol
        
        if atr:
            analytics['atr_on_entry'] = round(atr, 4)
        
        if initial_stop:
            analytics['initial_stop'] = round(initial_stop, 2)
        
        if r_value:
            analytics['r_value'] = round(r_value, 2)
            
            # Calculate R-multiple
            if r_value > 0:
                analytics['r_multiple'] = round(pnl / r_value, 2)
        
        if be_lock_time:
            analytics['breakeven_lock_time'] = be_lock_time.isoformat() if hasattr(be_lock_time, 'isoformat') else str(be_lock_time)
    
    except Exception as e:
        log.debug(f"Error extracting GAG analytics for {symbol}: {e}")
    
    return analytics


def run_backtest(
    symbols: List[str], 
    tf: str, 
    strategy, 
    settings: Dict[str, Any],
    loader: Callable[[str], List[Bar]], 
    run_dir: Path,
    adapter=None
) -> Dict[str, Any]:
    """
    Run backtest with Gap-and-Go v2 analytics support and SHORT position support
    """
    trades: List[Trade] = []
    equity_records = []
    east = pytz.timezone("US/Eastern")
    
    # Counters for progress logging
    trade_counter = 0
    bar_counter = 0
    
    # Initialize account
    starting_cash = 100000.0
    if adapter:
        try:
            starting_cash = adapter.get_account_equity()
            log.info(f"Starting backtest with account balance: ${starting_cash:,.2f}")
        except Exception as e:
            log.warning(f"Could not fetch account equity: {e}. Using $100k default.")
    cash = starting_cash
    positions: Dict[str, Position] = {}
    
    # ADDED: Track last bar timestamp for proper end-of-backtest cleanup
    last_bar_timestamp = None
    
    # Create session state for strategy
    session_state = SessionState(
        run_mode=RunMode.BACKTEST,
        started=True,
        paused=False,
        should_stop=False
    )
    
    # Strategy initialization
    try:
        strategy.on_start(session_state)
        log.info("Strategy initialized: %s", type(strategy).__name__)
    except Exception as e:
        log.warning("Strategy on_start failed: %s", e)
    
    # Extract settings
    risk_pct = settings.get("risk_percent", 1.0) / 100.0
    sl_pct = settings.get("stop_loss_percent", 1.0) / 100.0
    tp_pct = settings.get("take_profit_percent", 2.0) / 100.0
    
    log.info("Backtest settings: risk=%.2f%%, SL=%.2f%%, TP=%.2f%%", 
             risk_pct*100, sl_pct*100, tp_pct*100)

    for sym in symbols:
        bars = loader(sym)
        bars = [b for b in bars if getattr(b, "timestamp", None) is not None]
        if not bars:
            log.info("No bars returned for %s; skipping.", sym)
            continue
        
        log.info("Processing %d bars for %s", len(bars), sym)

        for bar in bars:
            bar_counter += 1
            
            # Progress indicator every 1000 bars
            if bar_counter % 1000 == 0:
                log.info("Progress: %d bars processed, %d trades, equity=$%.2f", 
                        bar_counter, trade_counter, cash + sum(p.shares * bar.close for p in positions.values()))
            
            ts = bar.timestamp
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            ts_east = ts.astimezone(east)
            
            # ADDED: Track last bar timestamp
            last_bar_timestamp = ts
            
            # FIXED: Check existing position for stop-loss / take-profit (with SHORT support)
            if sym in positions:
                pos = positions[sym]
                
                # FIXED: Different checks for long vs short
                if pos.side == 'long':
                    hit_sl = pos.stop_loss and bar.low <= pos.stop_loss
                    hit_tp = pos.take_profit and bar.high >= pos.take_profit
                else:  # short
                    hit_sl = pos.stop_loss and bar.high >= pos.stop_loss
                    hit_tp = pos.take_profit and bar.low <= pos.take_profit
                
                if hit_sl:
                    exit_price = pos.stop_loss
                    
                    # FIXED: P&L calculation for shorts
                    if pos.side == 'long':
                        pnl = (exit_price - pos.entry_price) * pos.shares
                    else:  # short
                        pnl = (pos.entry_price - exit_price) * pos.shares
                    
                    pnl_pct = (pnl / (pos.entry_price * pos.shares)) * 100
                    
                    # ✅ FIXED: Correct cash flow for longs vs shorts
                    if pos.side == 'short':
                        cash -= exit_price * pos.shares  # Pay to buy back shares
                    else:
                        cash += exit_price * pos.shares  # Receive cash from closing long
                    
                    hold_time = ts - pos.entry_time
                    hold_minutes = hold_time.total_seconds() / 60
                    
                    trades.append(Trade(
                        symbol=sym,
                        side=pos.side,
                        entry_time=pos.entry_time,
                        exit_time=ts,
                        entry_price=pos.entry_price,
                        exit_price=exit_price,
                        shares=pos.shares,
                        pnl=pnl,
                        pnl_pct=pnl_pct,
                        exit_reason="stop_loss"
                    ))
                    
                    trade_counter += 1
                    if trade_counter % 10 == 0 or pnl < -100:
                        log.debug("Trade #%d: STOP_LOSS %s %s | pnl=$%.2f (%+.2f%%) | hold=%.1fmin", 
                                 trade_counter, pos.side.upper(), sym, pnl, pnl_pct, hold_minutes)
                    
                    del positions[sym]
                    
                elif hit_tp:
                    exit_price = pos.take_profit
                    
                    # FIXED: P&L calculation for shorts
                    if pos.side == 'long':
                        pnl = (exit_price - pos.entry_price) * pos.shares
                    else:  # short
                        pnl = (pos.entry_price - exit_price) * pos.shares
                    
                    pnl_pct = (pnl / (pos.entry_price * pos.shares)) * 100
                    
                    # ✅ FIXED: Correct cash flow for longs vs shorts
                    if pos.side == 'short':
                        cash -= exit_price * pos.shares  # Pay to buy back shares
                    else:
                        cash += exit_price * pos.shares  # Receive cash from closing long
                    
                    pnl_pct = (pnl / (pos.entry_price * pos.shares)) * 100
                    cash += exit_price * pos.shares
                    
                    hold_time = ts - pos.entry_time
                    hold_minutes = hold_time.total_seconds() / 60
                    
                    trades.append(Trade(
                        symbol=sym,
                        side=pos.side,
                        entry_time=pos.entry_time,
                        exit_time=ts,
                        entry_price=pos.entry_price,
                        exit_price=exit_price,
                        shares=pos.shares,
                        pnl=pnl,
                        pnl_pct=pnl_pct,
                        exit_reason="take_profit"
                    ))
                    
                    trade_counter += 1
                    if trade_counter % 10 == 0 or pnl > 100:
                        log.debug("Trade #%d: TAKE_PROFIT %s %s | pnl=$%.2f (%+.2f%%) | hold=%.1fmin", 
                                 trade_counter, pos.side.upper(), sym, pnl, pnl_pct, hold_minutes)
                    
                    del positions[sym]
            
            # Get strategy signal
            try:
                signal = strategy.on_bar(sym, bar, session_state)
            except Exception as e:
                log.warning("Strategy error on %s at %s: %s", sym, ts, e)
                signal = None
            
            # FIXED: Process signal with SHORT support
            if signal and signal.type == SignalType.BUY:
                # BUY can mean: open long OR close short
                if sym in positions:
                    # Close existing SHORT position
                    pos = positions[sym]
                    if pos.side == 'short':
                        exit_price = bar.close
                        pnl = (pos.entry_price - exit_price) * pos.shares
                        pnl_pct = (pnl / (pos.entry_price * pos.shares)) * 100
                        cash -= exit_price * pos.shares  # ✅ FIXED: Pay to buy back shares
                        
                        hold_time = ts - pos.entry_time
                        hold_minutes = hold_time.total_seconds() / 60
                        
                        trades.append(Trade(
                            symbol=sym,
                            side='short',
                            entry_time=pos.entry_time,
                            exit_time=ts,
                            entry_price=pos.entry_price,
                            exit_price=exit_price,
                            shares=pos.shares,
                            pnl=pnl,
                            pnl_pct=pnl_pct,
                            exit_reason="signal"
                        ))
                        
                        trade_counter += 1
                        if trade_counter % 10 == 0:
                            log.debug("Trade #%d: SIGNAL_EXIT SHORT %s | pnl=$%.2f (%+.2f%%)", 
                                     trade_counter, sym, pnl, pnl_pct)
                        
                        del positions[sym]
                else:
                    # Open new LONG position
                    current_equity = cash + sum(
                        p.shares * bar.close for p in positions.values()
                    )
                    position_value = current_equity * risk_pct
                    shares = int(position_value / bar.close)
                    
                    if shares > 0 and (shares * bar.close) <= cash:
                        cash -= shares * bar.close
                        
                        stop_loss = bar.close * (1 - (signal.sl_pct or sl_pct))
                        take_profit = bar.close * (1 + (signal.tp_pct or tp_pct))
                        
                        positions[sym] = Position(
                            symbol=sym,
                            side='long',
                            entry_time=ts,
                            entry_price=bar.close,
                            shares=shares,
                            stop_loss=stop_loss,
                            take_profit=take_profit
                        )
                        
                        if len(positions) % 5 == 1 or shares * bar.close > current_equity * 0.05:
                            log.debug("ENTRY: BUY LONG %s | qty=%d | price=$%.2f | positions=%d", 
                                     sym, shares, bar.close, len(positions))
            
            elif signal and signal.type == SignalType.SELL:
                # SELL can mean: close long OR open short
                if sym in positions:
                    # Close existing LONG position
                    pos = positions[sym]
                    if pos.side == 'long':
                        exit_price = bar.close
                        pnl = (exit_price - pos.entry_price) * pos.shares
                        pnl_pct = (pnl / (pos.entry_price * pos.shares)) * 100
                        cash += exit_price * pos.shares
                        
                        hold_time = ts - pos.entry_time
                        hold_minutes = hold_time.total_seconds() / 60
                        
                        trades.append(Trade(
                            symbol=sym,
                            side='long',
                            entry_time=pos.entry_time,
                            exit_time=ts,
                            entry_price=pos.entry_price,
                            exit_price=exit_price,
                            shares=pos.shares,
                            pnl=pnl,
                            pnl_pct=pnl_pct,
                            exit_reason="signal"
                        ))
                        
                        trade_counter += 1
                        if trade_counter % 10 == 0:
                            log.debug("Trade #%d: SIGNAL_EXIT LONG %s | pnl=$%.2f (%+.2f%%)", 
                                     trade_counter, sym, pnl, pnl_pct)
                        
                        del positions[sym]
                else:
                    # Open new SHORT position
                    current_equity = cash + sum(
                        p.shares * bar.close for p in positions.values()
                    )
                    position_value = current_equity * risk_pct
                    shares = int(position_value / bar.close)
                    
                    if shares > 0:  # ✅ Removed cash check - shorts generate cash
                        cash += shares * bar.close
                        
                        stop_loss = bar.close * (1 + (signal.sl_pct or sl_pct))  # Above entry for shorts
                        take_profit = bar.close * (1 - (signal.tp_pct or tp_pct))  # Below entry for shorts
                        
                        positions[sym] = Position(
                            symbol=sym,
                            side='short',
                            entry_time=ts,
                            entry_price=bar.close,
                            shares=shares,
                            stop_loss=stop_loss,
                            take_profit=take_profit
                        )
                        
                        if len(positions) % 5 == 1 or shares * bar.close > current_equity * 0.05:
                            log.debug("ENTRY: SELL SHORT %s | qty=%d | price=$%.2f | positions=%d", 
                                     sym, shares, bar.close, len(positions))
            
            # Calculate current equity (do this less frequently)
            if bar_counter % 100 == 0:
                position_value = sum(p.shares * bar.close for p in positions.values())
                current_equity = cash + position_value
                
                equity_records.append({
                    "timestamp": ts,
                    "equity": current_equity,
                    "cash": cash,
                    "positions_value": position_value
                })
    
    # FIXED: Close any remaining positions using LAST BAR timestamp
    if last_bar_timestamp is None:
        last_bar_timestamp = datetime.now(timezone.utc)
    
    for sym, pos in list(positions.items()):
        exit_price = pos.entry_price
        pnl = 0.0
        cash += exit_price * pos.shares
        
        trades.append(Trade(
            symbol=sym,
            side=pos.side,
            entry_time=pos.entry_time,
            exit_time=last_bar_timestamp,  # FIXED: Use last bar timestamp
            entry_price=pos.entry_price,
            exit_price=exit_price,
            shares=pos.shares,
            pnl=pnl,
            pnl_pct=0.0,
            exit_reason="end_of_backtest"
        ))
        log.info("Closed remaining %s position in %s", pos.side.upper(), sym)
    
    # Strategy cleanup
    try:
        strategy.on_stop(session_state)
    except Exception as e:
        log.warning("Strategy on_stop failed: %s", e)
    
    # Save artifacts with enhanced data
    eq_df = pd.DataFrame(equity_records)
    if not eq_df.empty:
        eq_df.to_csv(run_dir / "equity.csv", index=False)
        log.info("Saved equity curve to %s", run_dir / "equity.csv")
    
    # === ENHANCED TRADES CSV WITH GAP-AND-GO V2 ANALYTICS ===
    trades_data = []
    for t in trades:
        # Get position that was closed (reconstruct for analytics)
        pos = Position(
            symbol=t.symbol,
            side=t.side,
            entry_time=t.entry_time,
            entry_price=t.entry_price,
            shares=t.shares
        )
        
        # Extract Gap-and-Go v2 analytics if available
        gag_analytics = extract_gag_analytics(strategy, t.symbol, pos, t.pnl)
        
        # Build complete trade record
        trade_record = {
            # Basic trade info
            "symbol": t.symbol,
            "date": t.entry_time.strftime('%Y-%m-%d'),
            "entry_time": t.entry_time.isoformat(),
            "exit_time": t.exit_time.isoformat(),
            "entry_price": round(t.entry_price, 2),
            "exit_price": round(t.exit_price, 2),
            "side": t.side.upper(),  # FIXED: Use actual side
            "shares": t.shares,
            
            # P&L
            "pnl": round(t.pnl, 2),
            "pnl_pct": round(t.pnl_pct, 2),
            
            # Gap-and-Go v2 analytics (empty if not GAG strategy)
            "prev_close": gag_analytics.get('prev_close', ''),
            "gap_pct": gag_analytics.get('gap_pct', ''),
            "premarket_high": gag_analytics.get('premarket_high', ''),
            "premarket_low": gag_analytics.get('premarket_low', ''),
            "premarket_volume": gag_analytics.get('premarket_volume', ''),
            "atr_on_entry": gag_analytics.get('atr_on_entry', ''),
            "initial_stop": gag_analytics.get('initial_stop', ''),
            "r_value": gag_analytics.get('r_value', ''),
            "r_multiple": gag_analytics.get('r_multiple', ''),
            "breakeven_lock_time": gag_analytics.get('breakeven_lock_time', ''),
            "vwap_exit": 'VWAP' in t.exit_reason.upper(),
            "time_exit": 'TIME' in t.exit_reason.upper(),
            "strategy_exit": 'STRATEGY' in t.exit_reason.upper() or t.exit_reason == 'signal',
            
            # Placeholders for future scanner data
            "spread_on_entry": '',
            "first_minute_vol": '',
            "avg_vol": '',
            
            # Strategy metadata
            "slot_name": '',
            "strategy": type(strategy).__name__,
            "exit_reason": t.exit_reason,
            "timeframe": tf,
            "hold_time_minutes": round((t.exit_time - t.entry_time).total_seconds() / 60, 1),
            
            # Risk parameters
            "risk_pct": settings.get("risk_percent", 0.0),
            "sl_pct": settings.get("stop_loss_percent", 0.0),
            "tp_pct": settings.get("take_profit_percent", 0.0),
        }
        
        trades_data.append(trade_record)
    
    trades_df = pd.DataFrame(trades_data)
    trades_df.to_csv(run_dir / "trades.csv", index=False)
    log.info("Saved %d trades to %s with extended analytics", len(trades), run_dir / "trades.csv")
    
    # Calculate statistics
    winning_trades = [t for t in trades if t.pnl > 0]
    losing_trades = [t for t in trades if t.pnl < 0]
    
    final_equity = cash
    total_return = ((final_equity - starting_cash) / starting_cash) * 100
    
    stats = {
        "trades": len(trades),
        "winners": len(winning_trades),
        "losers": len(losing_trades),
        "win_rate": 0.0 if not trades else (len(winning_trades) / len(trades)) * 100,
        "total_pnl": sum(t.pnl for t in trades),
        "avg_win": sum(t.pnl for t in winning_trades) / len(winning_trades) if winning_trades else 0,
        "avg_loss": sum(t.pnl for t in losing_trades) / len(losing_trades) if losing_trades else 0,
        "largest_win": max((t.pnl for t in trades), default=0),
        "largest_loss": min((t.pnl for t in trades), default=0),
        "starting_equity": starting_cash,
        "final_equity": final_equity,
        "total_return_pct": total_return,
        "profit_factor": abs(sum(t.pnl for t in winning_trades) / sum(t.pnl for t in losing_trades)) if losing_trades else 0
    }
    
    log.info("=" * 60)
    log.info("BACKTEST RESULTS")
    log.info("=" * 60)
    log.info("Total Trades: %d (W: %d, L: %d)", stats["trades"], stats["winners"], stats["losers"])
    log.info("Win Rate: %.2f%%", stats["win_rate"])
    log.info("Total P&L: $%.2f (%.2f%%)", stats["total_pnl"], stats["total_return_pct"])
    log.info("Avg Win: $%.2f | Avg Loss: $%.2f", stats["avg_win"], stats["avg_loss"])
    log.info("Profit Factor: %.2f", stats["profit_factor"])
    log.info("=" * 60)
    
    # Compact summary with key strategy info
    log.info("")
    log.info("Strategy: %s | Timeframe: %s | Risk: %.2f%% | SL: %.2f%% | TP: %.2f%%",
             type(strategy).__name__, tf,
             settings.get("risk_percent", 0.0),
             settings.get("stop_loss_percent", 0.0),
             settings.get("take_profit_percent", 0.0))
    
    # Calculate average hold time
    if trades:
        avg_hold = sum((t.exit_time - t.entry_time).total_seconds() / 60 for t in trades) / len(trades)
        log.info("Average hold time: %.1f minutes", avg_hold)
    
    # Show top 5 best and worst trades for quick insight
    if trades:
        sorted_trades = sorted(trades, key=lambda t: t.pnl, reverse=True)
        log.info("")
        log.info("Top 5 Winners:")
        for i, t in enumerate(sorted_trades[:5], 1):
            log.info("  %d. %s %s: $%.2f (%+.2f%%) | %s to %s", 
                    i, t.side.upper(), t.symbol, t.pnl, t.pnl_pct,
                    t.entry_time.strftime("%m/%d %H:%M"),
                    t.exit_time.strftime("%m/%d %H:%M"))
        
        log.info("")
        log.info("Top 5 Losers:")
        for i, t in enumerate(sorted_trades[-5:][::-1], 1):
            log.info("  %d. %s %s: $%.2f (%+.2f%%) | %s to %s",
                    i, t.side.upper(), t.symbol, t.pnl, t.pnl_pct,
                    t.entry_time.strftime("%m/%d %H:%M"),
                    t.exit_time.strftime("%m/%d %H:%M"))
    
    log.info("=" * 60)
    
    return stats
