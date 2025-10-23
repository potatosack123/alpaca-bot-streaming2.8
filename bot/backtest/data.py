"""
Backtest data loader - POLYGON ONLY
All Yahoo and Alpaca data sources removed
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd

from ..state import Bar

log = logging.getLogger(__name__)

# Global Polygon adapter (set by controller)
_POLYGON_ADAPTER = None

def register_polygon_adapter(adapter) -> None:
    """Register the Polygon adapter for data loading"""
    global _POLYGON_ADAPTER
    _POLYGON_ADAPTER = adapter
    log.info("Polygon adapter registered for backtesting")

def _csv_path(symbol: str, tf: str) -> Path:
    """Get path to CSV file (fallback only)"""
    return Path("data") / f"{symbol.upper()}_{tf}.csv"

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize CSV DataFrame to standard format"""
    if df.empty:
        return df
    
    # Rename columns to title case
    rename = {}
    cols_lower = {c.lower(): c for c in df.columns}
    for want in ["open", "high", "low", "close", "volume"]:
        if want in cols_lower and cols_lower[want] != want.title():
            rename[cols_lower[want]] = want.title()
    df = df.rename(columns=rename)
    
    # Handle timestamp/index
    if "timestamp" in cols_lower:
        ts_col = cols_lower["timestamp"]
        ts = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
        mask = ts.notna()
        df = df.loc[mask].copy()
        df.index = ts[mask]
        df.drop(columns=[ts_col], inplace=True, errors="ignore")
    elif isinstance(df.index, pd.DatetimeIndex):
        if df.index.tz is None:
            df.index = pd.to_datetime(df.index, utc=True)
        else:
            df.index = df.index.tz_convert("UTC")
        mask = df.index.notna()
        df = df.loc[mask].copy()
    else:
        return df.iloc[0:0]
    
    # Validate required columns
    for col in ["Open", "High", "Low", "Close"]:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
    
    if "Volume" not in df.columns:
        df["Volume"] = 0
    
    return df[["Open", "High", "Low", "Close", "Volume"]]

def _bars_from_df(df: pd.DataFrame) -> List[Bar]:
    """Convert DataFrame to Bar objects"""
    if df.empty:
        return []
    
    bars: List[Bar] = []
    for ts, row in df.iterrows():
        if pd.isna(ts):
            continue
        bars.append(Bar(
            timestamp=ts.to_pydatetime(),
            open=float(row["Open"]),
            high=float(row["High"]),
            low=float(row["Low"]),
            close=float(row["Close"]),
            volume=int(row["Volume"])
        ))
    return bars

def _read_csv(symbol: str, tf: str) -> List[Bar]:
    """Read bars from local CSV file (fallback only)"""
    p = _csv_path(symbol, tf)
    if not p.exists():
        log.warning(f"CSV not found for {symbol} at {p}")
        return []
    
    try:
        df = pd.read_csv(p)
        df = _normalize_df(df)
        bars = _bars_from_df(df)
        log.info(f"Loaded {len(bars)} bars for {symbol} from CSV")
        return bars
    except Exception as e:
        log.warning(f"Failed to read CSV for {symbol}: {e}")
        return []

def _coerce_utc(dt: datetime) -> datetime:
    """Ensure datetime is UTC"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def load_bars(symbol: str, tf: str, start: datetime, end: datetime) -> List[Bar]:
    """
    Load historical bars for backtesting - POLYGON ONLY
    
    Data source priority:
    1. Polygon API (via registered adapter)
    2. CSV fallback (data/ folder)
    
    Args:
        symbol: Stock ticker
        tf: Timeframe (1m, 3m, 5m)
        start: Start datetime (UTC)
        end: End datetime (UTC)
    
    Returns:
        List of Bar objects
    """
    # Ensure UTC and clamp to avoid real-time data
    now = datetime.now(timezone.utc)
    end_eff = _coerce_utc(end)
    start_eff = _coerce_utc(start)
    
    # Clamp end to 20 minutes ago to avoid incomplete bars
    cutoff = now - timedelta(minutes=20)
    if end_eff > cutoff:
        end_eff = cutoff
    
    # Validate range
    if end_eff <= start_eff:
        log.warning(f"Invalid date range after clamping: {start_eff} to {end_eff}")
        return []
    
    # Warn about 2-year Polygon limit
    two_years_ago = now - timedelta(days=730)
    if start_eff < two_years_ago:
        log.warning(f"Start date {start_eff.date()} exceeds Polygon 2-year limit. Data may be incomplete.")
    
    # Try Polygon first
    if _POLYGON_ADAPTER is not None:
        try:
            log.info(f"Loading {symbol} bars from Polygon ({start_eff.date()} to {end_eff.date()})")
            bars = _POLYGON_ADAPTER.historical_bars(symbol, tf, start_eff, end_eff)
            
            if bars:
                log.info(f"Successfully loaded {len(bars)} bars for {symbol} from Polygon")
                return bars
            else:
                log.warning(f"Polygon returned 0 bars for {symbol}")
        except Exception as e:
            log.error(f"Polygon data load failed for {symbol}: {e}")
    else:
        log.warning("No Polygon adapter registered. Set POLYGON_API_KEY in settings.")
    
    # Fallback to CSV
    log.info(f"Trying CSV fallback for {symbol}...")
    csv_bars = _read_csv(symbol, tf)
    if csv_bars:
        log.info(f"Loaded {len(csv_bars)} bars for {symbol} from CSV")
        return csv_bars
    
    # No data available
    log.error(f"No data available for {symbol}. Check Polygon API key and CSV files.")
    return []
