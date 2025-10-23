"""
Polygon.io data adapter - handles ALL market data
Alpaca is ONLY for order execution
"""
from __future__ import annotations
import logging
import requests
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta
import time

from ..state import Bar

log = logging.getLogger(__name__)

class PolygonAdapter:
    """Polygon.io data adapter - free tier: 5 API calls/min, 2 years historical"""
    
    BASE_URL = "https://api.polygon.io"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._rate_limit_delay = 12.0  # 5 calls/min = 12s between calls
        self._last_call_time = 0.0
        
    def _wait_for_rate_limit(self):
        """Enforce rate limiting (5 calls/min for free tier)"""
        elapsed = time.time() - self._last_call_time
        if elapsed < self._rate_limit_delay:
            sleep_time = self._rate_limit_delay - elapsed
            log.debug(f"Rate limit: sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)
        self._last_call_time = time.time()
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Optional[Dict]:
        """Make rate-limited API request"""
        self._wait_for_rate_limit()
        
        params['apiKey'] = self.api_key
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                log.error("Polygon rate limit exceeded. Free tier: 5 calls/min")
            else:
                log.error(f"Polygon HTTP error: {e}")
            return None
        except Exception as e:
            log.error(f"Polygon request failed: {e}")
            return None
    
    def latest_trade(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get latest trade for a symbol"""
        endpoint = f"/v2/last/trade/{symbol}"
        data = self._make_request(endpoint, {})
        
        if not data or 'results' not in data:
            return None
        
        result = data['results']
        try:
            # Polygon timestamps are in nanoseconds
            ts_ns = result.get('t', 0)
            ts = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
            price = float(result.get('p', 0.0))
            
            return {"t": ts, "p": price}
        except Exception as e:
            log.debug(f"Failed to parse latest trade: {e}")
            return None
    
    def latest_bar(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get latest bar (1-minute) for a symbol"""
        # Use previous day endpoint for most recent bar
        endpoint = f"/v2/aggs/ticker/{symbol}/prev"
        data = self._make_request(endpoint, {})
        
        if not data or 'results' not in data or not data['results']:
            return None
        
        bar = data['results'][0]
        try:
            ts_ms = bar.get('t', 0)
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            
            return {
                "t": ts,
                "o": float(bar.get('o', 0.0)),
                "h": float(bar.get('h', 0.0)),
                "l": float(bar.get('l', 0.0)),
                "c": float(bar.get('c', 0.0)),
                "v": int(bar.get('v', 0))
            }
        except Exception as e:
            log.debug(f"Failed to parse latest bar: {e}")
            return None
    
    def historical_bars(self, symbol: str, timeframe: str, 
                       start: datetime, end: datetime) -> List[Bar]:
        """
        Get historical bars from Polygon
        
        Timeframe mapping:
        - 1m, 3m, 5m -> minute bars
        
        Free tier limits:
        - 2 years historical data
        - 5 API calls/minute
        - 50,000 data points per request
        """
        bars: List[Bar] = []
        
        # Map timeframe to Polygon format
        if timeframe == "1m":
            multiplier, timespan = 1, "minute"
        elif timeframe == "3m":
            multiplier, timespan = 3, "minute"
        elif timeframe == "5m":
            multiplier, timespan = 5, "minute"
        else:
            log.error(f"Unsupported timeframe: {timeframe}")
            return bars
        
        # Polygon expects milliseconds
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        
        # Check 2-year limit
        two_years_ago = datetime.now(timezone.utc) - timedelta(days=730)
        if start < two_years_ago:
            log.warning(f"Start date {start} exceeds Polygon free tier 2-year limit. Adjusting to {two_years_ago}")
            start = two_years_ago
            start_ms = int(start.timestamp() * 1000)
        
        endpoint = f"/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{start_ms}/{end_ms}"
        
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000  # Max per request
        }
        
        log.info(f"Fetching Polygon data for {symbol}: {start.date()} to {end.date()} ({timeframe})")
        
        data = self._make_request(endpoint, params)
        
        if not data:
            log.warning(f"No data returned from Polygon for {symbol}")
            return bars
        
        if data.get('status') != 'OK':
            log.warning(f"Polygon status: {data.get('status')} - {data.get('error', 'Unknown error')}")
            return bars
        
        results = data.get('results', [])
        if not results:
            log.warning(f"No results in Polygon response for {symbol}")
            return bars
        
        for bar_data in results:
            try:
                # Polygon timestamps are in milliseconds
                ts_ms = bar_data.get('t', 0)
                ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                
                bars.append(Bar(
                    timestamp=ts,
                    open=float(bar_data.get('o', 0.0)),
                    high=float(bar_data.get('h', 0.0)),
                    low=float(bar_data.get('l', 0.0)),
                    close=float(bar_data.get('c', 0.0)),
                    volume=int(bar_data.get('v', 0))
                ))
            except Exception as e:
                log.debug(f"Failed to parse bar: {e}")
                continue
        
        log.info(f"Loaded {len(bars)} bars for {symbol} from Polygon")
        return bars
