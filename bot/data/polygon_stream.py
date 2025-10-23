"""
Polygon.io WebSocket streaming for live market data
Replaces Alpaca's StockDataStream
"""
from __future__ import annotations
import logging
import json
import threading
import time
from typing import Iterable, Optional, Callable
import websocket

log = logging.getLogger(__name__)

class PolygonStream:
    """
    Polygon WebSocket stream for real-time bars
    Free tier: Real-time data with some delay
    """
    
    def __init__(self, api_key: str, symbols: Iterable[str], 
                 on_bar: Optional[Callable] = None):
        self.api_key = api_key
        self.symbols = list(symbols)
        self.on_bar = on_bar
        self._ws = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._authenticated = False
        
        # Polygon WebSocket URL
        self.ws_url = f"wss://socket.polygon.io/stocks"
    
    def start(self):
        """Start the WebSocket connection"""
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="polygon-stream", daemon=True)
        self._thread.start()
        log.info("Polygon stream started")
    
    def stop(self):
        """Stop the WebSocket connection"""
        self._stop_event.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        log.info("Polygon stream stopped")
    
    def _run(self):
        """Main WebSocket loop with reconnection"""
        while not self._stop_event.is_set():
            try:
                self._connect()
            except Exception as e:
                log.error(f"Polygon stream error: {e}")
            
            if not self._stop_event.is_set():
                log.info("Reconnecting to Polygon in 5 seconds...")
                self._stop_event.wait(5.0)
    
    def _connect(self):
        """Establish WebSocket connection"""
        self._authenticated = False
        
        self._ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        
        # Run the WebSocket (blocking)
        self._ws.run_forever()
    
    def _on_open(self, ws):
        """Handle WebSocket connection opened"""
        log.info("Polygon WebSocket connected")
        
        # Authenticate
        auth_msg = {
            "action": "auth",
            "params": self.api_key
        }
        ws.send(json.dumps(auth_msg))
    
    def _on_message(self, ws, message):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)
            
            # Handle array of messages
            if isinstance(data, list):
                for msg in data:
                    self._process_message(msg)
            else:
                self._process_message(data)
                
        except Exception as e:
            log.debug(f"Failed to process message: {e}")
    
    def _process_message(self, msg: dict):
        """Process individual message"""
        ev = msg.get('ev')
        
        # Handle authentication response
        if ev == 'status':
            status = msg.get('status')
            if status == 'auth_success':
                self._authenticated = True
                log.info("Polygon authentication successful")
                self._subscribe_symbols()
            elif status == 'auth_failed':
                log.error("Polygon authentication failed")
                self._stop_event.set()
            return
        
        # Handle aggregate bars (AM = minute aggregates)
        if ev == 'AM' and self._authenticated:
            try:
                symbol = msg.get('sym')
                if symbol not in self.symbols:
                    return
                
                # Extract bar data
                bar_data = {
                    't': msg.get('s'),  # Start timestamp (ms)
                    'o': msg.get('o'),  # Open
                    'h': msg.get('h'),  # High
                    'l': msg.get('l'),  # Low
                    'c': msg.get('c'),  # Close
                    'v': msg.get('v', 0)  # Volume
                }
                
                if self.on_bar:
                    self.on_bar(symbol, bar_data)
                    
            except Exception as e:
                log.debug(f"Failed to process bar: {e}")
    
    def _subscribe_symbols(self):
        """Subscribe to minute bars for all symbols"""
        if not self._authenticated:
            log.warning("Cannot subscribe - not authenticated")
            return
        
        # Subscribe to minute aggregates (AM)
        for symbol in self.symbols:
            sub_msg = {
                "action": "subscribe",
                "params": f"AM.{symbol}"
            }
            self._ws.send(json.dumps(sub_msg))
        
        log.info(f"Subscribed to Polygon minute bars: {', '.join(self.symbols)}")
    
    def _on_error(self, ws, error):
        """Handle WebSocket error"""
        log.error(f"Polygon WebSocket error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket closed"""
        log.info(f"Polygon WebSocket closed: {close_status_code} - {close_msg}")
        self._authenticated = False
