"""Real implementations of Transport and Scheduler for production use."""

import threading
import time
import requests
from typing import Callable, Dict, Any


class HTTPTransport:
    """
    Transport implementation that uses HTTP requests to send messages.
    Each Raft node runs a Flask server, and we POST messages to other nodes.
    """
    
    def __init__(self, broker_urls: Dict[str, str]):
        """
        broker_urls: Maps node_id -> "http://localhost:PORT"
        Example: {"broker-1": "http://localhost:6001", ...}
        """
        self.broker_urls = broker_urls
        self.handlers = {}  # node_id -> handler function
        
    def send(self, to: str, msg: Dict[str, Any]) -> None:
        """
        Send message to another node via HTTP POST.
        """
        if to not in self.broker_urls:
            print(f"[TRANSPORT] Warning: Unknown destination {to}")
            return
        
        url = f"{self.broker_urls[to]}/raft_message"
        
        # Send in background thread (non-blocking)
        def send_async():
            try:
                requests.post(url, json=msg, timeout=1)
            except Exception as e:
                # Network failures are expected in distributed systems
                pass
        
        threading.Thread(target=send_async, daemon=True).start()
    
    def register(self, node_id: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """
        Register a handler for incoming messages.
        The Flask route will call this handler.
        """
        self.handlers[node_id] = handler
    
    def deliver(self, node_id: str, msg: Dict[str, Any]) -> None:
        """
        Called by Flask route to deliver message to the registered handler.
        """
        if node_id in self.handlers:
            self.handlers[node_id](msg)


class RealScheduler:
    """
    Scheduler implementation using real system time and threading.
    """
    
    def __init__(self):
        self.start_time = time.time() * 1000  # milliseconds
        
    def call_later(self, ms: int, cb: Callable[[], None]):
        """
        Schedule callback to run after ms milliseconds.
        Returns a cancel function.
        """
        timer_cancelled = threading.Event()
        
        def run_callback():
            # Wait for the specified time
            timer_cancelled.wait(timeout=ms / 1000.0)
            
            # If not cancelled, run the callback
            if not timer_cancelled.is_set():
                cb()
        
        thread = threading.Thread(target=run_callback, daemon=True)
        thread.start()
        
        # Return cancel function
        def cancel():
            timer_cancelled.set()
        
        return cancel
    
    def now_ms(self) -> int:
        """
        Return current time in milliseconds.
        """
        return int(time.time() * 1000 - self.start_time)