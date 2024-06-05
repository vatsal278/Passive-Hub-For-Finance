import json
import threading
import time
import websocket
import signal
from confluent_kafka import Producer
from market_data_service.config.config_manager import ConfigManager
import logging
import sys
import os



class BinanceFetcher:
    def __init__(self, symbol, market_type, config):
        self.symbol = symbol
        self.market_type = market_type
        self.config = config
        self.ws_url = self.generate_ws_url()
        self.ws = None
        self.reconnect_interval = self.config.getint('DEFAULT', 'reconnect_interval')
        self.max_reconnect_interval = self.config.getint('DEFAULT', 'max_reconnect_interval')
        self.ping_interval = self.config.getint('DEFAULT', 'ping_interval')
        self.ping_timeout = self.config.getint('DEFAULT', 'ping_timeout')
        self.connection_check_interval = self.config.getint('DEFAULT', 'connection_check_interval')
        self.stop_event = threading.Event()
        self.connect()

    def generate_ws_url(self):
        if self.market_type == 'futures':
            return self.config.get('WEBSOCKET', 'ws_url_futures').format(symbol=self.symbol)
        else:  # Default to spot market
            return self.config.get('WEBSOCKET', 'ws_url_spot').format(symbol=self.symbol)

    def connect(self):
        logging.info(f"Connecting to {self.ws_url}")
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        self.wst = threading.Thread(target=self.run_forever_wrapper)
        self.wst.daemon = True
        self.wst.start()
        self.wait_for_connection()
        self.start_connection_checker()

    def run_forever_wrapper(self):
        while not self.stop_event.is_set():
            try:
                self.ws.run_forever(ping_interval=self.ping_interval, ping_timeout=self.ping_timeout)
            except Exception as e:
                logging.error(f"Exception in WebSocket thread: {e}")
                self.reconnect()

    def wait_for_connection(self):
        while not self.ws.sock or not self.ws.sock.connected:
            logging.info("Waiting for connection...")
            time.sleep(1)
        logging.info("Connection established.")

    def on_open(self, ws):
        logging.info("WebSocket connection opened.")
        self.reconnect_interval = self.config.getint('DEFAULT', 'reconnect_interval')  # Reset reconnect interval on successful connection

    def on_message(self, ws, message):
        data = json.loads(message)
        logging.info(f"Received data: {data}")

    def on_error(self, ws, error):
        logging.error(f"WebSocket Error: {error}")
        self.reconnect()

    def on_close(self, ws, close_status_code, close_msg):
        logging.info(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.reconnect()

    def reconnect(self):
        if self.stop_event.is_set():
            return
        logging.info(f"Reconnecting in {self.reconnect_interval} seconds...")
        time.sleep(self.reconnect_interval)
        self.reconnect_interval = min(self.reconnect_interval * 2, self.max_reconnect_interval)
        self.connect()

    def start_connection_checker(self):
        self.connection_checker = threading.Thread(target=self.check_connection)
        self.connection_checker.daemon = True
        self.connection_checker.start()

    def check_connection(self):
        while not self.stop_event.is_set():
            if not self.ws.sock or not self.ws.sock.connected:
                logging.warning("Connection lost. Attempting to reconnect...")
                self.reconnect()
            time.sleep(self.connection_check_interval)

    def close(self):
        self.stop_event.set()
        if self.ws:
            self.ws.close()
        logging.info("Fetcher closed gracefully.")

def signal_handler(sig, frame):
    logging.info("Signal received, shutting down...")
    fetcher.close()
    exit(0)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    config = ConfigManager()

    try:
        fetcher = BinanceFetcher(symbol='btcusdt', market_type='spot', config=config)
        while True:
            time.sleep(10)  # Keep the main thread alive
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
        fetcher.close()
