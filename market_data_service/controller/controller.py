import threading
import time
import logging
import signal
from market_data_service.fetchers.binance_websocket import BinanceFetcher
from market_data_service.processors.candlestick_processor import CandlestickProcessor
from market_data_service.processors.renko_processor import RenkoProcessor
from market_data_service.config.config_manager import ConfigManager

class MarketDataService:
    def __init__(self, config):
        self.config = config
        self.fetchers = []
        self.candlestick_processor = CandlestickProcessor(config)
        self.renko_processor = RenkoProcessor(config)

    def start(self):
        logging.info("Starting MarketDataService...")
        self.candlestick_processor.start()
        self.renko_processor.start()
        symbols = self.config.get('DEFAULT', 'symbols').split(',')
        market_type = self.config.get('DEFAULT', 'market_type')
        for symbol in symbols:
            fetcher = BinanceFetcher(symbol, market_type, self.config)
            self.fetchers.append(fetcher)
        logging.info("MarketDataService started.")

    def stop(self):
        logging.info("Stopping MarketDataService...")
        for fetcher in self.fetchers:
            fetcher.close()
        self.candlestick_processor.stop()
        self.renko_processor.stop()
        logging.info("MarketDataService stopped gracefully.")

def signal_handler(sig, frame):
    logging.info("Signal received, shutting down...")
    service.stop()
    exit(0)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = ConfigManager()
    service = MarketDataService(config)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    service.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
        service.stop()
