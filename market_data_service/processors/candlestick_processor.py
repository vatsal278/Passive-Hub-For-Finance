import json
import logging
from datetime import datetime, timedelta
from confluent_kafka import Consumer, Producer
from market_data_service.config.config_manager import ConfigManager
import pandas as pd

from confluent_kafka import Consumer, Producer
from datetime import datetime, timedelta
import json

class CandlestickProcessor:
    def __init__(self, config):
        self.config = config
        self.consumer = Consumer(self.config.get_kafka_consumer_config())
        self.producer = Producer(self.config.get_kafka_producer_config())
        self.candlestick_interval = int(self.config.get('PROCESSOR', 'candlestick_interval'))
        self.data = []
        self.current_candlestick = None
        self.last_candlestick_time = None

    def process_data(self, message):
        price = float(message['p'])
        quantity = float(message['q'])
        timestamp = datetime.fromtimestamp(message['E'] / 1000.0)

        if self.current_candlestick is None:
            self.current_candlestick = {
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': quantity,
                'start_time': timestamp,
            }
            self.last_candlestick_time = timestamp
        else:
            self.current_candlestick['high'] = max(self.current_candlestick['high'], price)
            self.current_candlestick['low'] = min(self.current_candlestick['low'], price)
            self.current_candlestick['close'] = price
            self.current_candlestick['volume'] += quantity

        if (timestamp - self.last_candlestick_time) >= timedelta(seconds=self.candlestick_interval):
            self.produce_candlestick()
            self.current_candlestick = {
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': quantity,
                'start_time': timestamp,
            }
            self.last_candlestick_time = timestamp

    def produce_candlestick(self):
        if self.current_candlestick:
            candlestick = self.current_candlestick.copy()
            candlestick['start_time'] = candlestick['start_time'].isoformat()
            self.producer.produce(self.config.get('PROCESSOR', 'output_topic'), value=json.dumps(candlestick))
            self.current_candlestick = None



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = ConfigManager()
    processor = CandlestickProcessor(config)
    processor.process_messages()
