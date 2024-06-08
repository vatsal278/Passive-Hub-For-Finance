import json
import logging
from datetime import datetime, timedelta
from confluent_kafka import Consumer, Producer
from market_data_service.config.config_manager import ConfigManager

class CandlestickConverter:
    def __init__(self, timeframe_seconds):
        self.timeframe_seconds = timeframe_seconds
        self.current_candlestick = None
        self.historical_candlesticks = []

    def update_tick(self, tick):
        tick_time = datetime.fromtimestamp(tick['T'] / 1000.0)
        price = float(tick['p'])
        volume = float(tick['q'])

        if self.current_candlestick is None:
            self.current_candlestick = {
                'start_time': tick_time,
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': volume
            }
        else:
            # Convert start_time to datetime if it's a string
            if isinstance(self.current_candlestick['start_time'], str):
                self.current_candlestick['start_time'] = datetime.fromisoformat(self.current_candlestick['start_time'])

            # Check if the tick belongs to the next timeframe
            if (tick_time - self.current_candlestick['start_time']).total_seconds() >= self.timeframe_seconds:
                self.historical_candlesticks.append(self.current_candlestick)
                self.current_candlestick = {
                    'start_time': tick_time,
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume
                }
                return True  # Indicate that a candlestick has closed and a new one has started
            else:
                self.current_candlestick['high'] = max(self.current_candlestick['high'], price)
                self.current_candlestick['low'] = min(self.current_candlestick['low'], price)
                self.current_candlestick['close'] = price
                self.current_candlestick['volume'] += volume
        return False  # No new candlestick has closed yet

    def get_closed_candlestick(self):
        if self.historical_candlesticks:
            return self.historical_candlesticks.pop(0)
        return None

    def generate_candlestick(self):
        return self.current_candlestick

    def get_historical_candlesticks(self):
        return self.historical_candlesticks


class CandlestickProcessor:
    def __init__(self, config):
        self.config = config
        self.consumer = self.create_consumer()
        self.producer = self.create_producer()
        self.converter = CandlestickConverter(timeframe_seconds=float(self.config.get('PROCESSOR', 'candlestick_interval')))

    def create_consumer(self):
        return Consumer({
            'bootstrap.servers': self.config.get('KAFKA', 'bootstrap_servers'),
            'group.id': self.config.get('KAFKA', 'group_id'),
            'auto.offset.reset': 'earliest'
        })

    def create_producer(self):
        return Producer({
            'bootstrap.servers': self.config.get('KAFKA', 'bootstrap_servers')
        })

    def process_messages(self):
        self.consumer.subscribe([self.config.get('KAFKA', 'market_data_topic')])
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            logging.info(f"Received message: {data}")
            self.process_data(data)

    def process_data(self, data):
        candlestick_closed = self.converter.update_tick(data)
        if candlestick_closed:
            closed_candlestick = self.converter.get_closed_candlestick()
            if closed_candlestick:
                self.produce_candlestick(closed_candlestick)

    def produce_candlestick(self, candlestick):
        # Convert datetime to string for serialization
        candlestick['start_time'] = candlestick['start_time'].isoformat()
        self.producer.produce(
            self.config.get('KAFKA', 'candlestick_data_topic'),
            key=str(candlestick['start_time']),
            value=json.dumps(candlestick)
        )
        self.producer.flush()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = ConfigManager()

    processor = CandlestickProcessor(config=config)
    processor.process_messages()

