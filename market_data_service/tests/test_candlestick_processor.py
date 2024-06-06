import unittest
from unittest.mock import patch, MagicMock
from market_data_service.processors.candlestick_processor import CandlestickProcessor
from market_data_service.config.config_manager import ConfigManager
import json
from datetime import datetime, timedelta

class TestCandlestickProcessor(unittest.TestCase):

    def setUp(self):
        self.sample_message = {
            'e': 'trade',
            'E': 1717662422639,
            's': 'BTCUSDT',
            't': 3626222999,
            'p': '70970.13000000',
            'q': '0.00103000',
            'b': 27672737647,
            'a': 27672738053,
            'T': 1717662422638,
            'm': True,
            'M': True
        }

        self.config = ConfigManager()
        self.processor = CandlestickProcessor(self.config)

    @patch('market_data_service.processors.candlestick_processor.Consumer')
    @patch('market_data_service.processors.candlestick_processor.Producer')
    def test_initialization(self, mock_producer, mock_consumer):
        mock_consumer_instance = mock_consumer.return_value
        mock_producer_instance = mock_producer.return_value

        processor = CandlestickProcessor(self.config)
        self.assertIsNotNone(processor.consumer)
        self.assertIsNotNone(processor.producer)
        self.assertEqual(processor.candlestick_interval, int(self.config.get('PROCESSOR', 'candlestick_interval')))

    @patch('market_data_service.processors.candlestick_processor.Consumer')
    @patch('market_data_service.processors.candlestick_processor.Producer')
    def test_process_single_message(self, mock_producer, mock_consumer):
        mock_consumer_instance = mock_consumer.return_value
        mock_producer_instance = mock_producer.return_value
        processor = CandlestickProcessor(self.config)
        processor.process_data(self.sample_message)

        self.assertEqual(len(processor.data), 1)
        self.assertEqual(processor.data[0]['price'], 70970.13)
        self.assertEqual(processor.data[0]['quantity'], 0.00103)

    @patch('market_data_service.processors.candlestick_processor.Consumer')
    @patch('market_data_service.processors.candlestick_processor.Producer')
    def test_candlestick_generation(self, mock_producer, mock_consumer):
        mock_consumer_instance = mock_consumer.return_value
        mock_producer_instance = mock_producer.return_value
        processor = CandlestickProcessor(self.config)

        processor.process_data(self.sample_message)
        timestamp = datetime.fromtimestamp(self.sample_message['E'] / 1000.0)
        new_message = self.sample_message.copy()
        new_message['p'] = '71000.00000000'
        new_message['q'] = '0.00200000'
        new_message['E'] = int((timestamp + timedelta(seconds=30)).timestamp() * 1000)

        processor.process_data(new_message)

        self.assertEqual(len(processor.data), 2)

        # Generate the candlestick
        processor.process_data(self.sample_message)  # Add an additional message to trigger processing

        produced_data = json.loads(mock_producer_instance.produce.call_args[1]['value'])
        print(produced_data)
        self.assertEqual(produced_data['open'], 70970.13)
        self.assertEqual(produced_data['high'], 71000.00)
        self.assertEqual(produced_data['low'], 70970.13)
        self.assertEqual(produced_data['close'], 71000.00)
        self.assertEqual(produced_data['volume'], 0.00303)

    @patch('market_data_service.processors.candlestick_processor.Consumer')
    @patch('market_data_service.processors.candlestick_processor.Producer')
    def test_candlestick_output(self, mock_producer, mock_consumer):
        mock_consumer_instance = mock_consumer.return_value
        mock_producer_instance = mock_producer.return_value
        processor = CandlestickProcessor(self.config)

        processor.process_data(self.sample_message)
        timestamp = datetime.fromtimestamp(self.sample_message['E'] / 1000.0)
        new_message = self.sample_message.copy()
        new_message['p'] = '71000.00000000'
        new_message['q'] = '0.00200000'
        new_message['E'] = int((timestamp + timedelta(seconds=processor.candlestick_interval)).timestamp() * 1000)

        processor.process_data(new_message)  # To trigger the candlestick generation

        produced_data = json.loads(mock_producer_instance.produce.call_args[1]['value'])
        print(produced_data)
        self.assertEqual(produced_data['open'], 70970.13)
        self.assertEqual(produced_data['high'], 71000.00)
        self.assertEqual(produced_data['low'], 70970.13)
        self.assertEqual(produced_data['close'], 71000.00)
        self.assertEqual(produced_data['volume'], 0.00303)

if __name__ == "__main__":
    unittest.main()
