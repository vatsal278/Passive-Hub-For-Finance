import unittest
from unittest.mock import patch, MagicMock, call
import json
import threading
import time
from market_data_service.fetchers.binance_websocket import BinanceFetcher
from market_data_service.config.config_manager import ConfigManager

class TestBinanceFetcher(unittest.TestCase):
    def setUp(self):
        # Mock the config manager
        self.config = MagicMock(ConfigManager)
        self.config.getint.side_effect = lambda section, option: {
            'reconnect_interval': 5,
            'max_reconnect_interval': 60,
            'ping_interval': 10,
            'ping_timeout': 5,
            'connection_check_interval': 15
        }[option]
        self.config.get.side_effect = lambda section, option: {
            'ws_url_spot': 'wss://stream.binance.com:9443/ws/{symbol}@trade',
            'ws_url_futures': 'wss://fstream.binance.com/ws/{symbol}@aggTrade'
        }[option]

        # Set up BinanceFetcher with the mocked config
        self.fetcher = BinanceFetcher(symbol='btcusdt', market_type='spot', config=self.config)

    @patch('websocket.WebSocketApp')
    def test_initialization(self, MockWebSocketApp):
        self.assertEqual(self.fetcher.symbol, 'btcusdt')
        self.assertEqual(self.fetcher.market_type, 'spot')
        self.assertEqual(self.fetcher.ws_url, 'wss://stream.binance.com:9443/ws/btcusdt@trade')
        self.assertIsNotNone(self.fetcher.ws)

    @patch('websocket.WebSocketApp')
    def test_connect(self, MockWebSocketApp):
        mock_ws = MockWebSocketApp.return_value
        self.fetcher.connect()
        mock_ws.run_forever.assert_called_with(ping_interval=10, ping_timeout=5)

    @patch('websocket.WebSocketApp')
    def test_on_message(self, MockWebSocketApp):
        mock_ws = MockWebSocketApp.return_value
        mock_message = json.dumps({"e": "trade", "E": 123456789, "s": "BTCUSDT", "t": 12345, "p": "10000.0", "q": "0.1", "b": 123, "a": 123, "T": 123456789, "m": False, "M": True})
        with patch('logging.info') as mock_log_info:
            self.fetcher.on_message(mock_ws, mock_message)
            mock_log_info.assert_called_with(f"Received data: {json.loads(mock_message)}")

    @patch('websocket.WebSocketApp')
    def test_on_error(self, MockWebSocketApp):
        mock_ws = MockWebSocketApp.return_value
        mock_error = Exception("Test Error")
        with patch('logging.error') as mock_log_error, patch.object(self.fetcher, 'reconnect') as mock_reconnect:
            self.fetcher.on_error(mock_ws, mock_error)
            mock_log_error.assert_called_with(f"WebSocket Error: {mock_error}")
            mock_reconnect.assert_called_once()

    @patch('websocket.WebSocketApp')
    def test_on_close(self, MockWebSocketApp):
        mock_ws = MockWebSocketApp.return_value
        with patch('logging.info') as mock_log_info, patch.object(self.fetcher, 'reconnect') as mock_reconnect:
            self.fetcher.on_close(mock_ws, 1000, "Test Close")
            mock_log_info.assert_called_with("WebSocket closed: 1000 - Test Close")
            mock_reconnect.assert_called_once()

    @patch('websocket.WebSocketApp')
    def test_reconnect(self, MockWebSocketApp):
        self.fetcher.reconnect_interval = 5
        with patch('time.sleep', return_value=None), patch.object(self.fetcher, 'connect') as mock_connect:
            self.fetcher.reconnect()
            self.assertEqual(self.fetcher.reconnect_interval, 10)  # Ensure exponential backoff is applied
            mock_connect.assert_called_once()

    @patch('websocket.WebSocketApp')
    def test_close(self, MockWebSocketApp):
        mock_ws = MockWebSocketApp.return_value
        self.fetcher.ws = mock_ws
        with patch('logging.info') as mock_log_info:
            self.fetcher.close()
            mock_ws.close.assert_called_once()
            mock_log_info.assert_called_with("Fetcher closed gracefully.")

if __name__ == '__main__':
    unittest.main()
