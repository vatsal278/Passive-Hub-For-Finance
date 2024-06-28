import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import threading
import logging
import csv  # Added for CSV operations

base_url = 'https://api.binance.com/api/v3/klines'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_klines_batch(symbol, interval, start_time, end_time):
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'endTime': end_time,
        'limit': 1000  # Fetch maximum 1000 klines per request
    }
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {start_time} - {end_time}: {e}")
        return []

def get_binance_klines(symbol, interval, start_time, end_time):
    all_klines = []
    lock = threading.Lock()  # Lock for synchronizing access to all_klines

    def fetch_and_append_klines(start_time, end_time):
        nonlocal all_klines
        try:
            klines = fetch_klines_batch(symbol, interval, start_time, end_time)
            if klines:
                with lock:
                    all_klines.extend(klines)
                logger.info(f"Fetched {len(klines)} klines from {start_time} to {end_time}")
                return klines
            else:
                logger.warning(f"No data fetched for {start_time} - {end_time}")
                return []
        except Exception as e:
            logger.error(f"Failed to fetch data for {start_time} - {end_time}: {e}")
            return []

    current_time = start_time
    with ThreadPoolExecutor(max_workers=50) as executor:
        while current_time < end_time:
            future = executor.submit(fetch_and_append_klines, current_time, end_time)
            try:
                result = future.result(timeout=60)  # Wait for each thread to complete with a timeout of 60 seconds
                if result:
                    current_time = int(result[-1][6]) + 1  # Update start_time to be just after the last timestamp fetched
                else:
                    break
            except Exception as e:
                logger.error(f"Thread execution failed: {e}")
                break

    # Save to CSV after fetching all klines
    save_to_csv(all_klines)

    return all_klines

def save_to_csv(data):
    csv_filename = 'binance_klines_pt8s.csv'
    headers = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume',
               'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore']

    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)

    logger.info(f"Saved {len(data)} klines to {csv_filename}")

def main():
    symbol = 'BTCUSDT'
    interval = '1s'
    # Replace with earliest date Binance provides data
    start_time = datetime(2017, 8, 17)
    end_time = datetime.utcnow()  # Replace with today's date

    start_timestamp = 1698839989000 #int(start_time.timestamp() * 1000)
    end_timestamp = int(end_time.timestamp() * 1000)

    logger.info(f"Fetching Binance klines for {symbol} from {start_time} to {end_time}...")

    all_klines = get_binance_klines(symbol, interval, start_timestamp, end_timestamp)

    # Now all_klines should contain all the fetched klines in sequential order
    logger.info(f"Fetched {len(all_klines)} klines")

if __name__ == "__main__":
    main()
