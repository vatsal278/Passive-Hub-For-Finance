import threading
import logging
import pandas as pd
import json
from datetime import datetime
from market_data_service.message_broker.kafka_client import KafkaClient

class Instrument:
    def __init__(self, df):
        self.odf = df
        self.df = df
        self._validate_df()

    ohlc = {'open', 'high', 'low', 'close'}

    UPTREND_CONTINUAL = 0
    UPTREND_REVERSAL = 1
    DOWNTREND_CONTINUAL = 2
    DOWNTREND_REVERSAL = 3

    def _validate_df(self):
        if not self.ohlc.issubset(self.df.columns):
            raise ValueError('DataFrame should have OHLC {} columns'.format(self.ohlc))

class Renko(Instrument):
    PERIOD_CLOSE = 1
    PRICE_MOVEMENT = 2

    TREND_CHANGE_DIFF = 2

    brick_size = 20
    chart_type = PERIOD_CLOSE

    def get_ohlc_data(self):
        if self.chart_type == self.PERIOD_CLOSE:
            self.period_close_bricks()
        else:
            self.price_movement_bricks()
        return self.cdf

    def price_movement_bricks(self):
        pass

    def period_close_bricks(self):
        brick_size = self.brick_size
        columns = ['date', 'open', 'high', 'low', 'close']
        self.df = self.df[columns]

        self.cdf = pd.DataFrame(columns=columns, data=[])
        self.cdf.loc[0] = self.df.loc[0]
        close = self.df.loc[0]['close'] // brick_size * brick_size
        self.cdf.iloc[0, 1:] = [close - brick_size, close, close - brick_size, close]
        self.cdf['uptrend'] = True

        columns = ['date', 'open', 'high', 'low', 'close', 'uptrend']

        for index, row in self.df.iterrows():
            close = row['close']
            date = row['date']

            row_p1 = self.cdf.iloc[-1]
            uptrend = row_p1['uptrend']
            close_p1 = row_p1['close']

            bricks = int((close - close_p1) / brick_size)
            data = []

            if uptrend and bricks >= 1:
                for i in range(bricks):
                    r = [date, close_p1, close_p1 + brick_size, close_p1, close_p1 + brick_size, uptrend]
                    data.append(r)
                    close_p1 += brick_size
            elif uptrend and bricks <= -self.TREND_CHANGE_DIFF:
                uptrend = not uptrend
                bricks += 1
                close_p1 -= brick_size
                for i in range(abs(bricks)):
                    r = [date, close_p1, close_p1, close_p1 - brick_size, close_p1 - brick_size, uptrend]
                    data.append(r)
                    close_p1 -= brick_size
            elif not uptrend and bricks <= -1:
                for i in range(abs(bricks)):
                    r = [date, close_p1, close_p1, close_p1 - brick_size, close_p1 - brick_size, uptrend]
                    data.append(r)
                    close_p1 -= brick_size
            elif not uptrend and bricks >= self.TREND_CHANGE_DIFF:
                uptrend = not uptrend
                bricks -= 1
                close_p1 += brick_size
                for i in range(abs(bricks)):
                    r = [date, close_p1, close_p1 + brick_size, close_p1, close_p1 + brick_size, uptrend]
                    data.append(r)
                    close_p1 += brick_size
            else:
                continue

            sdf = pd.DataFrame(data=data, columns=columns)
            self.cdf = pd.concat([self.cdf, sdf])

        self.cdf.reset_index(inplace=True, drop=True)
        return self.cdf

    def shift_bricks(self):
        shift = self.df['close'].iloc[-1] - self.bdf['close'].iloc[-1]
        if abs(shift) < self.brick_size:
            return
        step = shift // self.brick_size
        self.bdf[['open', 'close']] += step * self.brick_size

    def process_tick_data(self, tick_data):
        new_tick_df = pd.DataFrame([tick_data])
        self.df = pd.concat([self.df, new_tick_df], ignore_index=True)
        self.get_ohlc_data()

class RenkoProcessor:
    def __init__(self, config):
        self.config = config
        self.kafka_client = KafkaClient(config)
        self.consumer = self.kafka_client.create_consumer(
            topic=self.config.get('KAFKA', 'candlestick_data_topic'),
            group_id=self.config.get('KAFKA', 'group_id')
        )
        self.producer = self.kafka_client.create_producer()
        self.ohlc_data = []
        self.last_processed_index = -1
        self.stop_event = threading.Event()
        self.thread = None

    def process_messages(self):
        try:
            while not self.stop_event.is_set():
                data = self.kafka_client.consume_message(self.consumer)
                if data:
                    self.process_data(data)
        except Exception as e:
            logging.error(f"Exception in RenkoProcessor: {str(e)}")
        finally:
            self.consumer.close()

    def process_data(self, data):
        try:
            logging.info(f"Processing data: {data}")
            if isinstance(data, dict):
                ohlc_record = {
                    'date': data['start_time'],
                    'open': data['open'],
                    'high': data['high'],
                    'low': data['low'],
                    'close': data['close']
                }
                self.ohlc_data.append(ohlc_record)
                self.generate_renko()
            else:
                logging.error("Invalid data format received in RenkoProcessor")
        except Exception as e:
            logging.error(f"Exception in RenkoProcessor: {e}")

    def generate_renko(self):
        if not self.ohlc_data or len(self.ohlc_data) == 0:
            return

        df = pd.DataFrame(self.ohlc_data)
        renko = Renko(df)
        renko_bricks = renko.get_ohlc_data()
        new_bricks = renko_bricks.iloc[self.last_processed_index + 1:]
        for _, brick in new_bricks.iterrows():
            self.produce_renko(brick)
        self.last_processed_index = len(renko_bricks) - 1

    def produce_renko(self, brick):
        brick_data = brick.to_dict()
        if not isinstance(brick_data['date'], datetime):
            try:
                brick_data['date'] = pd.to_datetime(brick_data['date'])
            except ValueError:
                logging.error(f"Invalid date format: {brick_data['date']}")
                return
        brick_data['date'] = brick_data['date'].isoformat()
        self.kafka_client.produce_message(
            producer=self.producer,
            topic=self.config.get('KAFKA', 'renko_data_topic'),
            key=brick_data['date'],
            value=brick_data
        )

    def start(self):
        self.thread = threading.Thread(target=self.process_messages)
        self.thread.start()

    def stop(self):
        logging.info("Stopping RenkoProcessor...")
        self.stop_event.set()
        if self.thread:
            self.thread.join()
        logging.info("RenkoProcessor stopped gracefully.")
