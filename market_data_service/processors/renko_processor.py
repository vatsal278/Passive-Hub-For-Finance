import pandas as pd
import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, Producer
from market_data_service.config.config_manager import ConfigManager

class Instrument:
    def __init__(self, df):
        self.odf = df
        self.df = df
        self._validate_df()

    ohlc = {'open', 'high', 'low', 'close'}

    def _validate_df(self):
        if not self.ohlc.issubset(self.df.columns):
            raise ValueError('DataFrame should have OHLC {} columns'.format(self.ohlc))

class Renko(Instrument):
    PERIOD_CLOSE = 1
    PRICE_MOVEMENT = 2

    TREND_CHANGE_DIFF = 2

    def __init__(self, df, brick_size, chart_type=PERIOD_CLOSE):
        super().__init__(df)
        self.brick_size = brick_size
        self.chart_type = chart_type
        self.cdf = pd.DataFrame()

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
            if index == 0:
                continue

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

class RenkoProcessor:
    def __init__(self, config):
        self.config = config
        self.consumer = self.create_consumer()
        self.producer = self.create_producer()
        self.ohlc_data = []

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
        self.consumer.subscribe([self.config.get('KAFKA', 'candlestick_data_topic')])
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            
            self.process_data(data)

    def process_data(self, data):
        ohlc_record = {
            'date': data['start_time'],
            'open': data['open'],
            'high': data['high'],
            'low': data['low'],
            'close': data['close']
        }
        self.ohlc_data.append(ohlc_record)

        # Limit the size of ohlc_data list to prevent it from growing indefinitely
        max_data_points = 10  # Set the maximum number of data points you want to retain
        if len(self.ohlc_data) > max_data_points:
            self.ohlc_data.pop(0)  # Remove the oldest data point

        # Call generate_renko method to check if new Renko bricks need to be generated
        self.generate_renko()


    def generate_renko(self):
        if not self.ohlc_data or len(self.ohlc_data) == 0:
            return

        df = pd.DataFrame(self.ohlc_data)
        renko = Renko(df, brick_size=20)
        renko_bricks = renko.get_ohlc_data()

        # Check if there are new Renko bricks to publish
        if len(renko_bricks) > len(self.ohlc_data):
            new_bricks = renko_bricks.iloc[len(self.ohlc_data):]
            for _, brick in new_bricks.iterrows():
                self.produce_renko(brick)

            # Update the OHLC data list
            self.ohlc_data = renko_bricks.to_dict(orient='records')

    def produce_renko(self, brick):
        brick_data = brick.to_dict()
        brick_data['date'] = brick_data['date'].isoformat()
        self.producer.produce(
            self.config.get('KAFKA', 'renko_data_topic'),
            key=brick_data['date'],
            value=json.dumps(brick_data)
        )
        self.producer.flush()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = ConfigManager()

    processor = RenkoProcessor(config=config)
    processor.process_messages()
