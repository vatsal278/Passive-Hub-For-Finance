import json
import logging
from confluent_kafka import Consumer, Producer, KafkaException
from market_data_service.config.config_manager import ConfigManager

class KafkaClient:
    def __init__(self, config):
        self.config = config

    def create_consumer(self, topic, group_id, auto_offset_reset='earliest'):
        consumer_config = {
            'bootstrap.servers': self.config.get('KAFKA', 'bootstrap_servers'),
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset
        }
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
        return consumer

    def create_producer(self):
        producer_config = {
            'bootstrap.servers': self.config.get('KAFKA', 'bootstrap_servers')
        }
        return Producer(producer_config)

    def produce_message(self, producer, topic, key, value):
        try:
            producer.produce(topic, key=key, value=json.dumps(value))
            producer.flush()
        except KafkaException as e:
            logging.error(f"Failed to produce message: {e}")
            raise

    def consume_message(self, consumer):
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                return None
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                return None
            return json.loads(msg.value().decode('utf-8'))
        except KafkaException as e:
            logging.error(f"Failed to consume message: {e}")
            raise

