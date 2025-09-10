import os
from src.kafka_server.consumer import Consumer
from src.kafka_server.producer import Producer
from dotenv import load_dotenv

load_dotenv()

class KafkaServerProducer:

    def __init__(self):
        self.producer = Producer(os.getenv('KAFKA_HOST'))

class KafkaServerConsumer:

    def __init__(self, topic_name):
        self.consumer = Consumer(topic_name, os.getenv('KAFKA_HOST'))




