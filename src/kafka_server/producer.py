import json
from kafka import KafkaProducer
from utils.logger import Logger

class Producer:

    def __init__(self,host):
        # creating producer object
        self.producer = KafkaProducer(
            bootstrap_servers=[host],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.logger = Logger.get_logger()


    def send_data(self, data, topic):
        try:
            self.producer.send(topic, data)
            self.producer.flush()
            self.logger.info("Message sent:", data)
        except Exception as e:
            self.logger.error("Error sending message:", e)

    def close_producer(self):
        self.producer.close()
