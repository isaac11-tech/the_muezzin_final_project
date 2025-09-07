from kafka import KafkaProducer
import json


class Producer:

    def __init__(self, host='localhost:9092'):#reaning on localhost fot testing
        # creating producer object
        self.producer = KafkaProducer(
            bootstrap_servers=[host],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )


    def send_data(self, data, topic):
        try:
            self.producer.send(topic, data)
            self.producer.flush()
            print("Message sent:", data)
        except Exception as e:
            print("Error sending message:", e)

    def close_producer(self):
        self.producer.close()
