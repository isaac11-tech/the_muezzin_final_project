from kafka import KafkaConsumer
import json
from config import KAFKA_HOST


class Consumer:

    def __init__(self, topic,host = KAFKA_HOST):

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[host],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )


    """""
    function that get message object from consumer and convert that to json
    """""
    def load_to_json(self):
        json_data = None

        for message in self.consumer:
            try:
                json_data = json.loads(message.value)
                print(f"Received JSON message: {json_data}")
            except json.JSONDecodeError:
                print(f"Received non-JSON message: {message.value}")

            finally:
                self.consumer.close()

        return json_data

    #this function use for testing
    def consume_messages(self):
        try:
            for message in self.consumer:
                print(message)
                print(f"Received message: {message.value} "
                      f"(topic={message.topic}, partition={message.partition}, offset={message.offset})")
        except Exception as e:
            print("Error consuming messages:", e)
        finally:
            self.consumer.close()
