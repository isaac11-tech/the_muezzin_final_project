import uuid

from kafka import KafkaConsumer
import json


class Consumer:

    def __init__(self, topic, host = 'localhost:9092'):

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[host],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    def consume_messages(self):
        try:
            for message in self.consumer:

                print(message.value)
                print(f"Received message: {message.value} "
                      f"(topic={message.topic}, partition={message.partition}, offset={message.offset})")
        except Exception as e:
            print("Error consuming messages:", e)
        finally:
            self.consumer.close()



    def load_to_json(self):
        i = 0
        json_data = None
        i = i + 1
        for message in self.consumer:
            try:
                json_data = json.loads(message.value)
                if i == 10:
                    self.consumer.close()
                    break
                print(f"Received JSON message: {json_data}")
            except json.JSONDecodeError:
                print(f"Received non-JSON message: {message.value}")

        return json_data




import uuid

c = Consumer("audio_metadata")

a = c.load_to_json()





