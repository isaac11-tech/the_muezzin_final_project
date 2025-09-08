import json
from kafka_server.consumer import Consumer
from config import KAFKA_HOST, TOPIC_NAME,INDEX_NAME
from data_service import DataService


class Main:

    def __init__(self):
        # create a connection to the consumer.
        self.consumer = Consumer(TOPIC_NAME, KAFKA_HOST)
        self.service = DataService()

    def ran(self):
        json_data = None

        for message in self.consumer.consumer:
            try:
                json_data = json.loads(message.value)
                print(f"Received JSON message: {json_data}")
                json_data = self.service.add_unique_id(json_data)
                print(f"Adding unique id to JSON : {json_data}")
                # send the metadata to elastic
                self.service.send_metadata_to_elasticsearch(INDEX_NAME,json_data['unique_id'],json_data)
                # send the file and the id to mongo db
                # self.service.send_file_to_mongodb()

            except json.JSONDecodeError:
                print(f"Received non-JSON message: {message.value}")

        # when ending to send all the data close the consumer (not use yet)
        self.consumer.consumer.close()


if __name__ == "__main__":
    m = Main()
    m.ran()
