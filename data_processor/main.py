from kafka_server.consumer import Consumer
from config import KAFKA_HOST,TOPIC_NAME
from data_service import DataService

class Main:

    def __init__(self):
        # create a connection to the consumer.
        self.consumer = Consumer(TOPIC_NAME, KAFKA_HOST)

    def ran(self):
        # get all the data from consumer as json
        data = self.consumer.load_to_json()
        # Creating an instance of DataService
        service = DataService()
        # adding unique_id for every file
        data = service.add_unique_id(data)


if __name__ == "__main__":
    pass







