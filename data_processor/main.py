import json
import os
from kafka_server.consumer import Consumer
from config import TOPIC_NAME,INDEX_NAME
from data_service import DataService
from logger import Logger
from dotenv import load_dotenv

load_dotenv()


class Main:

    def __init__(self):
        # create a connection to the consumer.
        self.consumer = Consumer(TOPIC_NAME,os.getenv('KAFKA_HOST'))
        self.service = DataService()
        self.logger = Logger.get_logger()


    def ran(self):
       
        for message in self.consumer.consumer:
            try:
                json_data = json.loads(message.value)
                self.logger.info(f"Received JSON message: {json_data}")
                json_data = self.service.add_unique_id(json_data)
                self.logger.info(f"Adding unique id to JSON : {json_data}")
                file_path = json_data['metadata']['absolute_path']
                transcribed_audio = self.service.audio_to_txt(file_path)
                json_data = self.service.add_transcribed_audio(json_data,transcribed_audio)
                # send the metadata to elastic
                unique_id = json_data['unique_id']
                # json_data = add txt file
                self.service.send_metadata_to_elasticsearch(INDEX_NAME,unique_id,json_data)
                #send the file and the id to mongo db
                file = self.service.get_file_by_path(file_path)
                self.service.send_file_to_mongodb(file,unique_id)

            except json.JSONDecodeError:
                self.logger.error(f"Received non-JSON message: {message.value}")

        # when ending to send all the data close the consumer (not use yet)
        self.consumer.consumer.close()


if __name__ == "__main__":
    m = Main()
    m.ran()
