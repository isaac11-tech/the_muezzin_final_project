import json
from app.instance_kafka import  KafkaServer
from utils.config import TOPIC_NAME,INDEX_NAME,UNIQUE_ID
from app.data_processor.processor_service import DataService
from utils.logger import Logger
from dotenv import load_dotenv

load_dotenv()


class DataProcessorMain:

    def __init__(self,kafka_server: KafkaServer):
        # create a connection to the consumer.
        self.consumer = kafka_server.consumer
        self.producer = kafka_server.producer
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
                #send to queue in kafka For retrieval from Elasticsearch for data analysis
                self.producer.send_data(unique_id,UNIQUE_ID)
                self.logger.info("send the unique_id to kafka")
                # json_data = add txt file
                self.service.send_metadata_to_elasticsearch(INDEX_NAME,unique_id,json_data)
                #send the file and the id to mongo db
                file = self.service.get_file_by_path(file_path)
                self.service.send_file_to_mongodb(file,unique_id)

            except json.JSONDecodeError:
                self.logger.error(f"Received non-JSON message: {message.value}")

        # when ending to send all the data close the consumer (not use yet)
        self.consumer.consumer.close()


