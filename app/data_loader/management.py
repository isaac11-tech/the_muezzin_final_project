import json
import datetime
from pathlib import Path
from app.instance_kafka import KafkaServerProducer
from utils.logger import Logger
from dotenv import load_dotenv

load_dotenv()



class Management:

    def __init__(self,kafka_producer: KafkaServerProducer):
        #creating Connection to kafka
        self.producer = kafka_producer.producer
        self.logger = Logger.get_logger()



    @staticmethod
    def get_metadata(path: Path):
        """""
        A function that get path and return from the file metadata mapping 
        """""

        metadata = {
            "metadata": {
                "name": path.name,
                "size": path.stat().st_size,
                "create_at": datetime.datetime.fromtimestamp(path.stat().st_ctime).isoformat(),
                'absolute_path': str(path.absolute())
            }
        }
        return metadata



    @staticmethod
    def create_json(file_path: Path):
        """""
        A function that get path and return json with a metadata
        """""
        metadata = Management.get_metadata(file_path)
        json_info = json.dumps(metadata)
        return json_info



    def send_to_kafka(self, data: json, topic):
        """""
        A function that get json and send that to kafka
        """""
        try:
            self.producer.send_data(data, topic)
        except Exception as e:
            self.logger.error("Error sending message:", e)

