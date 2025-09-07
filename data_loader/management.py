from kafka_server.producer import Producer
from mutagen.wave import WAVE
from pathlib import Path
import json
import datetime


class Management:

    def __init__(self):
        #creating Connection to kafka
        self.producer = Producer()


    """""
    A function that get path and returns metadata 
    """""
    @staticmethod
    def get_metadata(path: Path):
        metadata = {
            "metadata": {
                "name": path.name,
                "size": path.stat().st_size,
                "create_at": datetime.datetime.fromtimestamp(path.stat().st_ctime).isoformat(),
                'absolute_path': str(path.absolute())
            }
        }
        return metadata


    """""
    A function that get path and return json with  a metadata
    """""
    @staticmethod
    def create_json(file_path: Path):

        metadata = Management.get_metadata(file_path)
        json_info = json.dumps(metadata)

        return json_info


    """""
    A function that get json and send that to kafka
    """""
    def send_to_kafka(self, data: json, topic):
        try:
            self.producer.send_data(data, topic)
        except Exception as e:
            print("Error sending message:", e)
