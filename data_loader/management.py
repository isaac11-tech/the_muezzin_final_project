from kafka_server.producer import Producer
from mutagen.wave import WAVE
from pathlib import Path
import json
import datetime


class Management:

    def __init__(self):
        self.producer = Producer()

    @staticmethod
    def get_metadata(path:Path):
        metadata = {
            "metadata": {
                "name": path.name,
                "size": path.stat().st_size,
                "create_at": datetime.datetime.fromtimestamp(path.stat().st_ctime).isoformat(),
                'absolute_path':path.absolute()
            }
        }
        return metadata

    @staticmethod
    def create_json(file_path, file_name):
        path = Path(file_path)
        audio_name = {
            'audio_info': {
                "file_path": file_path,

            }
        }
        audio_name = json.dumps(audio_name)

        return audio_name

    def send_to_kafka(self, data: json, topic):
        try:
            self.producer.send_data(data, topic)
        except Exception as e:
            print("Error sending message:", e)
