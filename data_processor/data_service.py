import uuid
import json


class DataService:

    """""
    function that for each file adding unique_id
    """""
    @staticmethod
    def add_unique_id(data: json):
        for file in data:
            file['unique_id'] = str(uuid.uuid4())
        return data


    def send_metadata_to_elasticsearch(self):
        pass#not ready yet

    def send_file_to_mongodb(self):
        pass#not ready yet
