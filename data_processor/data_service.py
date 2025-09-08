import uuid
import json
from http.client import responses

from config import ES_URL, INDEX_NAME
from elastic_search_dal.elastic_servies import ElasticService


class DataService:

    def __init__(self):
        # creat connection to Elastic
        self.elastic_conn = ElasticService(ES_URL, INDEX_NAME)

    """""
    function that for each file adding unique_id
    """""

    @staticmethod
    def add_unique_id(data: json):
        data['unique_id'] = str(uuid.uuid4())
        return data

    def send_metadata_to_elasticsearch(self, index_name, _id, document):
        try:
            response = self.elastic_conn.insert_document(index=index_name, _id=_id, body =document)
            print(f"data Sent successfully to elasticsearch response : {response}")
        except Exception as e:
            print("Error to connect to elasticsearch, messages:", e)

    def send_file_to_mongodb(self):
        pass  # not ready yet
