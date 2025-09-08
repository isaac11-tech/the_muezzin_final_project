import uuid
import json
from config import ES_URL, INDEX_NAME
from elastic_search_dal.elastic_servies import ElasticService
from mongodb_dal.mongo_service import MongoService
from config import DB_NAME,COLLECTION_NAME

class DataService:

    def __init__(self):
        # create connection to Elastic
        self.elastic_conn = ElasticService(ES_URL, INDEX_NAME)
        self.mongo_conn = MongoService(DB_NAME,COLLECTION_NAME)


    """""
    function that for each file adding unique_id
    """""
    @staticmethod
    def add_unique_id(data: json):
        data['unique_id'] = str(uuid.uuid4())
        return data

    """""
    function that send metadata to elasticsearch
    """""
    def send_metadata_to_elasticsearch(self, index_name, _id, document):
        try:
            response = self.elastic_conn.insert_document(index=index_name, _id=_id, body =document)
            print(f"data Sent successfully to elasticsearch response : {response}")
        except Exception as e:
            print("Error to connect to elasticsearch, messages:", e)


    """""
    function that get path and return file
    """""
    @staticmethod
    def get_file_by_path(file_path):
        try:
            with open(file_path, 'rb') as file:
                f = file.read()
                return f
        except Exception as e:
            print("Error to open the file, messages:", e)


    """""
    function that send the file and id to mongodb
    """""
    def send_file_to_mongodb(self,file,unique_id):#need to add try !!!
        data = {unique_id:file}
        result = self.mongo_conn.insert_one(data)
        print(result)