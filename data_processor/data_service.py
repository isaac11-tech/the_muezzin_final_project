import uuid
import json
import os
from config import  INDEX_NAME
from elastic_search_dal.elastic_servies import ElasticService
from mongodb_dal.mongo_service import MongoService
from config import DB_NAME,COLLECTION_NAME
from logger import Logger
from dotenv import load_dotenv

load_dotenv()


class DataService:

    def __init__(self):
        # create connection to Elastic
        self.elastic_conn = ElasticService(os.getenv('ES_URL'), INDEX_NAME)
        self.mongo_conn = MongoService(DB_NAME,COLLECTION_NAME)
        self.logger = Logger.get_logger()


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
            self.logger.info(f"data Sent successfully to elasticsearch response : {response}")
        except Exception as e:
            self.logger.error("Error to connect to elasticsearch, messages:", e)


    """""
    function that get path and return file
    """""
    def get_file_by_path(self,file_path):
        try:
            with open(file_path, 'rb') as file:
                f = file.read()
                return f
        except Exception as e:
           self.logger.error("Error to open the file, messages:", e)


    """""
    function that send the file and id to mongodb
    """""
    def send_file_to_mongodb(self,unique_id,file):
        try:
          data = {unique_id:file}
          result = self.mongo_conn.insert_one(data)
          self.logger.info(f"this the responds from mongodb:{result}")
        except Exception as e:
            self.logger.error("error to send to mongodb,massage:",e)
