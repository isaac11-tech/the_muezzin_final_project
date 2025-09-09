from elasticsearch import Elasticsearch
from utils.logger import Logger


class ElasticService:


    def __init__(self, es_host, index_name):
        self.es_host = es_host
        self.es = Elasticsearch(self.es_host)
        self.index_name = index_name
        self.logger = Logger.get_logger()


    def insert_document(self,index,_id,body):
        try:
          response = self.es.index(index=index,id=_id,body=body)
          return response
        except Exception as e:
            self.logger.error("Error to send to elasticsearch, messages:", e)



