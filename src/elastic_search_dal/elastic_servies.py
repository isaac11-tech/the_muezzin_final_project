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


    def get_document_by_match(self,index_name, field_name,field_value):
        try:
            query = {
                "query": {
                    "match": {
                        field_name: field_value
                    }
                }
            }
            response = self.es.search(index=index_name, body=query)

            hits = response["hits"]["hits"]
            if hits:
                return hits[0]["_source"]
            else:
                return None
        except Exception as e:
            print(f"Error searching document: {e}")
            return None

    def update_doc(self, doc_id, body):
        return self.es.update(index=self.index_name, id=doc_id, body=body)



