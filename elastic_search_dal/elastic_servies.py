from elasticsearch import Elasticsearch, helpers
from config import ES_URL, INDEX_NAME

class ElasticService:


    def __init__(self, es_host=None, index_name=None):
        self.es_host = es_host or ES_URL
        self.es = Elasticsearch(self.es_host)
        self.index_name = index_name or INDEX_NAME


    def create_index(self):
        mapping = ElasticService.get_mapping_project()
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(
                index=self.index_name,
                body= mapping
            )
            print(f"Index '{self.index_name}' created")
        else:
            print(f"Index '{self.index_name}' already exists")



    def load_to_elastic(self, docs, chunk_size):
        actions = [
            {
                "_index": self.index_name,
                "_source": doc
            } for doc in docs
        ]
        helpers.bulk(self.es, actions, chunk_size=chunk_size)
        print(f"Loaded {len(docs)} documents into index '{self.index_name}'")


    def search(self, query, size):
        response = self.es.search(index=self.index_name, query=query, size=size)
        return response["hits"]["hits"]


    def update_doc(self, doc_id, body):
        return self.es.update(index=self.index_name, id=doc_id, body=body)


    def delete_by_query(self, query):
        self.es.delete_by_query(index=self.index_name, body={"query": query})


    def insert_one_by_one(self,dic:dict):
        for d in dic:
            self.es.index(index=self.index_name, document=d)
            print("data send")


    @staticmethod
    def get_mapping_project():
        mapping = {
            "mappings": {
                "properties": {
                    "TweetID": {
                        "type": "keyword"
                    },
                    "CreateDate": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss"
                    },
                    "Antisemitic": {
                        "type": "integer"
                    },
                    "text": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "sentiment_label": {
                        "type": "text"
                    },
                    "sentiment_score": {
                        "type": "text"
                    },
                    "weapons": {
                        "type": "keyword"
                    }
                }
            }
        }
        return mapping
