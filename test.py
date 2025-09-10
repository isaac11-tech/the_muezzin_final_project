import os
from utils.config import INDEX_NAME




from dotenv import load_dotenv

load_dotenv()

from src.elastic_search_dal.elastic_servies import ElasticService

ec = ElasticService(os.getenv("ES_URL"),INDEX_NAME)

doc = ec.get_document_by_match(index_name=INDEX_NAME,field_name='unique_id',field_value="d95d2719-416c-4120-9b9a-dab66b89de7f")
print(doc["transcribed_audio"])
doc["isaac"] = 'tunik'



