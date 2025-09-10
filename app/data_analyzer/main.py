import json
import os
from logging import setLogRecordFactory

from app.instance_kafka import KafkaServerConsumer
from utils.logger import Logger
from dotenv import load_dotenv
from analyzer_service import AnalyzerService
from utils.config import INDEX_NAME
from src.elastic_search_dal.elastic_servies import ElasticService

load_dotenv()


class AnalyzerMain:

    def __init__(self, kafka_server: KafkaServerConsumer):
        self.consumer = kafka_server.consumer
        self.analyzer_service = AnalyzerService()
        self.logger = Logger.get_logger()

    def run(self):

        for message in self.consumer.consumer:
            try:
                #get the unique_id from a consumer
                unique_id = message.value
                self.logger.info(f"Received message from kafka: {unique_id}")
                #pull the metadata from elastic
                metadata_file = self.analyzer_service.get_file_from_elastic_by_id(unique_id)
                self.logger.info("The data was successfully extracted from Elasticsearch.")
                #pull the transcribed_audio from metadata
                txt = metadata_file["transcribed_audio"]
                #classification transcribed_audio
                score = self.analyzer_service.classification(txt)
                #enter new filed of bds_percent
                metadata_file["bds_percent"] = score
                metadata_file["is_bds"] = self.analyzer_service.is_bds_by_score(score)
                metadata_file["bds_threat_level"] = self.analyzer_service.get_bds_threat_level(score)
                #send the analyzer data to elastic
                self.analyzer_service.update_elastic_by_id(unique_id,metadata_file)
            except Exception as e:
                self.logger.error(f"can't connect to kafka ",e)





#result
# {'metadata': {'name': 'download (12).wav', 'size': 1430970, 'create_at': '1979-12-31T23:00:00', 'absolute_path': 'C:\\podcasts\\download (12).wav'}, 'unique_id': '44f8088f-4175-4f1c-b951-faa9d134aa81'}