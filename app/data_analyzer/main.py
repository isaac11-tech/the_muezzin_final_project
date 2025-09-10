from app.instance_kafka import KafkaServerConsumer
from utils.logger import Logger
from app.data_analyzer.analyzer_service import AnalyzerService


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


