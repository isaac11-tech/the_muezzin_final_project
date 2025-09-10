from data_loader.main import DataLoaderMain

from data_processor.main import DataProcessorMain
from  data_analyzer.main import AnalyzerMain
from instance_kafka import KafkaServerProducer,KafkaServerConsumer
from utils.config import TOPIC_NAME,UNIQUE_ID

if __name__ == "__main__":


    # data loder :
    kafka_conn_producer = KafkaServerProducer()
    data_loder_main = DataLoaderMain(kafka_conn_producer)
    data_loder_main.run()

    # data processer:
    kafka_conn_Consumer=KafkaServerConsumer(TOPIC_NAME)
    data_processor_main= DataProcessorMain(kafka_conn_Consumer,kafka_conn_producer)
    data_processor_main.run()


    # #data analyzer:
    kafka_conn_id_Consumer = KafkaServerConsumer(UNIQUE_ID)
    data_analyzer_main = AnalyzerMain(kafka_conn_id_Consumer)
    data_analyzer_main.run()

