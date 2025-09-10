from data_loader.main import DataLoaderMain
from data_processor.main import DataProcessorMain
from instance_kafka import KafkaServer
from utils.config import TOPIC_NAME

if __name__ == "__main__":

    kafka_conn = KafkaServer(TOPIC_NAME)

    # data loder :
    data_loder_main = DataLoaderMain(kafka_conn)
    data_loder_main.ran()

    # data processer:
    data_processor_main = DataProcessorMain(kafka_conn)
    data_processor_main.ran()

    #data analyzer:
