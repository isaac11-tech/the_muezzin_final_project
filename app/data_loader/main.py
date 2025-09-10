from pathlib import Path
from utils.config import FILE_PATH,TOPIC_NAME
from utils.logger import Logger
from app.instance_kafka import  KafkaServerProducer
from app.data_loader.management import Management



class DataLoaderMain:

    def __init__(self,kafka_producer: KafkaServerProducer):
        self. manager = Management(kafka_producer)
        self.logger = Logger.get_logger()

    def ran(self):
        path = Path(FILE_PATH)
        # loop that send all the files to kafka
        for file in path.iterdir():
            if file.is_file():
                json_metadata = Management.create_json(file.absolute())
                self.manager.send_to_kafka(json_metadata, TOPIC_NAME)
                self.logger.info(f"file:{file.name} metadata send to kafka at topik{TOPIC_NAME}")

        # after the loop finish send all the data close the producer
        self.manager.producer.close_producer()










