from management import Management
from pathlib import Path
from config import FILE_PATH,TOPIC_NAME

class Main:

    def __init__(self):
        self. manager = Management()


    def ran(self):

        path = Path(FILE_PATH)
        # loop that send all the files to kafka
        for file in path.iterdir():
            if file.is_file():
                json_metadata = Management.create_json(file.absolute())
                self.manager.send_to_kafka(json_metadata, TOPIC_NAME)

        # after the loop finish send all the data close the producer
        self.manager.producer.close_producer()


if __name__ == "__main__":
    pass










