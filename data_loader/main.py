from management import Management
from pathlib import Path
from kafka_server.producer import Producer


if __name__ == "__main__":

    manager = Management()


    path = Path("C:/podcasts")

    #loop that send all the files to kafka
    for file in path.iterdir():
        if file.is_file():
            json_metadata = Management.create_json(file.absolute())
            manager.send_to_kafka(json_metadata,"audio_metadata")

    #after the loop finish send all the data close the producer
    manager.producer.close_producer()






