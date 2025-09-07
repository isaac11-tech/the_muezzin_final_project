from management import Management
from kafka_server.producer import Producer


if __name__ == "__main__":

    manager = Management()


    'C:\podcasts\download (1).wav'

    for i in range(1,34):#this length of file that we get

        json = manager.create_json(f'C:\podcasts\download ({i}).wav',f"audio_no_{i}")
        manager.send_to_kafka(json, topic="info_audio")





