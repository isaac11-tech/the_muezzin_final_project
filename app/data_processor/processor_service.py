import uuid
import json
import os
import speech_recognition as sr
from utils.config import INDEX_NAME
from src.elastic_search_dal.elastic_servies import ElasticService
from src.mongodb_dal.mongo_service import MongoService
from utils.config import DB_NAME, COLLECTION_NAME
from utils.logger import Logger
from dotenv import load_dotenv

load_dotenv()


class DataService:

    def __init__(self):
        # create connection to Elastic
        self.elastic_conn = ElasticService(os.getenv('ES_URL'), INDEX_NAME)
        self.mongo_conn = MongoService(DB_NAME, COLLECTION_NAME)
        self.logger = Logger.get_logger()



    @staticmethod
    def add_unique_id(data: json):
        """""
        function that adding unique_id to file
        """""
        data['unique_id'] = str(uuid.uuid4())
        return data


    def send_metadata_to_elasticsearch(self, index_name, _id, document):
        """""
        function that send metadata to elasticsearch
        """""
        try:
            response = self.elastic_conn.insert_document(index=index_name, _id=_id, body=document)
            self.logger.info(f"data Sent successfully to elasticsearch response : {response}")
        except Exception as e:
            self.logger.error("Error to connect to elasticsearch, messages:", e)



    def get_file_by_path(self, file_path):
        """""
        function that get path and return file
        """""
        try:
            with open(file_path, 'rb') as file:
                f = file.read()
                return f
        except Exception as e:
            self.logger.error("Error to open the file, messages:", e)


    def send_file_to_mongodb(self, unique_id, file):
        """""
        function that send the file and id to mongodb
        """""
        try:
            data = {unique_id: file}
            result = self.mongo_conn.insert_one(data)
            self.logger.info(f"this the responds from mongodb:{result}")
        except Exception as e:
            self.logger.error("error to send to mongodb,massage:", e)


    def audio_to_txt(self, file_path):
        """""
        function that get a path of audio_file and return transcribed text
        """""
        recognizer = sr.Recognizer()
        with sr.AudioFile(file_path) as source:
            audio_data = recognizer.record(source)
            self.logger.info("Reading audio...")
        try:
            self.logger.info("Recognized Text:")
            text = recognizer.recognize_google(audio_data)
            return text
        except sr.UnknownValueError:
            self.logger.error("sorry, could not understand the audio.")
        except sr.RequestError:
            self.logger.error("could not connect to Google API.")


    @staticmethod
    def add_transcribed_audio(data: json, transcribed_audio):
        """""
        function that adding unique_id to file
        """""
        data['transcribed_audio'] = transcribed_audio
        return data
