import base64
import os
from dotenv import load_dotenv
from utils.config import INDEX_NAME
from src.elastic_search_dal.elastic_servies import ElasticService
from utils.logger import Logger
from utils.config import NEGATIVE_WORDS_PATH, NEUTRAL_WORDS_PATH

load_dotenv()


class AnalyzerService:

    def __init__(self):
        self.negative_words_list = self.txt_file_to_list(NEGATIVE_WORDS_PATH)
        self.neutral_words_list = self.txt_file_to_list(NEUTRAL_WORDS_PATH)
        self.ec = ElasticService(os.getenv("ES_URL"), INDEX_NAME)
        self.logger = Logger.get_logger()

    def decoder(self, txt):
        """""
        A function that decoder the black list with base64
        """""
        base64_bytes = txt.encode("ascii")
        sample_string_bytes = base64.b64decode(base64_bytes)
        sample_string = sample_string_bytes.decode("ascii")
        self.logger.info(f"Decoded string: {sample_string}")
        return sample_string


    def txt_file_to_list(self, file_path):
        """""
        A function that get file path ,open it,decoder and return as a list of words
        """""
        with open(file_path, "r") as file:
            text = file.read()
        text = self.decoder(text)
        list_txt = text.split()
        return list_txt

    def get_file_from_elastic_by_id(self, unique_id):
        """""
        A function that get unique_id,and send queries to elastic
        """""
        field_name = 'unique_id'
        field_value = unique_id
        return self.ec.get_document_by_match(index_name=INDEX_NAME, field_name=field_name, field_value=field_value)

    def update_elastic_by_id(self, _id, document):
        """""
        A function that get unique_id,and update it in elastic
        """""
        self.ec.update_doc(_id, document)

    def classification(self, text: str):
        """""
        A function that classification text by logic of count / len_txt * 10
        """""
        count = 0
        len_txt = len(text)
        words = text.split()
        for word in words:
            if word in self.negative_words_list:
                count += 2
            if word in self.neutral_words_list:
                count += 1
        return (count / len_txt) * 10

    @staticmethod
    def is_bds_by_score(score):
        """""
        A boolean function that by score return danger or not
        """""
        is_bds = False
        if score > 10:
            is_bds = True
        return is_bds

    @staticmethod
    def get_bds_threat_level(score):
        """""
        A function that by score return level of danger
        """""
        if score < 3:
            level_danger = 'None'
        if score >= 3 & score < 7:
            level_danger = 'medium'
        else:
            level_danger = 'high'
        return level_danger
