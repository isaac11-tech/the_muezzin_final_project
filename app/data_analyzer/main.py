from ..instance_kafka import KafkaServer


class AnalyzerMain:

    def __init__(self, kafka_server: KafkaServer):
        self.consumer = kafka_server.consumer
