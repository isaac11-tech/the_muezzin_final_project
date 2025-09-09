import json
from kafka import KafkaConsumer


class Consumer:

    def __init__(self, topic,host):

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[host],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )


