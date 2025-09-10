FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY . .


COPY ../utils /app/utils
COPY ../instance_kafka/KafkaServerProducer /app
COPY ../instance_kafka/KafkaServerConsumer /app
COPY data_processor/processor_service/DataService /app/data_processor


VOLUME ["/data/data_processor"]


CMD ["python","-m","main.py"]