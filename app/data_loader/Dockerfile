FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY . .


COPY ../utils /app/utils
COPY ../instance_kafka/KafkaServerProducer /app
COPY data_loader/management/Management /app/data_loader


VOLUME ["/data/data_analyzer"]


CMD ["python","-m","main.py"]