FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY . .

COPY ../utils /app/utils
COPY ../instance_kafka/KafkaServerConsumer /app
COPY data_analyzer/analyzer_service/AnalyzerService /app/data_analyzer


VOLUME ["/data/data_analyzer"]


CMD ["python","-m","main.py"]