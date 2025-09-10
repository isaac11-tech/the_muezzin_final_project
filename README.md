# the_muezzin_final_project

## Introduction:
Hi! this is my final project . In this project, I handle audio files, process them,Analyze them, make a pipeline, and save the file in MongoDB.the metadata about the file, including classification and hazard analysis, will be stored in Elasticsearch.
Think of it as:  
file(.wav) → Kafka → process → Save → Elasticsearch & mongoDB → Analysis → Elasticsearch

├── README.md

├── __pycache__

├── app

│   ├── __pycache__

│   ├── data_analyzer
│   ├── data_loader
│   ├── data_processor
│   ├── instance_kafka.py
│   └── main.py
├── data
│   ├── negative_words
│   └── neutral_words
├── docker-compose.yml
├── requirements.txt
├── src
│   ├── __init__.py
│   ├── __pycache__
│   ├── elastic_search_dal
│   ├── kafka_server
│   └── mongodb_dal
└── utils
    ├── __init__.py
    ├── __pycache__
    ├── config.py
    └── logger.py

---

## What We Built

### 1.data_loader :
- A loop that goes through all files and opens them.
- for every file take (with Pathlib) the metadata from the file.
- convert it to json
- Publishes them to Kafka.
- the mapping looks like that:
   metadata = {
            "metadata": {
                "name": path.name,
                "size": path.stat().st_size,
                "create_at": datetime.datetime.fromtimestamp(path.stat().st_ctime).isoformat(),
                'absolute_path': str(path.absolute())
            }
        }

---
### 2.data_processor :
-A loop that runs all the time and listens to the consumer
  - Every file you receive runs a process on it:
    - Adds a unique id to the metadata
    - Transcribes audio to text and adds to the metadata
    - send the metadata to elasticsearch
    - by the path of the file send to mongodb uniqueid + file
      
---
### 3.data_analyzer :
-A loop that runs all the time and listens to the consumer
-According to the ID that KAFKA receives, it pulls the metadata from Elasticsearch.
-pull the 'transcribes_txt' from the metadata
-Classifies the danger by the amount of dangerous words by logic of (count of dangers word/ len_txt) * 10
-update the new data in Elasticsearch

---
### 4.src :
-I built a small library that holds all the basic structure of our serves
- Elasticsearch dal with connection to service
- kafka that have basic producer and consumer
- mongo dal for connection and CRUD










