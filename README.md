# the_muezzin_final_project

## Introduction:
Hi! this is my final project . In this project, I handle audio files, process them, make a pipeline, and save them in MongoDB. The file itself is processed with the audio processed in Elasticsearch, the information about the processed file, and everything goes through kafka server 

Think of it as:  
file(.wav) → Kafka → process → Save → Save → Elasticsearch & mongoDB

---

## What We Built

### 1.data_loader :
- A loop that goes through all files and opens them.
- for evary file take (with Path object) the metadata from the file.
- convert it to json
- Publishes them to Kafka.
- the maping looks like that:
   metadata = {
            "metadata": {
                "name": path.name,
                "size": path.stat().st_size,
                "create_at": datetime.datetime.fromtimestamp(path.stat().st_ctime).isoformat(),
                'absolute_path': str(path.absolute())
            }
        }

---




  "relevant_timestamp": "25/03/2020"
}
