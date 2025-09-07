from kafka_server.producer import Producer
from mutagen.wave import WAVE
from pathlib import Path
import json
import datetime

def get_metadata(path :Path):
    metadata = {
        "metadata": {
            "name": path.name,
            "size": path.stat().st_size,
            "create_at": datetime.datetime.fromtimestamp(path.stat().st_ctime).isoformat(),
            'absolute_path' :path.absolute()
        }
    }
    return metadata

path = Path('C:\podcasts\download (1).wav')
print(get_metadata(path))
