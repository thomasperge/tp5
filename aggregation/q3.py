import os
from os.path import join, dirname
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient
from pprint import pprint

# Chargement des variables d'environnement
dotenv_path = join(dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

MONGO_URI = os.environ.get('MONGO_URI', None)
DATABASE_NAME = os.environ.get('DATABASE_NAME', None)
SENSOR_DATA_COLLECTION_NAME = os.environ.get('SENSOR_DATA_COLLECTION_NAME', None)
DRONE_COLLECTION_NAME = os.environ.get('APIARY_COLLECTION_NAME', None)

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
sensor_col = db[SENSOR_DATA_COLLECTION_NAME]

def q3_last_5_measures_models():
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$limit": 5},
        {"$lookup": {
            "from": DRONE_COLLECTION_NAME,
            "localField": "drone_id",
            "foreignField": "drone_id",
            "as": "drone_info"
        }},
        {"$unwind": "$drone_info"},
        {"$project": {"_id": 0, "timestamp": 1, "drone_id": 1, "model": "$drone_info.model"}}
    ]
    result = list(sensor_col.aggregate(pipeline))
    pprint(result)
    return result

if __name__ == "__main__":
    q3_last_5_measures_models() 