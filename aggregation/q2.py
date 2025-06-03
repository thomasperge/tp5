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

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
sensor_col = db[SENSOR_DATA_COLLECTION_NAME]

def q2_last_measure_per_drone():
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$drone_id",
            "last_measure": {"$first": "$$ROOT"}
        }}
    ]
    result = list(sensor_col.aggregate(pipeline))
    pprint(result)
    return result

if __name__ == "__main__":
    q2_last_measure_per_drone() 