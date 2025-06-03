import os
from os.path import join, dirname
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta, UTC
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

def q1_temperature_hourly_last_12h():
    since = datetime.now(UTC) - timedelta(hours=12)
    pipeline = [
        {"$match": {"timestamp": {"$gte": since}}},
        {"$group": {
            "_id": {"hour": {"$hour": "$timestamp"}, "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}}},
            "avg_temp": {"$avg": "$temperature"}
        }},
        {"$sort": {"_id.date": 1, "_id.hour": 1}}
    ]
    result = list(sensor_col.aggregate(pipeline))
    pprint(result)
    return result

if __name__ == "__main__":
    q1_temperature_hourly_last_12h() 