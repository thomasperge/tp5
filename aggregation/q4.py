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

# Q4 : Calculez le nombre total de mesures regroupées par fabricant de
# drones

def q4_count_by_manufacturer():
    pipeline = [
        {"$lookup": {
            "from": DRONE_COLLECTION_NAME,
            "localField": "drone_id",
            "foreignField": "drone_id",
            "as": "drone_info"
        }},
        {"$unwind": "$drone_info"},
        {"$group": {
            "_id": "$drone_info.manufacturer",
            "count": {"$sum": 1}
        }}
    ]
    result = list(sensor_col.aggregate(pipeline))
    pprint(result)
    return result

if __name__ == "__main__":
    q4_count_by_manufacturer() 