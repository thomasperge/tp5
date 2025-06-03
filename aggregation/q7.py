import os
from os.path import join, dirname
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient

# Chargement des variables d'environnement
dotenv_path = join(dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

MONGO_URI = os.environ.get('MONGO_URI', None)
DATABASE_NAME = os.environ.get('DATABASE_NAME', None)
SENSOR_DATA_COLLECTION_NAME = os.environ.get('SENSOR_DATA_COLLECTION_NAME', None)
DRONE_COLLECTION_NAME = os.environ.get('APIARY_COLLECTION_NAME', None)

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Q7 : Créez une vue contenant chaque drone et leur dernière donnée
# générée par les drones actifs.


def q7_create_active_drones_last_data_view():
    pipeline = [
        {"$match": {"active": True}},
        {"$lookup": {
            "from": SENSOR_DATA_COLLECTION_NAME,
            "let": {"drone_id": "$drone_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$drone_id", "$$drone_id"]}}},
                {"$sort": {"timestamp": -1}},
                {"$limit": 1}
            ],
            "as": "last_data"
        }},
        {"$unwind": {"path": "$last_data", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 1,
            "drone_id": 1,
            "model": 1,
            "manufacturer": 1,
            "battery_capacity_mAh": 1,
            "deployment_zone": 1,
            "active": 1,
            "last_data": 1
        }}
    ]
    try:
        db.drop_collection('active_drones_last_data')
    except Exception:
        pass
    db.command({
        "create": "active_drones_last_data",
        "viewOn": DRONE_COLLECTION_NAME,
        "pipeline": pipeline
    })
    print("Vue 'active_drones_last_data' créée avec succès.")

if __name__ == "__main__":
    q7_create_active_drones_last_data_view() 