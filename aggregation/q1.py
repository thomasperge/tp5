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

# Q1 :Affichez l'évolution horaire de la température moyenne enregistrée
# par l'ensemble des drones durant les 12 derinères heures.

def q1_temperature_hourly_last_12h():
    # Pipeline d'agrégation :
    # 1. $match : Ne garder que les mesures des 12 dernières heures (filtre sur le champ timestamp)
    # 2. $group : Regrouper par heure et par date, et calculer la température moyenne pour chaque groupe
    # 3. $sort : Trier les résultats par date puis par heure croissante
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