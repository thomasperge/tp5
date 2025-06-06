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

# Q5 : Comptez le nombre de mesures effectuées dans un rayon de 3 km
# autour du Parc Montsouris (coordonnées : 48.8210, 2.3388).

def q5_count_near_montsouris():
    # Pipeline d'agrégation :
    # 1. $geoNear : Chercher les mesures dont la localisation est dans un rayon de 3 km autour du Parc Montsouris
    #    (centre : 48.8210, 2.3388, champ géospatial : location)
    # 2. $count : Compter le nombre de mesures trouvées
    db[SENSOR_DATA_COLLECTION_NAME].create_index([("location", "2dsphere")])
    center = {"type": "Point", "coordinates": [2.3388, 48.8210]}
    pipeline = [
        {"$geoNear": {
            "near": center,
            "distanceField": "distance",
            "maxDistance": 3000,
            "spherical": True,
            "key": "location"
        }},
        {"$count": "count"}
    ]
    result = list(sensor_col.aggregate(pipeline))
    pprint(result)
    return result

if __name__ == "__main__":
    q5_count_near_montsouris() 