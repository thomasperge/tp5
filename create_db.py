import json
import os
from os.path import join, dirname
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient

# Chargement des variables d'environnement
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

MONGO_URI = os.environ.get('MONGO_URI', None)
DATABASE_NAME = os.environ.get('DATABASE_NAME', None)
DRONE_COLLECTION_NAME = os.environ.get('DRONE_COLLECTION_NAME', None)

# Connexion à MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Schéma de validation pour la collection 'drones'
drone_schema = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "drone_id",
            "model",
            "manufacturer",
            "battery_capacity_mAh",
            "deployment_zone",
            "active"
        ],
        "properties": {
            "drone_id": {"bsonType": "string", "description": "Identifiant unique du drone"},
            "model": {"bsonType": "string", "description": "Modèle du drone"},
            "manufacturer": {"bsonType": "string", "description": "Fabricant du drone"},
            "battery_capacity_mAh": {"bsonType": "int", "minimum": 0, "description": "Capacité de la batterie en mAh"},
            "deployment_zone": {"bsonType": "string", "description": "Zone de déploiement"},
            "active": {"bsonType": "bool", "description": "Drone actif ou non"}
        }
    }
}

# Suppression de la collection si elle existe déjà (pour relancer le script sans erreur)
if DRONE_COLLECTION_NAME in db.list_collection_names():
    db[DRONE_COLLECTION_NAME].drop()

# Création de la collection avec le schéma de validation
db.create_collection(
    DRONE_COLLECTION_NAME,
    validator=drone_schema
)

print(f"Collection '{DRONE_COLLECTION_NAME}' créée avec schéma de validation dans la base '{DATABASE_NAME}'.")

# Insertion des données du fichier drones.json
drones_file = 'drones.json'
with open(drones_file, 'r', encoding='utf-8') as f:
    drones_data = json.load(f)

result = db[DRONE_COLLECTION_NAME].insert_many(drones_data)
print(f"{len(result.inserted_ids)} documents insérés dans la collection '{DRONE_COLLECTION_NAME}'.") 