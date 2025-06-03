import json
import os
from os.path import join, dirname
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient
import paho.mqtt.client as mqtt
from datetime import datetime

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

MONGO_URI = os.environ.get('MONGO_URI', None)
DATABASE_NAME = os.environ.get('DATABASE_NAME', None)
SENSOR_DATA_COLLECTION_NAME = os.environ.get('SENSOR_DATA_COLLECTION_NAME', None)

MQTT_BROKER = os.environ.get('MQTT_BROKER', None)
MQTT_PORT = int(os.environ.get('MQTT_PORT', None))
MQTT_TOPIC = os.environ.get('MQTT_TOPIC', None)

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Création de la collection timeseries si elle n'existe pas déjà
if SENSOR_DATA_COLLECTION_NAME not in db.list_collection_names():
    db.create_collection(
        SENSOR_DATA_COLLECTION_NAME,
        timeseries={
            'timeField': 'timestamp',  # Champ pour le temps
            'metaField': 'drone_id',    # Champ pour les métadonnées
            'granularity': 'seconds'     # Granularité des données
        }
    )
    print(f"Collection timeseries '{SENSOR_DATA_COLLECTION_NAME}' créée.")
else:
    print(f"Collection timeseries '{SENSOR_DATA_COLLECTION_NAME}' déjà existante.")

sensor_col = db[SENSOR_DATA_COLLECTION_NAME]

# Callback appelé à la réception d'un message MQTT
def on_message(client, userdata, msg):
    try:
        # Décodage du message reçu
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)  # Mettre en JSON la data reçue
        # Conversion du timestamp si besoin
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        # Insertion des données dans MongoDB
        sensor_col.insert_one(data)
        print(f"Message inséré : {data}")
    except Exception as e:
        print(f"Erreur lors du traitement du message : {e}")

# Configuration du client MQTT
mqtt_client = mqtt.Client()
mqtt_client.on_message = on_message  # Assignation de la fonction de rappel

# Connexion au broker MQTT et abonnement au topic
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.subscribe(MQTT_TOPIC)

print(f"En écoute sur le topic MQTT '{MQTT_TOPIC}'...")
# Boucle d'écoute pour recevoir les messages
mqtt_client.loop_forever() 