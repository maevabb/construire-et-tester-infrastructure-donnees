#%%
import boto3
import json
from pymongo import MongoClient
from pymongo.errors import WriteError
from bson import ObjectId
from datetime import datetime
from bson import Timestamp
import pandas as pd

#%%
# Configurer le client S3
s3 = boto3.client('s3')

# Détails du bucket et des fichiers JSON
bucket_name = "p8-airbyte-greenandcoop"
stations_file_key = "data_transformed/stations.json"  # Fichier JSON des stations
hourly_data_file_key = "data_transformed/hourly_data.json"  # Fichier JSON des données horaires

#%%
# Fonction pour charger les données depuis S3 dans un objet Python
def load_json_from_s3(bucket_name, file_key):
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')  # Lire et décoder en UTF-8
    return json.loads(data)  # Convertir en objet Python (dictionnaire ou liste)

# Charger les données des fichiers JSON
stations_data = load_json_from_s3(bucket_name, stations_file_key)
hourly_data = load_json_from_s3(bucket_name, hourly_data_file_key)

#%%
#Connexion à MongoDB
client = MongoClient("mongodb://mongodb1:27017,mongodb2:27017,mongodb3:27017/?replicaSet=rs0") #connexion au serveur mongodb
db = client["weather_data"]  # Nom de la base de données

#%%
# Définition des schémas
stations_schema = {
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["id_station", "name", "latitude", "longitude", "elevation"],
            "properties": {
                "id_station": {"bsonType": "string"},
                "name": {"bsonType": "string"},
                "latitude": {"bsonType": "double", "minimum": -90, "maximum": 90},
                "longitude": {"bsonType": "double", "minimum": -180, "maximum": 180},
                "elevation": {"bsonType": "int", "minimum": 0},
                "city": {"bsonType": "string"},
                "state": {"bsonType": "string"},
                "hardware": {"bsonType": "string"},
                "software": {"bsonType": "string"}
            }
        }
    }
}

hourly_data_schema = {
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["id_station", "datetime"],
            "properties": {
                "id_station": {"bsonType": "string"},
                "datetime": {"bsonType": "string"},
                "temperature": {"bsonType": "double"},
                "pressure": {"bsonType": "double"},
                "humidity": {"bsonType": "int", "minimum": 0, "maximum": 100},
                "dew_point": {"bsonType": "double"},
                "visibility": {"bsonType": ["double", "null"], "minimum": 0},
                "wind_speed": {"bsonType": "double", "minimum": 0},
                "wind_gust": {"bsonType": ["double", "null"], "minimum": 0},
                "wind_direction": {"bsonType": ["double", "null"], "minimum": 0, "maximum": 337.5},
                "precip_1h": {"bsonType": ["double", "null"], "minimum": 0},
                "precip_3h": {"bsonType": ["double", "null"], "minimum": 0},
                "precip_accum": {"bsonType": ["double", "null"], "minimum": 0},
                "precip_rate": {"bsonType": ["double", "null"], "minimum": 0},
                "solar": {"bsonType": "double", "minimum": 0},
                "uv": {"bsonType": "int", "minimum": 0, "maximum": 11},
                "snow_depth": {"bsonType": ["double", "null"], "minimum": 0},
                "nebulosity": {"bsonType": ["double", "null"], "minimum": 0},
                "weather_wmo": {"bsonType": ["double", "null"], "minimum": 0}
            }
        }
    }
}

#%%
# Suppression des collections existantes
db.drop_collection("stations")
db.drop_collection("hourly_data")

# Création des collections avec validation
db.create_collection("stations", **stations_schema)
db.create_collection("hourly_data", **hourly_data_schema)

stations_collection = db["stations"]  
hourly_data_collection = db["hourly_data"]

#%%
# Vérifier et corriger les valeurs NaN en None dans les champs numériques
for document in hourly_data:
# Liste des champs numériques à vérifier
    numeric_fields = ['wind_direction', 'temperature', 'pressure', 'humidity', 'dew_point', 'visibility', 'wind_speed', 'wind_gust', 'precip_1h', 'precip_3h', 'snow_depth', 'nebulosity', 'weather_wmo']

    # Vérifier et corriger les valeurs NaN pour chaque champ numérique
    for field in numeric_fields:
        if isinstance(document.get(field), float) and document[field] != document[field]:  # Vérifier NaN
            document[field] = None

#%%
def insert_documents(collection, data, collection_name):
    total_docs = len(data)
    inserted_count = 0
    rejected_docs = []
    
    for doc in data:
        try:
            collection.insert_one(doc)
            inserted_count += 1
        except WriteError as e:
            print(f"Document rejeté : {doc} - Erreur : {e}")
            rejected_docs.append(doc)
    
    success_rate = (inserted_count / total_docs) * 100 if total_docs else 0
    print(f"{collection_name.upper()}: ")
    print(f"Total de documents dans le fichier : {total_docs}")
    print(f"Nombre de documents insérés : {inserted_count}")
    print(f"Nombre de documents rejetés : {len(rejected_docs)}")
    print(f"Taux de succès de l'insertion : {success_rate:.2f}%")

    if rejected_docs:
        s3_object_key = f"rejected_doc/{collection_name}_rejected_docs.json"
        rejected_json = json.dumps(rejected_docs, indent=4, default=str)
        s3.put_object(Bucket=bucket_name, Key=s3_object_key, Body=rejected_json, ContentType="application/json")
        print(f"Documents {collection_name} rejetés sauvegardés sur S3 : s3://{bucket_name}/{s3_object_key}")
    
    return inserted_count, len(rejected_docs)

insert_documents(stations_collection, stations_data, "stations")
insert_documents(hourly_data_collection, hourly_data, "hourly_data")
# %%
