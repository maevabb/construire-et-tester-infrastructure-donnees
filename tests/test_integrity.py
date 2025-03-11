import pytest
import random
import json
import boto3
from pymongo import MongoClient

# Configuration MongoDB
MONGO_URI = "mongodb://mongodb1:27017,mongodb2:27017,mongodb3:27017/?replicaSet=rs0"
client = MongoClient(MONGO_URI)
db = client["weather_db"]
collection = db["weather_data"]

# Configuration du client S3
s3 = boto3.client('s3')

# Détails du bucket et des fichiers JSON
BUCKET_NAME = "p8-airbyte-greenandcoop"
WEATHER_DATA_FILE_KEY = "data_transformed/weather_data.json"

def load_json_from_s3(bucket_name, file_key):
    """Charge un fichier JSON depuis S3 et retourne les données sous forme d'objet Python."""
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    return json.loads(data)

@pytest.fixture(scope="module")
def json_data():
    """Fixture qui charge les données JSON depuis S3 une seule fois par session de test."""
    return load_json_from_s3(BUCKET_NAME, WEATHER_DATA_FILE_KEY)

def test_count_records(json_data):
    """Teste si le nombre d'enregistrements JSON correspond à celui de MongoDB."""
    count_json = len(json_data)
    count_mongo = collection.count_documents({})

    print(f"\nNombre de lignes dans le JSON : {count_json}")
    print(f"Nombre de documents dans MongoDB : {count_mongo}")

    assert count_json == count_mongo, "🚨 Incohérence dans le nombre d'enregistrements !"
    print("✅ Vérification du nombre d'enregistrements OK")

def test_data_types(json_data):
    """Teste si les types de données dans MongoDB correspondent à ceux du JSON."""
    sample_doc = collection.find_one()
    assert sample_doc, "🚨 Aucun document trouvé dans MongoDB !"

    for key in json_data[0].keys():
        json_dtype = type(json_data[0][key]).__name__
        mongo_dtype = type(sample_doc.get(key, None)).__name__
        
        print(f"Clé '{key}': JSON = {json_dtype}, MongoDB = {mongo_dtype}")
        assert json_dtype == mongo_dtype, f"🚨 Mismatch sur '{key}' : JSON={json_dtype}, MongoDB={mongo_dtype}"

    print("✅ Vérification des types de données OK")

def test_random_record_comparison(json_data):
    """Teste si un document aléatoire dans le JSON est bien répliqué et identique dans MongoDB."""
    random_index = random.randint(0, len(json_data) - 1)
    json_doc = json_data[random_index]

    mongo_doc = collection.find_one({"id_station": json_doc["id_station"], "datetime": json_doc["datetime"]})

    assert mongo_doc, f"🚨 Document introuvable dans MongoDB : {json_doc}"

    for key in json_doc.keys():
        json_value = json_doc[key]
        mongo_value = mongo_doc.get(key, None)

        assert json_value == mongo_value, f"🚨 Différence trouvée dans '{key}' : JSON={json_value}, MongoDB={mongo_value}"

    print("✅ Vérification des valeurs OK")
