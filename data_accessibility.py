# %%
import time
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os

# Configuration du logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Paramètres de connexion à MongoDB
MONGO_URI = "mongodb://mongodb1:27017,mongodb2:27017,mongodb3:27017/?replicaSet=rs0"
DB_NAME = "weather_db"
COLLECTION_NAME = "weather_data"

# %%
# Connexion à MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    client.admin.command('ping')
    logging.info("\n Connexion réussie à MongoDB")
except ConnectionFailure as e:
    logging.error(f"\n Impossible de se connecter à MongoDB : {e}")
    exit(1)

# %%
# Mesurer le temps d'exécution d'une requête
query = {"datetime": {"$regex": "^2024-10-02"}, "id_station": "ILAMAD25"}  # Ex : données météo du 2 octobre 2024 à La Madeleine
start_time = time.time()
result = list(db[COLLECTION_NAME].find(query))
elapsed_time = round((time.time() - start_time) * 1000, 2)  # Convertir en ms

logging.info(f"Temps d'accès aux données : {elapsed_time} ms - Documents retournés : {len(result)}")
logging.info("Test d'accessibilité terminé avec succès !")