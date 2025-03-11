import pymongo
import pytest
import os
import time
from pymongo import ReadPreference

PRIMARY_URI = os.getenv("MONGO_PRIMARY_URI", "mongodb://mongodb1:27017")
SECONDARY_URI = os.getenv("MONGO_SECONDARY_URI", "mongodb://mongodb2:27017")
DB_NAME = "weather_db"
COLLECTION_NAME = "weather_data"
TEST_DOC = {"id_station": "TestStation", "datetime": "2025-03-05T14:00:00", "weather_data": {"temperature": 22.5}}

@pytest.fixture(scope="module")
def primary_client():
    """Fixture pour la connexion au primaire."""
    client = pymongo.MongoClient(PRIMARY_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    yield collection
    client.close()

@pytest.fixture(scope="module")
def secondary_client():
    """Fixture pour la connexion au secondaire."""
    client = pymongo.MongoClient(SECONDARY_URI, read_preference=ReadPreference.SECONDARY)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    yield collection
    client.close()

def test_replication(primary_client, secondary_client):
    """Teste la réplication des données entre le primaire et le secondaire."""
    # Insérer un document test
    primary_client.insert_one(TEST_DOC)
    print("✅ Document inséré sur le primaire.")

    # Attente de la réplication
    time.sleep(5)

    # Vérifier la réplication
    replicated_doc = secondary_client.find_one({"id_station": "TestStation"})
    assert replicated_doc is not None, "❌ Le document n'a pas été répliqué."
    assert replicated_doc["datetime"] == "2025-03-05T14:00:00", "❌ Le document répliqué ne correspond pas."

    print("✅ Réplication réussie.")

    # Suppression et vérification de la réplication de la suppression
    primary_client.delete_one({"id_station": "TestStation"})
    time.sleep(5)
    replicated_doc_after_deletion = secondary_client.find_one({"id_station": "TestStation"})
    assert replicated_doc_after_deletion is None, "❌ Suppression non répliquée."

    print("✅ Suppression répliquée avec succès.")
