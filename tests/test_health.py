import os
import requests

# La URL se puede sobreescribir con una variable de entorno;
# por defecto asume que corre dentro de la red de compose (nombre del servicio).
API_URL = os.environ.get("API_URL", "http://api:5000")


def test_health_responde_200():
    r = requests.get(f"{API_URL}/health", timeout=10)
    assert r.status_code == 200


def test_health_status_ok():
    r = requests.get(f"{API_URL}/health", timeout=10)
    data = r.json()
    assert data["status"] == "ok"


def test_health_mongo_conectado():
    r = requests.get(f"{API_URL}/health", timeout=10)
    data = r.json()
    assert data["mongo"] == "conectado"
