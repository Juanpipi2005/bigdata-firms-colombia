from flask import Flask, jsonify
from pymongo import MongoClient
import os

app = Flask(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "firms_colombia")

@app.route("/health")
def health():
    estado_mongo = "desconocido"
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
        estado_mongo = "conectado"
    except Exception as e:
        estado_mongo = f"error: {e}"

    return jsonify({
        "status": "ok",
        "mongo": estado_mongo,
        "db": MONGO_DB
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
