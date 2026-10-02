"""
Carga los CSV crudos (historico + API) a MongoDB, SIN limpiar nada.
Procesa archivo por archivo (no carga todo en memoria a la vez),
para que funcione incluso con recursos limitados.
"""
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "firms_colombia")
COLECCION = "focos_calor_raw"

RAW_HISTORICO = Path("data/raw/historico")
RAW_API = Path("data/raw/api")


def origen_desde_nombre(nombre_archivo: str) -> str:
    if "_SP_" in nombre_archivo:
        return "api_sp"
    if "_NRT_" in nombre_archivo:
        return "api_nrt"
    return "historico"


def cargar_y_subir(carpeta: Path, coleccion, contador):
    for archivo in sorted(carpeta.glob("*.csv")):
        df = pd.read_csv(archivo)
        df["archivo_fuente"] = archivo.name
        df["origen"] = origen_desde_nombre(archivo.name)

        registros = df.to_dict("records")
        if registros:
            coleccion.insert_many(registros)

        contador["total"] += len(registros)
        print(f"  {archivo.name}: {len(registros):,} filas insertadas "
              f"(acumulado: {contador['total']:,})")

        del df, registros  # liberar memoria explicitamente


def main():
    print(f"Conectando a MongoDB ({MONGO_URI})...")
    client = MongoClient(MONGO_URI)
    coleccion = client[MONGO_DB][COLECCION]
    coleccion.drop()

    contador = {"total": 0}

    print("\nCargando historicos...")
    cargar_y_subir(RAW_HISTORICO, coleccion, contador)

    print("\nCargando API...")
    cargar_y_subir(RAW_API, coleccion, contador)

    total_en_mongo = coleccion.count_documents({})
    print(f"\nTotal de documentos en '{COLECCION}': {total_en_mongo:,}")

    print("\nConteo por origen:")
    for origen in ["historico", "api_sp", "api_nrt"]:
        n = coleccion.count_documents({"origen": origen})
        print(f"  {origen}: {n:,}")


if __name__ == "__main__":
    main()
