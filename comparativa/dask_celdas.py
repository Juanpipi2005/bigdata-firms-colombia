"""
Comparativa Dask vs Spark - lado Dask.

Misma operacion que spark_celdas.py (agregacion por celda), leyendo de Mongo.
Dask no trae lector de Mongo, asi que cada tarea lee UN ANIO completo de
focos_calor (filtrando por acq_date, que tiene indice). Como cada anio esta
entero en una sola tarea, los dias, meses y anios distintos por celda salen
exactos dentro de la tarea, y el resto es solo sumar.

Uso (desde la raiz del repo):  python3 comparativa/dask_celdas.py
Los recursos (cuantos workers) se leen del scheduler.

El tiempo cubre desde la consulta del rango de anios hasta tener el resumen.
No incluye la conexion al scheduler.
"""
import json
import os
import sys
import time
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import dask
import dask.dataframe as dd
import pandas as pd
from distributed import Client
from pymongo import MongoClient

from parametros import (CELDA_GRADOS, MIN_ANIOS, MIN_DETECCIONES, MIN_DIAS,
                        MIN_MESES, control)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "firms_colombia")
SCHEDULER = os.environ.get("DASK_SCHEDULER", "tcp://dask-scheduler:8786")
COLUMNAS = ["celda_lat", "celda_lon", "detecciones", "dias", "meses", "anios"]


def leer_anio(uri, db, anio, grados):
    """Tarea de Dask: lee un anio completo de focos_calor y lo resume por celda."""
    from datetime import datetime

    import numpy as np
    import pandas as pd
    from pymongo import MongoClient

    columnas = ["celda_lat", "celda_lon", "detecciones", "dias", "meses", "anios"]
    cliente = MongoClient(uri)
    try:
        cursor = cliente[db]["focos_calor"].find(
            {"acq_date": {"$gte": datetime(anio, 1, 1), "$lt": datetime(anio + 1, 1, 1)}},
            {"_id": 0, "latitude": 1, "longitude": 1, "acq_date": 1},
        )
        df = pd.DataFrame(list(cursor))
    finally:
        cliente.close()

    if df.empty:
        return pd.DataFrame({c: pd.Series(dtype="int64") for c in columnas})

    df["celda_lat"] = np.floor(df["latitude"] / grados).astype("int64")
    df["celda_lon"] = np.floor(df["longitude"] / grados).astype("int64")
    df["mes"] = df["acq_date"].dt.month
    resumen = (
        df.groupby(["celda_lat", "celda_lon"])
        .agg(detecciones=("acq_date", "size"),
             dias=("acq_date", "nunique"),
             meses=("mes", "nunique"))
        .reset_index()
    )
    resumen["anios"] = 1  # esta celda aparecio en este anio
    return resumen[columnas].astype("int64")


def main():
    warnings.filterwarnings("ignore")
    cliente = Client(SCHEDULER, timeout=30)
    workers = cliente.scheduler_info()["workers"]
    if not workers:
        sys.exit("El scheduler no tiene workers registrados")
    hilos = sum(w["nthreads"] for w in workers.values())

    inicio = time.perf_counter()
    mongo = MongoClient(MONGO_URI)
    coleccion = mongo[MONGO_DB]["focos_calor"]
    primero = coleccion.find_one({}, {"_id": 0, "acq_date": 1}, sort=[("acq_date", 1)])["acq_date"].year
    ultimo = coleccion.find_one({}, {"_id": 0, "acq_date": 1}, sort=[("acq_date", -1)])["acq_date"].year
    mongo.close()

    meta = pd.DataFrame({c: pd.Series(dtype="int64") for c in COLUMNAS})
    tareas = [dask.delayed(leer_anio)(MONGO_URI, MONGO_DB, anio, CELDA_GRADOS)
              for anio in range(primero, ultimo + 1)]
    por_celda = dd.from_delayed(tareas, meta=meta).groupby(["celda_lat", "celda_lon"]).sum().compute()

    candidata = ((por_celda["detecciones"] >= MIN_DETECCIONES)
                 & (por_celda["dias"] >= MIN_DIAS)
                 & (por_celda["meses"] >= MIN_MESES)
                 & (por_celda["anios"] >= MIN_ANIOS))
    resultado = {
        "celdas": int(len(por_celda)),
        "detecciones": int(por_celda["detecciones"].sum()),
        "candidatas": int(candidata.sum()),
        "detecciones_candidatas": int(por_celda.loc[candidata, "detecciones"].sum()),
    }
    segundos = time.perf_counter() - inicio

    ok, difs = control(resultado)
    resultado.update({"motor": "dask",
                      "recursos": f"{len(workers)} worker(s) x {hilos // len(workers)} hilos",
                      "tareas": len(tareas),
                      "segundos": round(segundos, 2), "control_ok": ok})
    print("RESULTADO " + json.dumps(resultado))
    if not ok:
        print("DIFERENCIAS (obtenido, esperado):", difs)
    cliente.close()


if __name__ == "__main__":
    main()
