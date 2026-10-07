"""
Comparativa Dask vs Spark - lado Spark.

Misma operacion que spark_jobs/estadisticas_celda.py (agregacion por celda),
pero solo calcula y resume: NO escribe en Mongo.

Uso (desde la raiz del repo):  python3 comparativa/spark_celdas.py <cores>
    cores: 1 o 2 (spark.cores.max)

El tiempo cubre desde la lectura de Mongo hasta tener el resumen (incluye la
inferencia del esquema); no incluye el arranque de la sesion de Spark.
"""
import json
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "spark_jobs"))
sys.path.insert(0, str(RAIZ / "comparativa"))

from pyspark.sql import functions as F

from comun import crear_sesion
from parametros import (CELDA_GRADOS, MIN_ANIOS, MIN_DETECCIONES, MIN_DIAS,
                        MIN_MESES, control)


def main():
    cores = sys.argv[1] if len(sys.argv) > 1 else "2"
    spark = crear_sesion(f"comparativa-spark-{cores}core", cores_max=cores)

    inicio = time.perf_counter()
    df = spark.read.format("mongodb").load()
    dia = F.to_date("acq_date")
    celdas = (
        df.select(
            F.floor(F.col("latitude") / CELDA_GRADOS).cast("long").alias("celda_lat"),
            F.floor(F.col("longitude") / CELDA_GRADOS).cast("long").alias("celda_lon"),
            dia.alias("dia"),
            F.date_format(dia, "yyyy-MM").alias("mes"),
            F.year(dia).alias("anio"),
        )
        .groupBy("celda_lat", "celda_lon")
        .agg(
            F.count("*").alias("detecciones"),
            F.countDistinct("dia").alias("dias"),
            F.countDistinct("mes").alias("meses"),
            F.countDistinct("anio").alias("anios"),
        )
        .withColumn(
            "candidata",
            (F.col("detecciones") >= MIN_DETECCIONES)
            & (F.col("dias") >= MIN_DIAS)
            & (F.col("meses") >= MIN_MESES)
            & (F.col("anios") >= MIN_ANIOS),
        )
    )
    fila = celdas.agg(
        F.count("*").alias("celdas"),
        F.sum("detecciones").alias("detecciones"),
        F.sum(F.col("candidata").cast("int")).alias("candidatas"),
        F.sum(F.when(F.col("candidata"), F.col("detecciones")).otherwise(0)).alias("detecciones_candidatas"),
    ).first()
    segundos = time.perf_counter() - inicio

    resultado = {k: int(fila[k]) for k in
                 ("celdas", "detecciones", "candidatas", "detecciones_candidatas")}
    ok, difs = control(resultado)
    resultado.update({"motor": "spark", "recursos": f"{cores} core(s)",
                      "segundos": round(segundos, 2), "control_ok": ok})
    print("RESULTADO " + json.dumps(resultado))
    if not ok:
        print("DIFERENCIAS (obtenido, esperado):", difs)
    spark.stop()


if __name__ == "__main__":
    main()
