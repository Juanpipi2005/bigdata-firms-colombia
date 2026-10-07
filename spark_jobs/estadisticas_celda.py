"""
Job de Spark: estadisticas de recurrencia por celda y marca de posibles
fuentes estaticas.

Lee:     focos_calor (coleccion limpia)
Escribe: celdas_recurrencia (una fila por celda; no modifica focos_calor)

Celda (definicion unica del proyecto):
    celda_lat = floor(latitude / 0.01), celda_lon = floor(longitude / 0.01)
0.01 grados son ~1.1 km en latitud; en longitud varia con la latitud.

Criterio: heuristica para identificar CANDIDATAS, no prueba de que sea una
fuente estatica. Umbrales tomados tal cual de clasificar_recurrencia.py:
    >= 500 detecciones, >= 300 dias, >= 60 meses, >= 7 anios distintos.
Las detecciones suman todos los satelites (desde 2018 hay dos viendo casi lo
mismo); dias, meses y anios distintos no se ven afectados por eso.
"""
from pyspark.sql import functions as F

from comun import crear_sesion

CELDA_GRADOS = 0.01
MIN_DETECCIONES = 500
MIN_DIAS = 300
MIN_MESES = 60
MIN_ANIOS = 7
SALIDA = "celdas_recurrencia"


def main():
    spark = crear_sesion("estadisticas-celda-recurrencia")
    df = spark.read.format("mongodb").load()

    dia = F.to_date("acq_date")
    celdas = (
        df.select(
            F.floor(F.col("latitude") / CELDA_GRADOS).cast("long").alias("celda_lat"),
            F.floor(F.col("longitude") / CELDA_GRADOS).cast("long").alias("celda_lon"),
            dia.alias("dia"),
            F.date_format(dia, "yyyy-MM").alias("mes"),
            F.year(dia).alias("anio"),
            "latitude",
            "longitude",
        )
        .groupBy("celda_lat", "celda_lon")
        .agg(
            F.count("*").alias("detecciones"),
            F.countDistinct("dia").alias("dias"),
            F.countDistinct("mes").alias("meses"),
            F.countDistinct("anio").alias("anios"),
            F.min("dia").alias("primera"),
            F.max("dia").alias("ultima"),
            F.round(F.avg("latitude"), 5).alias("lat_media"),
            F.round(F.avg("longitude"), 5).alias("lon_media"),
        )
        .withColumn("celda_id", F.concat_ws("_", "celda_lat", "celda_lon"))
        .withColumn(
            "posible_fuente_estatica",
            (F.col("detecciones") >= MIN_DETECCIONES)
            & (F.col("dias") >= MIN_DIAS)
            & (F.col("meses") >= MIN_MESES)
            & (F.col("anios") >= MIN_ANIOS),
        )
        .cache()
    )

    total_celdas = celdas.count()
    total_detecciones = celdas.agg(F.sum("detecciones")).first()[0]
    marcadas = celdas.filter("posible_fuente_estatica")
    n_marcadas = marcadas.count()
    det_marcadas = marcadas.agg(F.sum("detecciones")).first()[0] or 0

    celdas.write.format("mongodb").mode("overwrite").option("collection", SALIDA).save()

    print("\n" + "=" * 60)
    print("RESUMEN: recurrencia por celda")
    print("=" * 60)
    print(f"Detecciones procesadas:        {total_detecciones:,}")
    print(f"Celdas con detecciones:        {total_celdas:,}")
    print(f"Celdas marcadas (candidatas):  {n_marcadas:,}")
    print(f"Detecciones en esas celdas:    {det_marcadas:,} "
          f"({100 * det_marcadas / total_detecciones:.4f}%)")
    print(f"Escrito en la coleccion:       {SALIDA}")
    print("\nTop 5 celdas marcadas:")
    (marcadas.orderBy(F.desc("detecciones"))
        .select("celda_id", "detecciones", "dias", "meses", "anios", "lat_media", "lon_media")
        .show(5, truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
