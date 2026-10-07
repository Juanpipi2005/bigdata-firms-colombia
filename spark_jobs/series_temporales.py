"""
Job de Spark: series temporales de focos de calor.

Lee:     focos_calor, celdas_recurrencia
Escribe: serie_mensual, serie_horaria (no modifica focos_calor)

Un solo sensor por anio, para no contar dos veces lo que ven dos satelites:
    2012-2018 -> N   (Suomi-NPP)
    2019-hoy  -> N20 (NOAA-20)
En 2019-2024 ambos ven casi lo mismo (diferencia de conteos < 3%), asi que el
cambio de sensor casi no mueve el nivel de la serie. Sumar ambos duplicaria
esos anios.

Fechas: se usa fecha_hora_colombia (hora local). 2026 es un anio parcial
(hay datos hasta el 2 de octubre; octubre es un mes parcial).

Hora local: VIIRS es un satelite de orbita polar y pasa por Colombia a horas
casi fijas. serie_horaria refleja sobre todo esos horarios de paso, NO el
ciclo diario real de los incendios.

detecciones_sin_estaticas excluye las celdas marcadas como posible fuente
estatica en celdas_recurrencia (minas, quemas industriales...).
"""
from pyspark.sql import functions as F

from comun import crear_sesion

CELDA_GRADOS = 0.01
ANIO_CAMBIO_SENSOR = 2019
SENSOR_ANTES = "N"
SENSOR_DESPUES = "N20"


def main():
    spark = crear_sesion("series-temporales")

    focos = spark.read.format("mongodb").load()
    estaticas = (
        spark.read.format("mongodb")
        .option("collection", "celdas_recurrencia")
        .load()
        .filter(F.col("posible_fuente_estatica"))
        .select("celda_lat", "celda_lon")
        .withColumn("es_estatica", F.lit(True))
    )

    anio = F.year("fecha_hora_colombia")
    df = (
        focos.filter(
            ((anio < ANIO_CAMBIO_SENSOR) & (F.col("satellite") == SENSOR_ANTES))
            | ((anio >= ANIO_CAMBIO_SENSOR) & (F.col("satellite") == SENSOR_DESPUES))
        )
        .withColumn("celda_lat", F.floor(F.col("latitude") / CELDA_GRADOS).cast("long"))
        .withColumn("celda_lon", F.floor(F.col("longitude") / CELDA_GRADOS).cast("long"))
        .join(F.broadcast(estaticas), ["celda_lat", "celda_lon"], "left")
        .withColumn("es_estatica", F.coalesce(F.col("es_estatica"), F.lit(False)))
        .cache()
    )

    mensual = (
        df.groupBy(
            F.date_format("fecha_hora_colombia", "yyyy-MM").alias("mes"),
            F.col("satellite").alias("sensor"),
        )
        .agg(
            F.count("*").alias("detecciones_total"),
            F.sum(F.when(~F.col("es_estatica"), 1).otherwise(0)).alias("detecciones_sin_estaticas"),
            F.round(F.avg("frp"), 2).alias("frp_promedio"),
        )
        .orderBy("mes")
        .cache()
    )

    horaria = (
        df.groupBy(F.hour("fecha_hora_colombia").alias("hora_local"))
        .agg(
            F.count("*").alias("detecciones"),
            F.round(F.avg("frp"), 2).alias("frp_promedio"),
        )
        .orderBy("hora_local")
        .cache()
    )

    total = df.count()
    n_estaticas = df.filter("es_estatica").count()
    n_meses = mensual.count()

    mensual.write.format("mongodb").mode("overwrite").option("collection", "serie_mensual").save()
    horaria.write.format("mongodb").mode("overwrite").option("collection", "serie_horaria").save()

    print("\n" + "=" * 60)
    print("RESUMEN: series temporales (un sensor por anio)")
    print("=" * 60)
    print(f"Detecciones usadas:            {total:,}")
    print(f"En celdas candidatas:          {n_estaticas:,} ({100 * n_estaticas / total:.2f}%)")
    print(f"Meses en serie_mensual:        {n_meses}")
    print("Colecciones escritas:          serie_mensual, serie_horaria")
    print("\nTop 5 meses por detecciones_total:")
    mensual.orderBy(F.desc("detecciones_total")).show(5, truncate=False)
    print("Distribucion por hora local:")
    horaria.show(24)
    spark.stop()


if __name__ == "__main__":
    main()
