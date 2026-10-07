"""Parametros comunes de la comparativa Dask vs Spark.

Los umbrales son los mismos de spark_jobs/estadisticas_celda.py. Se repiten
aqui para que ambos motores usen exactamente la misma definicion.
"""
CELDA_GRADOS = 0.01
MIN_DETECCIONES = 500
MIN_DIAS = 300
MIN_MESES = 60
MIN_ANIOS = 7

# Resultado que ya obtuvo el job de Spark sobre focos_calor (2.333.350 documentos).
# Sirve de control: si un motor da otra cifra, la comparacion no es valida.
ESPERADO = {
    "celdas": 327057,
    "detecciones": 2333350,
    "candidatas": 60,
    "detecciones_candidatas": 84272,
}


def control(resultado):
    """Devuelve (ok, diferencias) comparando con ESPERADO."""
    difs = {k: (resultado[k], v) for k, v in ESPERADO.items() if resultado[k] != v}
    return (not difs), difs
