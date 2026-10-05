"""
Clasificación de posibles fuentes estáticas mediante recurrencia espacial.

Fuente:
    MongoDB -> focos_calor

Salida:
    MongoDB -> focos_calor_clasificados

IMPORTANTE:
    - No elimina registros.
    - Conserva todos los focos de calor.
    - Agrega información de recurrencia espacial.
"""

import os

from dotenv import load_dotenv
from pymongo import MongoClient, ReplaceOne


load_dotenv()


MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://mongo:27017",
)

MONGO_DB = os.environ.get(
    "MONGO_DB",
    "firms_colombia",
)

COLECCION_ORIGEN = "focos_calor"
COLECCION_SALIDA = "focos_calor_clasificados"

TAM_CELDA = 0.01

# Criterio conservador de posible fuente estática.
MIN_DETECCIONES = 500
MIN_DIAS = 300
MIN_MESES = 60
MIN_ANIOS = 7

BATCH_SIZE = 100_000


def crear_pipeline_recurrencia():
    """
    Crea la agregación de MongoDB que calcula
    la recurrencia de cada celda espacial.
    """

    return [

        {
            "$match": {
                "dentro_colombia": True,
                "fecha_hora_colombia": {
                    "$ne": None
                },
                "latitude": {
                    "$ne": None
                },
                "longitude": {
                    "$ne": None
                },
            }
        },

        # Crear celda espacial.
        {
            "$set": {
                "celda_lat": {
                    "$floor": {
                        "$divide": [
                            "$latitude",
                            TAM_CELDA,
                        ]
                    }
                },

                "celda_lon": {
                    "$floor": {
                        "$divide": [
                            "$longitude",
                            TAM_CELDA,
                        ]
                    }
                }
            }
        },

        # Identificador de celda.
        {
            "$set": {
                "celda": {
                    "$concat": [
                        {
                            "$toString":
                            "$celda_lat"
                        },
                        "_",
                        {
                            "$toString":
                            "$celda_lon"
                        }
                    ]
                },

                "dia": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$fecha_hora_colombia",
                    }
                },

                "mes": {
                    "$dateToString": {
                        "format": "%Y-%m",
                        "date": "$fecha_hora_colombia",
                    }
                },

                "anio": {
                    "$year":
                    "$fecha_hora_colombia"
                }
            }
        },

        # Agrupar estadísticas.
        {
            "$group": {
                "_id": "$celda",

                "detecciones": {
                    "$sum": 1
                },

                "dias": {
                    "$addToSet": "$dia"
                },

                "meses": {
                    "$addToSet": "$mes"
                },

                "anios": {
                    "$addToSet": "$anio"
                },

                "primera_deteccion": {
                    "$min":
                    "$fecha_hora_colombia"
                },

                "ultima_deteccion": {
                    "$max":
                    "$fecha_hora_colombia"
                },
            }
        },

        # Convertir los conjuntos en cantidades.
        {
            "$project": {
                "_id": 0,

                "celda": "$_id",

                "detecciones": 1,

                "dias_distintos": {
                    "$size": "$dias"
                },

                "meses_distintos": {
                    "$size": "$meses"
                },

                "anios_distintos": {
                    "$size": "$anios"
                },

                "primera_deteccion": 1,

                "ultima_deteccion": 1,
            }
        },
    ]


def clasificar_celda(estadisticas):
    """
    Determina si una celda cumple el criterio
    de posible fuente estática.
    """

    es_estatica = (
        estadisticas["detecciones"]
        >= MIN_DETECCIONES
        and
        estadisticas["dias_distintos"]
        >= MIN_DIAS
        and
        estadisticas["meses_distintos"]
        >= MIN_MESES
        and
        estadisticas["anios_distintos"]
        >= MIN_ANIOS
    )

    if es_estatica:
        return True, "alta"

    return False, "normal"


def crear_celdas_recurrencia(coleccion):
    """
    Calcula las estadísticas de recurrencia
    de todas las celdas.
    """

    print("\nCalculando recurrencia espacial...")

    pipeline = crear_pipeline_recurrencia()

    estadisticas = {}

    cursor = coleccion.aggregate(
        pipeline,
        allowDiskUse=True,
    )

    for registro in cursor:

        celda = registro["celda"]

        posible, nivel = (
            clasificar_celda(registro)
        )

        registro[
            "posible_fuente_estatica"
        ] = posible

        registro[
            "nivel_recurrencia"
        ] = nivel

        estadisticas[celda] = registro

    print(
        f"Celdas analizadas: "
        f"{len(estadisticas):,}"
    )

    candidatas = sum(
        1
        for x in estadisticas.values()
        if x[
            "posible_fuente_estatica"
        ]
    )

    print(
        f"Celdas candidatas a fuente "
        f"estática: {candidatas:,}"
    )

    return estadisticas


def construir_celda(latitude, longitude):
    """
    Construye el identificador espacial utilizado
    por la agregación.
    """

    celda_lat = int(
        latitude // TAM_CELDA
    )

    celda_lon = int(
        longitude // TAM_CELDA
    )

    return (
        f"{celda_lat}_{celda_lon}"
    )


def clasificar_documentos(
    coleccion_origen,
    coleccion_salida,
    estadisticas,
):
    """
    Copia los documentos originales a la colección
    clasificada agregando las variables de recurrencia.
    """

    print(
        "\nClasificando documentos..."
    )

    cursor = coleccion_origen.find(
        {},
        batch_size=BATCH_SIZE,
    )

    operaciones = []

    total = 0
    marcados = 0

    for documento in cursor:

        latitude = documento.get(
            "latitude"
        )

        longitude = documento.get(
            "longitude"
        )

        if (
            latitude is not None
            and longitude is not None
        ):

            celda = construir_celda(
                latitude,
                longitude,
            )

            info = estadisticas.get(
                celda
            )

        else:
            info = None

        if info is not None:

            posible = info[
                "posible_fuente_estatica"
            ]

            nivel = info[
                "nivel_recurrencia"
            ]

            detecciones = info[
                "detecciones"
            ]

            dias = info[
                "dias_distintos"
            ]

            meses = info[
                "meses_distintos"
            ]

            anios = info[
                "anios_distintos"
            ]

        else:

            posible = False
            nivel = "sin_datos"

            detecciones = 0
            dias = 0
            meses = 0
            anios = 0

        documento[
            "posible_fuente_estatica"
        ] = posible

        documento[
            "nivel_recurrencia"
        ] = nivel

        documento[
            "recurrencia_detecciones"
        ] = detecciones

        documento[
            "recurrencia_dias"
        ] = dias

        documento[
            "recurrencia_meses"
        ] = meses

        documento[
            "recurrencia_anios"
        ] = anios

        if posible:
            marcados += 1

        operaciones.append(
            ReplaceOne(
                {
                    "_id":
                    documento["_id"]
                },
                documento,
                upsert=True,
            )
        )
        total += 1

        if len(operaciones) >= BATCH_SIZE:

            coleccion_salida.bulk_write(
                operaciones,
                ordered=False,
            )

            operaciones = []

            print(
                f"  Procesados: "
                f"{total:,} | "
                f"Marcados: "
                f"{marcados:,}"
            )

    if operaciones:

        coleccion_salida.bulk_write(
            operaciones,
            ordered=False,
        )

    print(
        f"\nTotal procesados: "
        f"{total:,}"
    )

    print(
        f"Posibles fuentes estáticas: "
        f"{marcados:,}"
    )

    return total, marcados


def crear_indices(coleccion):

    print(
        "\nCreando índices..."
    )

    coleccion.create_index(
        "posible_fuente_estatica"
    )

    coleccion.create_index(
        "nivel_recurrencia"
    )

    coleccion.create_index(
        "recurrencia_meses"
    )

    coleccion.create_index(
        "recurrencia_anios"
    )

    coleccion.create_index(
        [
            ("location", "2dsphere")
        ]
    )

    coleccion.create_index(
        "fecha_hora_colombia"
    )

    print(
        "Índices creados."
    )


def procesar():

    print("=" * 70)
    print(
        "CLASIFICACIÓN DE RECURRENCIA ESPACIAL"
    )
    print("=" * 70)

    cliente = MongoClient(
        MONGO_URI
    )

    db = cliente[MONGO_DB]

    coleccion_origen = db[
        COLECCION_ORIGEN
    ]

    coleccion_salida = db[
        COLECCION_SALIDA
    ]

    print(
        f"Origen: {COLECCION_ORIGEN}"
    )

    print(
        f"Salida: {COLECCION_SALIDA}"
    )

    print(
        f"Documentos origen: "
        f"{coleccion_origen.count_documents({}):,}"
    )

    # No modificamos focos_calor.
    coleccion_salida.drop()

    estadisticas = (
        crear_celdas_recurrencia(
            coleccion_origen
        )
    )

    total, marcados = (
        clasificar_documentos(
            coleccion_origen,
            coleccion_salida,
            estadisticas,
        )
    )

    crear_indices(
        coleccion_salida
    )

    total_salida = (
        coleccion_salida.count_documents({})
    )

    print("\n" + "=" * 70)
    print("RESUMEN")
    print("=" * 70)

    print(
        f"Documentos origen:     "
        f"{total:,}"
    )

    print(
        f"Documentos salida:     "
        f"{total_salida:,}"
    )

    print(
        f"Marcados como posibles "
        f"fuentes estáticas:    "
        f"{marcados:,}"
    )

    if total:

        porcentaje = (
            marcados / total
        ) * 100

        print(
            f"Porcentaje marcado:     "
            f"{porcentaje:.4f}%"
        )

    print(
        "\nLa colección original "
        "'focos_calor' no fue modificada."
    )

    cliente.close()


if __name__ == "__main__":
    procesar()
