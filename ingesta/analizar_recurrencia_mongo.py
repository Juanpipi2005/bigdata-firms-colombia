"""
Análisis de recurrencia espacial usando agregación de MongoDB.

No modifica ni elimina registros.

Calcula por celda espacial:
    - detecciones
    - días distintos
    - meses distintos
    - años distintos
    - primera detección
    - última detección
"""

import os

from dotenv import load_dotenv
from pymongo import MongoClient


load_dotenv()


MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://mongo:27017",
)

MONGO_DB = os.environ.get(
    "MONGO_DB",
    "firms_colombia",
)

COLECCION = "focos_calor"

TAM_CELDA = 0.01


def procesar():

    cliente = MongoClient(MONGO_URI)

    coleccion = cliente[
        MONGO_DB
    ][COLECCION]

    total = coleccion.count_documents({})

    print("=" * 70)
    print("ANÁLISIS REAL DE RECURRENCIA ESPACIAL")
    print("=" * 70)

    print(
        f"Documentos analizados: {total:,}"
    )

    pipeline = [

        # Solo necesitamos documentos válidos.
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

        # Crear identificador de celda.
        {
            "$set": {
                "celda": {
                    "$concat": [
                        {
                            "$toString": "$celda_lat"
                        },
                        "_",
                        {
                            "$toString": "$celda_lon"
                        }
                    ]
                },

                "dia": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$fecha_hora_colombia"
                    }
                },

                "mes": {
                    "$dateToString": {
                        "format": "%Y-%m",
                        "date": "$fecha_hora_colombia"
                    }
                },

                "anio": {
                    "$year": "$fecha_hora_colombia"
                }
            }
        },

        # Agrupar por celda.
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
                    "$min": "$fecha_hora_colombia"
                },

                "ultima_deteccion": {
                    "$max": "$fecha_hora_colombia"
                }
            }
        },

        # Convertir arrays en cantidades.
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

                "ultima_deteccion": 1
            }
        },

        # Las celdas más recurrentes primero.
        {
            "$sort": {
                "detecciones": -1
            }
        },

        # Solo necesitamos las principales
        # para el análisis inicial.
        {
            "$limit": 100
        }
    ]

    resultados = list(
        coleccion.aggregate(
            pipeline,
            allowDiskUse=True,
        )
    )

    print(
        f"\nCeldas analizadas: "
        f"{len(resultados)}"
    )

    print("\nTOP 100 CELDAS")
    print("-" * 70)

    for posicion, r in enumerate(
        resultados,
        start=1,
    ):

        print(
            f"{posicion:3}. "
            f"{r['celda']:>12} | "
            f"det={r['detecciones']:>5} | "
            f"días={r['dias_distintos']:>4} | "
            f"meses={r['meses_distintos']:>3} | "
            f"años={r['anios_distintos']:>2} | "
            f"{r['primera_deteccion']} -> "
            f"{r['ultima_deteccion']}"
        )

    cliente.close()


if __name__ == "__main__":
    procesar()
