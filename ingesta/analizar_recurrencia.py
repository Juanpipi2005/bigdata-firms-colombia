"""
Análisis de recurrencia espacial de focos de calor.

Objetivo:
    Identificar celdas espaciales con comportamiento
    altamente recurrente que puedan corresponder a
    fuentes estáticas.

Importante:
    Este proceso NO elimina registros.
    Solamente genera estadísticas para definir
    posteriormente la marca:

        posible_fuente_estatica = True / False
"""

import os

import pandas as pd
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

BATCH_SIZE = 100_000

# Aproximadamente 1 km en latitud.
# Se usa inicialmente para estudiar la recurrencia.
TAM_CELDA = 0.01


def leer_lotes():

    cliente = MongoClient(MONGO_URI)

    coleccion = cliente[
        MONGO_DB
    ][COLECCION]

    total = coleccion.count_documents({})

    print(
        f"Total documentos: {total:,}"
    )

    cursor = coleccion.find(
        {},
        {
            "_id": 0,
            "latitude": 1,
            "longitude": 1,
            "fecha_hora_colombia": 1,
            "fecha_hora_utc": 1,
            "satellite": 1,
            "confidence": 1,
        },
        batch_size=BATCH_SIZE,
    )

    try:

        while True:

            documentos = []

            for _ in range(BATCH_SIZE):

                try:
                    documentos.append(
                        next(cursor)
                    )

                except StopIteration:
                    break

            if not documentos:
                break

            yield pd.DataFrame(
                documentos
            )

    finally:

        cliente.close()


def agregar_celdas(df):

    df["celda_lat"] = (
        df["latitude"] / TAM_CELDA
    ).round().astype("int64")

    df["celda_lon"] = (
        df["longitude"] / TAM_CELDA
    ).round().astype("int64")

    df["celda"] = (
        df["celda_lat"].astype(str)
        + "_"
        + df["celda_lon"].astype(str)
    )

    df["fecha"] = pd.to_datetime(
        df["fecha_hora_colombia"],
        errors="coerce",
    )

    df["dia"] = (
        df["fecha"]
        .dt.date
    )

    df["mes"] = (
        df["fecha"]
        .dt.to_period("M")
        .astype(str)
    )

    df["anio"] = (
        df["fecha"].dt.year
    )

    return df


def procesar():

    print("=" * 70)
    print("ANÁLISIS DE RECURRENCIA ESPACIAL")
    print("=" * 70)

    acumulados = {}

    for numero_lote, df in enumerate(
        leer_lotes(),
        start=1,
    ):

        print(
            f"\nProcesando lote "
            f"{numero_lote}: "
            f"{len(df):,}"
        )

        df = agregar_celdas(df)

        agrupado = (
            df.groupby("celda")
            .agg(
                detecciones=(
                    "celda",
                    "size",
                ),
                dias=(
                    "dia",
                    "nunique",
                ),
                meses=(
                    "mes",
                    "nunique",
                ),
                anios=(
                    "anio",
                    "nunique",
                ),
                primera_deteccion=(
                    "fecha",
                    "min",
                ),
                ultima_deteccion=(
                    "fecha",
                    "max",
                ),
                satelites=(
                    "satellite",
                    "nunique",
                ),
            )
            .reset_index()
        )

        for registro in agrupado.to_dict(
            orient="records"
        ):

            celda = registro["celda"]

            if celda not in acumulados:

                acumulados[celda] = {
                    "detecciones": 0,
                    "dias": set(),
                    "meses": set(),
                    "anios": set(),
                    "primera_deteccion": (
                        registro[
                            "primera_deteccion"
                        ]
                    ),
                    "ultima_deteccion": (
                        registro[
                            "ultima_deteccion"
                        ]
                    ),
                    "satelites": set(),
                }

            acumulado = acumulados[
                celda
            ]

            acumulado[
                "detecciones"
            ] += registro[
                "detecciones"
            ]

            # Para mantener días/meses/años
            # distintos necesitamos volver a
            # consultar el lote original.
            #
            # Por ahora los contadores se
            # acumulan mediante estadísticas
            # conservadoras.

            acumulado[
                "primera_deteccion"
            ] = min(
                acumulado[
                    "primera_deteccion"
                ],
                registro[
                    "primera_deteccion"
                ],
            )

            acumulado[
                "ultima_deteccion"
            ] = max(
                acumulado[
                    "ultima_deteccion"
                ],
                registro[
                    "ultima_deteccion"
                ],
            )

    print(
        "\nCeldas espaciales encontradas:",
        f"{len(acumulados):,}",
    )

    resultados = []

    for celda, datos in acumulados.items():

        resultados.append(
            {
                "celda": celda,
                "detecciones": datos[
                    "detecciones"
                ],
                "primera_deteccion": datos[
                    "primera_deteccion"
                ],
                "ultima_deteccion": datos[
                    "ultima_deteccion"
                ],
            }
        )

    resultado = pd.DataFrame(
        resultados
    )

    print("\n" + "=" * 70)
    print("RESULTADOS PRELIMINARES")
    print("=" * 70)

    print(
        resultado[
            "detecciones"
        ].describe()
    )

    print(
        "\nTop 20 celdas por número "
        "de detecciones:"
    )

    print(
        resultado
        .sort_values(
            "detecciones",
            ascending=False,
        )
        .head(20)
        .to_string(
            index=False
        )
    )


if __name__ == "__main__":
    procesar()
