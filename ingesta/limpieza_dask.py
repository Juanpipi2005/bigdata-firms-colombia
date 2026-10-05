"""
Pipeline de limpieza y normalización de focos de calor usando Dask.

Fuente:
    MongoDB -> focos_calor_raw

Salida:
    MongoDB -> focos_calor

Procesos:
    - Lectura por lotes desde MongoDB
    - Normalización de tipos
    - Normalización de fecha/hora
    - Conversión UTC -> Colombia (UTC-5)
    - Validación de coordenadas
    - Validación de fecha/hora
    - Normalización de confidence, version y type
    - Filtro espacial con polígono oficial de Colombia
    - Creación de geometría GeoJSON Point
    - Conservación de trazabilidad
    - Estadísticas de calidad
"""

import os
from datetime import timedelta
from pathlib import Path

import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from shapely.geometry import Point


load_dotenv()


MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://mongo:27017",
)

MONGO_DB = os.environ.get(
    "MONGO_DB",
    "firms_colombia",
)


COLECCION_RAW = "focos_calor_raw"
COLECCION_LIMPIA = "focos_calor"

BATCH_SIZE = 100_000

POLIGONO_COLOMBIA = Path(
    "data/boundaries/colombia.geojson"
)


def cargar_poligono_colombia():
    """
    Carga el límite ADM0 de Colombia.

    El archivo debe estar en EPSG:4326,
    igual que las coordenadas de FIRMS.
    """

    if not POLIGONO_COLOMBIA.exists():
        raise FileNotFoundError(
            f"No existe el polígono de Colombia: "
            f"{POLIGONO_COLOMBIA}"
        )

    gdf = gpd.read_file(POLIGONO_COLOMBIA)

    if gdf.empty:
        raise ValueError(
            "El archivo del polígono de Colombia está vacío."
        )

    if gdf.crs is None:
        raise ValueError(
            "El polígono de Colombia no tiene CRS definido."
        )

    gdf = gdf.to_crs("EPSG:4326")

    return gdf.geometry.iloc[0]


def leer_mongo_en_lotes():
    """
    Lee MongoDB por lotes para evitar cargar todos
    los registros simultáneamente en memoria.
    """

    client = MongoClient(MONGO_URI)
    coleccion = client[MONGO_DB][COLECCION_RAW]

    total = coleccion.count_documents({})

    print(
        f"Total de documentos en MongoDB: "
        f"{total:,}"
    )

    print(
        f"Tamaño de lote: {BATCH_SIZE:,}"
    )

    cursor = coleccion.find(
        {},
        {
            "_id": 0,
            "latitude": 1,
            "longitude": 1,
            "bright_ti4": 1,
            "bright_ti5": 1,
            "scan": 1,
            "track": 1,
            "acq_date": 1,
            "acq_time": 1,
            "satellite": 1,
            "instrument": 1,
            "confidence": 1,
            "version": 1,
            "frp": 1,
            "daynight": 1,
            "type": 1,
            "archivo_fuente": 1,
            "origen": 1,
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

            yield pd.DataFrame(documentos)

    finally:
        client.close()


def normalizar_dataframe(df):
    """
    Normaliza tipos y crea columnas derivadas.
    """

    columnas_numericas = [
        "latitude",
        "longitude",
        "bright_ti4",
        "bright_ti5",
        "scan",
        "track",
        "frp",
    ]

    for columna in columnas_numericas:
        if columna in df.columns:
            df[columna] = pd.to_numeric(
                df[columna],
                errors="coerce",
            )

    # Fecha de adquisición.
    df["acq_date"] = pd.to_datetime(
        df["acq_date"],
        errors="coerce",
    )

    # Hora FIRMS.
    #
    # Ejemplos:
    # 507 -> 05:07
    # 45  -> 00:45
    hora = pd.to_numeric(
        df["acq_time"],
        errors="coerce",
    )

    df["acq_time"] = hora

    horas = (hora // 100).astype("Int64")
    minutos = (hora % 100).astype("Int64")

    df["fecha_hora_utc"] = (
        df["acq_date"]
        + pd.to_timedelta(
            horas,
            unit="h",
        )
        + pd.to_timedelta(
            minutos,
            unit="m",
        )
    )

    # Colombia = UTC-5.
    df["fecha_hora_colombia"] = (
        df["fecha_hora_utc"]
        - timedelta(hours=5)
    )

    # Confidence.
    df["confidence"] = (
        df["confidence"]
        .astype("string")
        .str.lower()
        .str.strip()
    )

    # Version.
    #
    # Ejemplos:
    # 2 -> 2
    # "2.0NRT" -> 2
    df["version_original"] = (
        df["version"].astype("string")
    )

    df["version"] = (
        df["version"]
        .astype("string")
        .str.extract(
            r"(\d+(?:\.\d+)?)"
        )[0]
    )

    df["version"] = pd.to_numeric(
        df["version"],
        errors="coerce",
    )

    # Type.
    #
    # NRT puede no traer este campo.
    df["type"] = pd.to_numeric(
        df["type"],
        errors="coerce",
    ).astype("Int64")

    # Coordenadas válidas.
    df["coordenadas_validas"] = (
        df["latitude"].between(-90, 90)
        & df["longitude"].between(-180, 180)
    )

    # Fecha/hora válida.
    df["fecha_hora_valida"] = (
        df["fecha_hora_utc"].notna()
        & df["fecha_hora_colombia"].notna()
    )

    return df


def aplicar_filtro_geografico(
    df,
    geometria_colombia,
):
    """
    Determina si cada detección está dentro
    del territorio colombiano.

    Se utiliza within() sobre el polígono ADM0.
    """

    df["dentro_colombia"] = False

    mascara_base = (
        df["coordenadas_validas"]
        & df["fecha_hora_valida"]
    )

    if not mascara_base.any():
        return df

    puntos = gpd.GeoSeries(
        [
            Point(longitud, latitud)
            for longitud, latitud in zip(
                df.loc[
                    mascara_base,
                    "longitude",
                ],
                df.loc[
                    mascara_base,
                    "latitude",
                ],
            )
        ],
        index=df.index[mascara_base],
        crs="EPSG:4326",
    )

    df.loc[
        mascara_base,
        "dentro_colombia",
    ] = puntos.within(
        geometria_colombia
    ).values

    return df


def agregar_location(df):
    """
    Crea GeoJSON Point para MongoDB.

    MongoDB espera:
        [longitude, latitude]
    """

    df["location"] = None

    mascara = (
        df["coordenadas_validas"]
        & df["dentro_colombia"]
    )

    if mascara.any():
        df.loc[
            mascara,
            "location",
        ] = [
            {
                "type": "Point",
                "coordinates": [
                    float(longitud),
                    float(latitud),
                ],
            }
            for longitud, latitud in zip(
                df.loc[
                    mascara,
                    "longitude",
                ],
                df.loc[
                    mascara,
                    "latitude",
                ],
            )
        ]

    return df


def crear_meta(df):
    """
    Genera el meta de Dask a partir de un DataFrame
    ya normalizado en una prueba controlada.
    """

    return df.iloc[0:0].copy()


def procesar():
    print("=" * 70)
    print("INICIO DEL PIPELINE DASK")
    print("=" * 70)

    geometria_colombia = (
        cargar_poligono_colombia()
    )

    print(
        "Polígono de Colombia cargado "
        "correctamente."
    )

    cliente = MongoClient(MONGO_URI)
    db = cliente[MONGO_DB]

    coleccion_salida = db[
        COLECCION_LIMPIA
    ]

    print(
        "\nEliminando colección limpia anterior..."
    )

    coleccion_salida.drop()

    total_leidos = 0
    total_invalidos = 0
    total_fuera_colombia = 0
    total_validos = 0

    for numero_lote, df in enumerate(
        leer_mongo_en_lotes(),
        start=1,
    ):
        cantidad_lote = len(df)

        total_leidos += cantidad_lote

        print(
            f"\nLote {numero_lote}: "
            f"{cantidad_lote:,} registros"
        )

        # Dask.
        ddf = dd.from_pandas(
            df,
            npartitions=4,
        )

        # Primera normalización.
        ddf = ddf.map_partitions(
            normalizar_dataframe,
        )

        df_limpio = ddf.compute()

        # Validación inicial.
        mascara_validacion = (
            df_limpio[
                "coordenadas_validas"
            ]
            & df_limpio[
                "fecha_hora_valida"
            ]
        )

        invalidos_lote = int(
            (~mascara_validacion).sum()
        )

        total_invalidos += (
            invalidos_lote
        )

        df_validacion = df_limpio.loc[
            mascara_validacion
        ].copy()

        # Filtro geográfico.
        df_validacion = (
            aplicar_filtro_geografico(
                df_validacion,
                geometria_colombia,
            )
        )

        fuera_lote = int(
            (~df_validacion[
                "dentro_colombia"
            ]).sum()
        )

        total_fuera_colombia += (
            fuera_lote
        )

        df_validos = df_validacion.loc[
            df_validacion[
                "dentro_colombia"
            ]
        ].copy()

        total_validos += len(
            df_validos
        )

        # Crear GeoJSON.
        df_validos = agregar_location(
            df_validos
        )

        # MongoDB no acepta NaN.
        df_validos = df_validos.where(
            pd.notnull(df_validos),
            None,
        )

        registros = (
            df_validos.to_dict(
                orient="records"
            )
        )

        if registros:
            coleccion_salida.insert_many(
                registros,
                ordered=False,
            )

        print(
            f"  Inválidos:          "
            f"{invalidos_lote:,}"
        )

        print(
            f"  Fuera de Colombia:  "
            f"{fuera_lote:,}"
        )

        print(
            f"  Conservados:        "
            f"{len(df_validos):,}"
        )

    print("\n" + "=" * 70)
    print("RESUMEN DE LIMPIEZA")
    print("=" * 70)

    print(
        f"Total leídos:          "
        f"{total_leidos:,}"
    )

    print(
        f"Total inválidos:       "
        f"{total_invalidos:,}"
    )

    print(
        f"Total fuera Colombia:  "
        f"{total_fuera_colombia:,}"
    )

    print(
        f"Total conservados:     "
        f"{total_validos:,}"
    )

    if total_leidos:
        print(
            f"\n% inválidos: "
            f"{(total_invalidos / total_leidos) * 100:.4f}%"
        )

        print(
            f"% fuera Colombia: "
            f"{(total_fuera_colombia / total_leidos) * 100:.4f}%"
        )

        print(
            f"% conservados: "
            f"{(total_validos / total_leidos) * 100:.4f}%"
        )

    total_mongo = (
        coleccion_salida.count_documents({})
    )

    print(
        f"\nDocumentos en "
        f"'{COLECCION_LIMPIA}': "
        f"{total_mongo:,}"
    )

    print("\nCreando índices...")

    coleccion_salida.create_index(
        [
            ("location", "2dsphere")
        ]
    )

    coleccion_salida.create_index(
        "fecha_hora_colombia"
    )

    coleccion_salida.create_index(
        "acq_date"
    )

    coleccion_salida.create_index(
        "confidence"
    )

    coleccion_salida.create_index(
        "satellite"
    )

    print("Índices creados.")

    cliente.close()

    print(
        "\nPipeline terminado correctamente."
    )


if __name__ == "__main__":
    procesar()
