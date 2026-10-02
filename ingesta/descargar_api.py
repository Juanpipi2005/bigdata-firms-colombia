"""
Descarga detecciones de FIRMS via API (requiere MAP_KEY) para el periodo
que los archivos historicos no cubren: desde 2025-01-01 hasta hoy.

Usa el endpoint de area (caja de coordenadas), en ventanas de 5 dias,
porque el endpoint por pais ya no esta disponible. NO recorta por el
poligono de Colombia todavia -- eso queda para la etapa de limpieza
con Dask, para que esa decision quede documentada con datos reales.
"""
import os
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

MAP_KEY = os.environ["MAP_KEY"]
API = "https://firms.modaps.eosdis.nasa.gov"
BBOX_COL = "-79.1,-4.3,-66.8,12.6"  # caja que cubre Colombia (incluye bordes de paises vecinos)
SALIDA = Path("data/raw/api")

# Un solo sensor por periodo evita pedir datos duplicados de sensores "gemelos"
# (NOAA-20 y Suomi-NPP ven casi lo mismo desde 2019). Se documenta la eleccion.
PERIODOS = [
    # (source, fecha_inicio, fecha_fin)
    ("VIIRS_NOAA20_SP", date(2025, 1, 1), date(2026, 6, 30)),
    ("VIIRS_NOAA20_NRT", date(2026, 7, 1), date.today()),
]


def ventanas_de_5_dias(inicio, fin):
    """Genera fechas de inicio de cada ventana de 5 dias (el maximo que acepta la API)."""
    actual = inicio
    while actual <= fin:
        yield actual
        actual += timedelta(days=5)


def descargar_api():
    SALIDA.mkdir(parents=True, exist_ok=True)
    descargados = []

    for source, inicio, fin in PERIODOS:
        for fecha in ventanas_de_5_dias(inicio, fin):
            fecha_str = fecha.isoformat()
            destino = SALIDA / f"{source}_{fecha_str}.csv"

            if destino.exists():
                continue  # ya descargado en una corrida anterior

            url = f"{API}/api/area/csv/{MAP_KEY}/{source}/{BBOX_COL}/5/{fecha_str}"
            r = requests.get(url, timeout=180)

            primera_linea = r.text.split("\n", 1)[0]
            if not r.ok or "latitude" not in primera_linea:
                print(f"  {source} {fecha_str}: sin datos o error ({r.status_code})")
                time.sleep(1)
                continue

            destino.write_text(r.text)
            filas = r.text.strip().count("\n")
            print(f"  {source} {fecha_str}: {filas:,} filas guardadas")
            descargados.append(str(destino))

            time.sleep(1)  # para no saturar la cuota (5000 cada 10 min)

    return descargados


if __name__ == "__main__":
    print("Descargando datos via API de FIRMS (2025 - hoy)...")
    archivos = descargar_api()
    print(f"\nTotal de archivos nuevos descargados: {len(archivos)}")
