"""
Descarga los archivos anuales de FIRMS por pais (VIIRS Suomi-NPP y NOAA-20),
2012-2024. Estos archivos ya vienen recortados a Colombia por NASA,
no requieren MAP_KEY.
"""
import requests
from pathlib import Path

BASE = "https://firms.modaps.eosdis.nasa.gov/data/country"
SALIDA = Path("data/raw/historico")

# sensor -> (nombre_archivo, anio_inicio, anio_fin)
SENSORES = {
    "viirs-snpp": (2012, 2024),
    "viirs-jpss1": (2018, 2024),  # jpss1 = NOAA-20
}


def descargar_historico():
    SALIDA.mkdir(parents=True, exist_ok=True)
    descargados = []

    for sensor, (anio_inicio, anio_fin) in SENSORES.items():
        for anio in range(anio_inicio, anio_fin + 1):
            destino = SALIDA / f"{sensor}_{anio}_Colombia.csv"

            if destino.exists():
                print(f"  {sensor} {anio}: ya existe, se omite")
                continue

            url = f"{BASE}/{sensor}/{anio}/{sensor}_{anio}_Colombia.csv"
            r = requests.get(url, timeout=180)

            primera_linea = r.text.split("\n", 1)[0]
            if r.status_code != 200 or "latitude" not in primera_linea:
                print(f"  {sensor} {anio}: no disponible ({r.status_code})")
                continue

            destino.write_text(r.text)
            filas = r.text.strip().count("\n")
            print(f"  {sensor} {anio}: {filas:,} filas guardadas")
            descargados.append(str(destino))

    return descargados


if __name__ == "__main__":
    print("Descargando archivos historicos de FIRMS (2012-2024)...")
    archivos = descargar_historico()
    print(f"\nTotal de archivos nuevos descargados: {len(archivos)}")
