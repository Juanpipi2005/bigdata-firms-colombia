"""
Mide la memoria de los contenedores mientras corre un comando.

Uso (desde la raiz del repo):
    python3 comparativa/medir_memoria.py python3 comparativa/dask_celdas.py
    python3 comparativa/medir_memoria.py python3 comparativa/spark_celdas.py 2

Limitaciones:
  - Muestrea 'docker stats' en bucle; cada muestra tarda ~1-2 s, asi que en
    jobs cortos hay pocas muestras y puede perderse un pico.
  - No incluye el driver (el proceso que lanza el job), que corre en el
    dev container.
  - No se verifico como cuenta Docker la cache de Mongo; su cifra es orientativa.
"""
import re
import subprocess
import sys
import threading
import time

CONTENEDORES = ["firms_spark_worker", "firms_dask_worker_1",
                "firms_dask_worker_2", "firms_mongo"]
UNIDADES = {"B": 1 / 1048576, "KiB": 1 / 1024, "MiB": 1.0, "GiB": 1024.0}


def a_mib(texto):
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", texto)
    return float(m.group(1)) * UNIDADES.get(m.group(2), 0.0)


def contenedores_activos():
    """Solo los contenedores de la lista que estan corriendo (omite los detenidos)."""
    activos = subprocess.run(["docker", "ps", "--format", "{{.Names}}"],
                             capture_output=True, text=True).stdout.split()
    return [c for c in CONTENEDORES if c in activos]


def leer_memoria():
    salida = subprocess.check_output(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.MemUsage}}"]
        + contenedores_activos(), text=True)
    memoria = {}
    for linea in salida.strip().splitlines():
        nombre, uso = linea.split("|")
        memoria[nombre] = a_mib(uso.split("/")[0])
    return memoria


def muestrear(parar, muestras):
    while not parar.is_set():
        try:
            for nombre, mib in leer_memoria().items():
                muestras.setdefault(nombre, []).append(mib)
        except Exception:
            pass


def main():
    if len(sys.argv) < 2:
        sys.exit("Uso: medir_memoria.py <comando> [args...]")

    reposo = leer_memoria()
    muestras = {}
    parar = threading.Event()
    hilo = threading.Thread(target=muestrear, args=(parar, muestras), daemon=True)
    hilo.start()

    proc = subprocess.run(sys.argv[1:], capture_output=True, text=True)
    parar.set()
    hilo.join(timeout=15)

    for linea in (proc.stdout + proc.stderr).splitlines():
        if linea.startswith(("RESULTADO", "DIFERENCIAS")):
            print(linea)
    if proc.returncode != 0:
        print("El comando termino con error:", proc.returncode)
        print("\n".join((proc.stderr or proc.stdout).splitlines()[-15:]))

    print(f"\n{'contenedor':<22}{'reposo MiB':>11}{'pico MiB':>10}{'subida':>9}{'muestras':>10}")
    for nombre in CONTENEDORES:
        if nombre not in reposo:
            continue
        valores = muestras.get(nombre, [])
        pico = max(valores) if valores else float("nan")
        print(f"{nombre:<22}{reposo[nombre]:>11.0f}{pico:>10.0f}"
              f"{pico - reposo[nombre]:>9.0f}{len(valores):>10}")


if __name__ == "__main__":
    main()
