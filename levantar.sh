#!/usr/bin/env bash
# Levanta todo el sistema con un solo comando:   ./levantar.sh
#
# 1) Servicios principales: MongoDB, API, Spark (master + worker) y Dask
#    (scheduler + 2 workers). Esto tambien crea la red firms_network.
# 2) Jenkins, que vive en su propio compose y se une a esa red.
# 3) Espera a que la API responda y a que MongoDB este conectado.
#
# No carga datos: para eso ver el README (paso 4 de "Como levantarlo desde cero").
set -euo pipefail
cd "$(dirname "$0")"

echo "[1/3] Servicios principales (MongoDB, API, Spark, Dask)..."
docker compose up -d --build

echo "[2/3] Jenkins..."
docker compose -f jenkins/docker-compose.jenkins.yml up -d --build

echo "[3/3] Esperando a la API..."
for intento in $(seq 1 30); do
  if docker exec firms_api python -c "import urllib.request,sys; sys.exit(0 if b'conectado' in urllib.request.urlopen('http://localhost:5000/health', timeout=3).read() else 1)" >/dev/null 2>&1; then
    echo "API lista y MongoDB conectado."
    docker compose ps
    docker compose -f jenkins/docker-compose.jenkins.yml ps
    exit 0
  fi
  sleep 2
done

echo "La API no respondio en 60 s. Revisar: docker logs firms_api" >&2
exit 1
