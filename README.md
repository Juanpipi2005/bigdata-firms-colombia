# Focos de calor en Colombia — NASA FIRMS

Pipeline Big Data geoespacial:
**NASA FIRMS → Dask → MongoDB → Spark → API Flask → Docker → Jenkins**.

Proyecto del curso de Big Data de la Institución Universitaria de Envigado.
Equipo: Juan Pablo Bernal y Samuel Muñoz. Entrega: 9 de octubre de 2026.

## Qué hace

Descarga detecciones satelitales de focos de calor (sensores VIIRS) sobre
Colombia, de 2012 a octubre de 2026; las limpia con Dask; las guarda en MongoDB
como GeoJSON con índice `2dsphere`; calcula agregaciones espaciales y temporales
con Spark; y las expone en una API REST con consultas geoespaciales. Todo corre
en contenedores y Jenkins lo redespliega solo cuando cambia el repositorio.

## Arquitectura

```
NASA FIRMS (archivos anuales + API)
      │  ingesta/descargar_historico.py, ingesta/descargar_api.py
      ▼
data/raw/*.csv ── ingesta/cargar_raw_mongo.py ──▶ MongoDB: focos_calor_raw
                                                       │  ingesta/limpieza_dask.py (Dask)
                                                       ▼
                                      MongoDB: focos_calor (GeoJSON + 2dsphere)
                                                       │  spark_jobs/*.py (Spark)
                                                       ▼
                    MongoDB: celdas_recurrencia, serie_mensual, serie_horaria
                                                       │
                                                       ▼
                                          API Flask  (puerto 5050)
```

Despliegue continuo: GitHub → Jenkins (consulta el repo cada minuto) →
construir imágenes → pruebas (pytest) → desplegar con Docker Compose.

## Servicios y puertos

Todos comparten la red Docker `firms_network`.

| Servicio | Contenedor | Puerto en el host | Para qué |
|---|---|---|---|
| mongo | firms_mongo | 27017 | Base de datos |
| api | firms_api | 5050 | API Flask (escucha en el 5000 dentro del contenedor) |
| spark-master | firms_spark_master | 8081 | Interfaz web de Spark |
| spark-worker | firms_spark_worker | 8082 | Worker de Spark (2 cores, 2 GB) |
| dask-scheduler | firms_dask_scheduler | 8787 | Dashboard de Dask |
| dask-worker-1, dask-worker-2 | firms_dask_worker_1/2 | — | Workers de Dask (2 hilos, 2 GB cada uno) |
| jenkins | firms_jenkins | 8080 | CI/CD (compose aparte en `jenkins/`) |

El puerto 5000 del host no se usa porque en macOS lo ocupa AirPlay Receiver.

## Datos

Fuente: NASA FIRMS, sensores VIIRS.

- **2012-2024:** archivos anuales por país (Suomi-NPP 2012-2024 y NOAA-20 2018-2024). No requieren llave y ya vienen recortados a Colombia.
- **2025-01-01 a 2026-06-30:** API de FIRMS, NOAA-20, datos de calidad científica (SP).
- **Desde 2026-07-01:** API de FIRMS, NOAA-20, tiempo casi real (NRT).

Cifras de la carga actual (datos hasta el 2 de octubre de 2026):

| | Documentos |
|---|---:|
| Crudos (`focos_calor_raw`) | 2.549.897 |
| Fuera de Colombia (descartados en la limpieza) | 216.547 (8,49 %) |
| Limpios (`focos_calor`) | 2.333.350 |

Los datos no se versionan (`data/` está en `.gitignore`); los scripts los
regeneran. La única excepción es `data/boundaries/colombia.geojson`, el
polígono de Colombia que usa la limpieza.

## Requisitos

- Docker Desktop con al menos 8 GB de memoria (las pruebas se hicieron con 11,68 GiB).
- VS Code con la extensión Dev Containers.
- Git.
- Una MAP_KEY de NASA FIRMS (gratuita): https://firms.modaps.eosdis.nasa.gov/api/map_key

## Cómo levantarlo desde cero

Los scripts de datos y de Spark se ejecutan **dentro del dev container**: los
nombres `mongo`, `spark-master` y `dask-scheduler` solo resuelven dentro de la
red `firms_network`, a la que el dev container se une al abrirse.

1. **Clonar y configurar.** Crear `.env` a partir de `.env.example` y poner la MAP_KEY:

```bash
   git clone https://github.com/Juanpipi2005/bigdata-firms-colombia.git
   cd bigdata-firms-colombia
   cp .env.example .env      # luego editar .env y completar MAP_KEY
```

2. **Levantar todo con un comando**, desde una terminal normal y *antes* de abrir
   el dev container (esto crea la red `firms_network`). Levanta los 7
   contenedores principales y Jenkins, y espera a que la API responda y MongoDB
   esté conectado. La primera vez tarda varios minutos: en nuestra prueba la
   imagen de Spark tardó unos 10 minutos, casi todo en instalar `pyspark`; las
   siguientes construcciones usan la caché.

```bash
   ./levantar.sh
```

   No ejecutarlo mientras corre un job de Spark o de Dask: recrea esos clusters
   y lo interrumpiría. No carga datos (eso son los pasos 4 y 5). Solo se probó
   sobre un sistema que ya estaba en marcha, no desde cero.

3. **Abrir el proyecto en el dev container** (VS Code → *Dev Containers: Reopen in Container*) y comprobar que hay 7 contenedores en marcha:

```bash
   docker compose ps
```

4. **Cargar y limpiar los datos** (desde la raíz del repo, en este orden):

```bash
   python3 ingesta/descargar_historico.py   # archivos anuales 2012-2024 -> data/raw/historico/
   python3 ingesta/descargar_api.py         # 2025 hasta hoy, con la MAP_KEY -> data/raw/api/
   python3 ingesta/cargar_raw_mongo.py      # CSV -> focos_calor_raw (la recrea cada vez)
   python3 ingesta/limpieza_dask.py         # focos_calor_raw -> focos_calor (la recrea cada vez)
```

5. **Procesar con Spark** (en este orden: el segundo job lee lo que escribe el primero):

```bash
   python3 spark_jobs/estadisticas_celda.py   # -> celdas_recurrencia (~1,5 min en nuestra prueba)
   python3 spark_jobs/series_temporales.py    # -> serie_mensual, serie_horaria (~2 min)
```

6. **Probar.** Con los pasos anteriores hechos, deben pasar los 47 tests:

```bash
   pytest tests/
```

   Los tests de `/spark/*` necesitan que el paso 5 ya se haya ejecutado.

7. **Jenkins** (opcional para usar la API; necesario para el despliegue continuo):
   `./levantar.sh` ya lo levanta. Su compose está en `jenkins/` y se une a la red
   que crea el principal. La configuración inicial se describe más abajo.

## Configurar Jenkins (primera vez)

El contenedor de Jenkins monta `/var/run/docker.sock` para dirigir el Docker del
anfitrión y trae instalados Docker CLI, Compose y Buildx (`jenkins/Dockerfile`).
Sus datos viven en el volumen `jenkins_jenkins_home`.

1. Abrir http://localhost:8080. Contraseña inicial:
   `docker exec firms_jenkins cat /var/jenkins_home/secrets/initialAdminPassword`
2. Instalar los plugins sugeridos y crear el usuario administrador. El plugin
   **Docker Pipeline** no viene entre los sugeridos: instalarlo desde
   *Administrar Jenkins → Plugins → Available plugins*.
3. Crear dos credenciales globales (*Administrar Jenkins → Credentials → System → Global*):
   - *Secret text* con ID `MAP_KEY`: la llave de NASA FIRMS. El `Jenkinsfile` la
     referencia por ese ID, así que debe existir.
   - *Username with password* con ID `github-token`: usuario de GitHub y un
     Personal Access Token con permiso `repo` (el repositorio es privado).
4. Crear un job de tipo **Pipeline** llamado `bigdata-firms-colombia`:
   *Definition* = Pipeline script from SCM, SCM = Git, la URL del repositorio,
   credencial `github-token`, rama `*/main`, Script Path `Jenkinsfile`.
5. En *Triggers* marcar **Consultar repositorio (SCM)** con `* * * * *`
   (cada minuto). Jenkins sugiere una frecuencia menor; para la demostración se
   quiere esta.

## API

Escucha en el puerto 5000 del contenedor y se publica en el **5050** del
anfitrión. Dentro del dev container se llama con `http://api:5000`
(`localhost:5050` no funciona desde ahí).

| Método y ruta | Qué hace | Parámetros |
|---|---|---|
| `GET /health` | Estado de la API y de MongoDB | — |
| `GET /focos/cerca` | Detecciones cercanas a un punto (`$near`) | `lat`, `lon` (obligatorios); `radio_km` (0,1-100, defecto 5); `limite` (1-1000, defecto 100); `confianza` (`l`, `n`, `h`, separadas por coma); `frp_min` (0-10000) |
| `POST /focos/poligono` | Detecciones dentro de un polígono (`$geoWithin`) | Cuerpo JSON: `poligono` (GeoJSON `Polygon`, anillos cerrados, `[lon, lat]`), `limite`, `confianza` |
| `GET /agregados/cerca-mensual` | Detecciones por mes alrededor de un punto (`$geoNear`) | `lat`, `lon`, `radio_km` (0,1-50, defecto 5) |
| `GET /spark/serie-mensual` | Serie mensual calculada por Spark | `desde`, `hasta` (formato `AAAA-MM`) |
| `GET /spark/serie-horaria` | Detecciones por hora local (Spark) | — |
| `GET /spark/celdas-estaticas` | Celdas candidatas a fuente estática (Spark) | `limite` (1-1000) |

Los parámetros inválidos devuelven `400` con `{"error": "..."}`; un fallo de
MongoDB devuelve `503`. Cada respuesta de `/spark/*` incluye una `nota` con las
aclaraciones metodológicas, y `serie-mensual` marca como `parcial` el mes de la
última detección disponible.

Ejemplos (desde el dev container):

```bash
# Detecciones a 5 km de la zona minera del Cerrejón, con FRP de al menos 10
curl -s "http://api:5000/focos/cerca?lat=11.13&lon=-72.58&radio_km=5&limite=3&frp_min=10"

# Dentro de un polígono (cuadrado de ~11 km)
curl -s -X POST http://api:5000/focos/poligono -H "Content-Type: application/json" \
  -d '{"poligono":{"type":"Polygon","coordinates":[[[-72.63,11.08],[-72.53,11.08],[-72.53,11.18],[-72.63,11.18],[-72.63,11.08]]]},"limite":1,"confianza":"n,h"}'

# Resultados de Spark
curl -s "http://api:5000/spark/serie-mensual?desde=2026-08"
curl -s "http://api:5000/spark/celdas-estaticas?limite=3"
```

## Despliegue continuo

El `Jenkinsfile` define cuatro etapas:

1. **Build:** `docker compose build` y `docker build -t firms-tests ./tests`.
2. **Test:** levanta `mongo` y `api`, espera 5 s y ejecuta la imagen de pruebas
   dentro de `firms_network`. Son 47 pruebas con `pytest`.
3. **Deploy:** `docker compose up -d --build`.
4. **Post:** `docker compose ps`.

Si una prueba falla, la etapa Test falla y el pipeline no llega a Deploy.

Jenkins consulta el repositorio cada minuto; no usa webhooks porque Jenkins
corre en una máquina local sin dirección pública. Jenkins **no** ejecuta la
ingesta ni los jobs de Spark: solo construye, prueba y despliega los servicios.

## Cambio en vivo (ensayo)

Se ensayó dos veces el ciclo que se pedirá en la sustentación:

1. Modificar `api/app.py` y agregar sus pruebas en `tests/`.
2. Probar localmente contra una copia de Flask en otro puerto, sin tocar los
   contenedores desplegados:
```bash
   cd api && python3 -m flask --app app run --port 5001 &
   API_URL=http://localhost:5001 pytest tests/
```
3. `git add`, `git commit`, `git push`.
4. Esperar a Jenkins y comprobar con `curl` que la API desplegada responde al cambio.

| Ensayo | Cambio | Del push o commit al inicio del build | Duración del build |
|---|---|---|---|
| 1 | Endpoints `/spark/*` | 1 min 9 s | 39 s |
| 2 | Filtro `frp_min` en `/focos/cerca` | 24 s | 46 s |

En el ensayo 2, sin `frp_min` el punto del Cerrejón devolvía 19.900 detecciones
a 5 km; con `frp_min=10`, 328. El primer tiempo se mide desde el `git push`
(ensayo 1) o desde el `git commit` (ensayo 2), que ocurre unos segundos antes.
Desde ese momento hasta el fin del build pasaron entre ~1 min 10 s y ~1 min 50 s.

## Comparativa Dask vs Spark

Está en `comparativa/`; los resultados completos y sus limitaciones, en
`comparativa/RESULTADOS.md`. Misma operación (agregación por celda sobre
`focos_calor`) en los dos motores, con un control de resultado idéntico. En este
montaje, Dask tardó unos 10-13 s y Spark unos 47-50 s, y Dask usó menos memoria.
**Esto no permite concluir que un motor sea mejor**: la lectura de MongoDB es
distinta en cada uno (conector oficial en Spark, lector propio por año en Dask)
y solo hay 3 corridas por configuración.

## Decisiones sobre los datos

- **Una fila es una detección de un píxel** (~375 m). Píxeles vecinos del mismo
  fuego son detecciones distintas, por eso no se eliminan duplicados.
- **No se elimina la confianza baja** (`l`): se conserva en `confidence` y la API
  permite filtrarla.
- **`type` no es fiable para distinguir incendios.** Los datos son de FIRMS
  versión 2 (`2` en históricos y SP, `2.0NRT` en NRT; la limpieza guarda el
  número y conserva el original en `version_original`). Las detecciones NRT no
  traen `type`. Ejemplo real: junto al centro de la mina del Cerrejón hay
  detecciones con `type: 2` (fuente estática) y otras, a metros, con `type: 0`
  (vegetación).
- **Candidatas a fuente estática:** celdas de 0,01° (~1,1 km en latitud; en
  longitud varía) con al menos 500 detecciones, 300 días, 60 meses y 7 años
  distintos. Es una heurística para señalar candidatas, no una prueba: marca 60
  celdas con 84.272 detecciones (3,61 %). Las celdas están en la colección
  `celdas_recurrencia`.
- **Horas:** `fecha_hora_utc` es la hora real. `fecha_hora_colombia` guarda la
  hora local (UTC−5) como fecha sin zona horaria, que MongoDB muestra como si
  fuera UTC; Spark se fija en UTC para no restar 5 horas otra vez.
- **Series temporales:** un solo sensor por año (Suomi-NPP hasta 2018, NOAA-20
  desde 2019). Desde 2018 hay dos satélites viendo casi lo mismo y sumarlos
  duplicaría esos años. Los datos llegan hasta el 2 de octubre de 2026, así que
  2026 y octubre son parciales.
- **Serie horaria:** VIIRS es de órbita polar y pasa por Colombia a horas casi
  fijas, así que esa distribución refleja los horarios de paso del satélite, no
  el ciclo diario de los incendios.
- **Polígono de Colombia:** versión simplificada de geoBoundaries (733 vértices).
  No contiene San Andrés ni Providencia, así que sus detecciones (9 en los datos
  crudos) se descartan como «fuera de Colombia».
- **Scripts de recurrencia previos** (`ingesta/analizar_recurrencia*.py` y
  `ingesta/clasificar_recurrencia.py`): son la primera versión en Python y
  MongoDB. La definición vigente de celda y de candidatas es la del job de Spark
  `spark_jobs/estadisticas_celda.py`. Los scripts previos no se ejecutaron en el
  entorno actual.

## Limitaciones conocidas

- **Jenkins recrea contenedores de Spark o Dask sin que cambien.** En la mayoría
  de los despliegues recreó los de Dask y alguna vez los de Spark. No se
  determinó la causa. Un job en ejecución en ese momento se perdería: no correr
  jobs de Dask ni de Spark mientras se hace un push.
- **Las pruebas dependen de los datos.** Las de `/focos/*` necesitan
  `focos_calor` con su índice; las de `/spark/*`, que los dos jobs de Spark ya se
  hayan ejecutado. En una máquina limpia hay que seguir primero los pasos de
  «Cómo levantarlo desde cero».
- **La ingesta no es automática.** Descargar, cargar y limpiar se ejecuta a mano.
- **`levantar.sh` no carga datos** y solo se probó sobre un sistema ya en marcha,
  no desde cero ni desde la terminal del Mac. Recrea los clusters de Spark y de
  Dask: no ejecutarlo con un job en curso.
- **Limpieza con Dask:** `ingesta/limpieza_dask.py` lee de MongoDB por lotes de
  100.000 documentos y usa Dask sobre cada lote; los workers del cluster de Dask
  intervienen en la comparativa, no en este script.
- **Aviso de versiones:** el dev container tiene Python 3.12.14 y el cluster
  3.12.15; Dask lo avisa con un warning y no se observó ningún efecto.

## Estructura del repositorio

```
api/            API Flask y su Dockerfile
ingesta/        descarga, carga cruda, limpieza (Dask) y scripts de recurrencia
spark_jobs/     jobs de Spark y sesión compartida (comun.py)
tests/          pruebas de la API (pytest) y su Dockerfile
comparativa/    comparativa Dask vs Spark y sus resultados
spark/, dask/   imágenes de los clusters
jenkins/        imagen y compose de Jenkins
.devcontainer/  entorno de desarrollo
data/           datos locales (ignorados por Git; solo se versiona boundaries/)
docker-compose.yml, Jenkinsfile, requirements-dev.txt
```

Cada script se ejecuta desde la raíz del repositorio.
