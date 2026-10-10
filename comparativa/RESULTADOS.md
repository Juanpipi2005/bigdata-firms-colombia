# Comparativa Dask vs Spark

Medido el 7 de octubre de 2026 sobre `focos_calor` (2.333.350 documentos).

## Qué se comparó

La misma operación en los dos motores: dividir Colombia en celdas de 0,01° y,
para cada celda, contar detecciones y días, meses y años distintos, y marcar las
candidatas a fuente estática con los umbrales del proyecto (500 detecciones,
300 días, 60 meses, 7 años).

**Control de validez.** Cada corrida compara su resultado contra el de referencia
(`parametros.py`) y lo reporta como `control_ok`. Las 17 corridas ejecutadas dieron
el mismo resultado: 327.057 celdas, 2.333.350 detecciones, 60 candidatas y
84.272 detecciones en ellas. Si una corrida hubiera dado otra cifra, no se
habría contado.

## Montaje

- Todo corre en una sola máquina: Docker Desktop con 11,68 GiB y 12 CPUs.
- Spark 3.5.1: 1 master y 1 worker (2 cores, 2 GB), executor de 1 GB. Se probó
  con `spark.cores.max` en 1 y en 2. El driver corre en el dev container.
- Dask 2024.8.0: 1 scheduler y 2 workers (2 hilos y 2 GB cada uno). La prueba de
  "1 worker" se hizo apagando uno con `docker stop`, sin tocar el compose.
- Lectura de datos, **distinta en cada motor**:
  - Spark lee de Mongo con el conector oficial.
  - Dask usa un lector propio: 15 tareas, una por año (2012-2026), cada una
    con un filtro por `acq_date` y solo 3 campos.
  - No se midió cuánto pesa esa diferencia en el tiempo total.

## Tiempos (segundos)

Cada configuración se corrió 3 veces seguidas. La primera corrida fue la más
lenta en las cuatro configuraciones (sospechamos el calentamiento de MongoDB,
sin comprobarlo). La tabla muestra las 3 corridas y dos promedios, porque
descartar la primera es una decisión arbitraria y cambia la conclusión sobre
Spark con 1 y 2 cores.

| Configuración   | Corrida 1 | Corrida 2 | Corrida 3 | Prom. de las 3 | Prom. de la 2 y 3 |
|-----------------|----------:|----------:|----------:|---------------:|------------------:|
| Spark, 1 core   |     97,85 |     46,43 |     47,99 |          64,09 |             47,21 |
| Spark, 2 cores  |     63,77 |     44,41 |     56,15 |          54,78 |             50,28 |
| Dask, 1 worker  |     14,96 |     11,99 |      9,80 |          12,25 |             10,90 |
| Dask, 2 workers |     37,06 |     15,71 |     10,96 |          21,24 |             13,34 |

El tiempo cubre desde que se empieza a leer de Mongo hasta tener el resumen. No
incluye el arranque de la sesión ni la conexión al scheduler. En Spark incluye
la inferencia del esquema de la colección.

## Memoria (MiB)

Se muestreó `docker stats` mientras corría cada job, partiendo de contenedores
recién reiniciados. Cada configuración tiene una medición válida (la primera
medición de Dask con 2 workers se descartó, ver limitaciones).

| Motor           | Contenedor           | Reposo | Pico  | Subida | Muestras |
|-----------------|----------------------|-------:|------:|-------:|---------:|
| Spark, 1 core   | firms_spark_worker   |    213 | 1.332 |  1.119 |      105 |
| Spark, 2 cores  | firms_spark_worker   |    198 | 1.529 |  1.331 |       47 |
| Dask, 1 worker  | firms_dask_worker_1  |    147 |   570 |    424 |       25 |
| Dask, 2 workers | firms_dask_worker_1  |    151 |   408 |    257 |       14 |
| Dask, 2 workers | firms_dask_worker_2  |    151 |   436 |    285 |       14 |

En cada medición, los contenedores del otro motor y Mongo no se movieron
(subida de 0 a 5 MiB).

## Qué se puede decir

- En este montaje, Dask terminó la operación en unos 10-13 s y Spark en unos
  47-50 s (promediando sin la primera corrida), es decir, entre 3,5 y 4,6 veces
  más rápido.
- Con la memoria medida en las dos configuraciones de cada motor, Dask usó menos
  que Spark en ambas: con 2 workers sumó unos 540 MiB de subida (257 + 285)
  contra 1.331 MiB de Spark con 2 cores; con 1 worker, 424 MiB contra 1.119 MiB
  de Spark con 1 core. Es decir, Spark usó entre 2,5 y 2,6 veces más.

## Qué NO se puede concluir

- **Que un motor sea mejor que el otro.** El método de lectura es distinto, y
  la diferencia puede venir de cómo se lee y no de qué motor calcula mejor.
- **Que más recursos ayuden o no.** Con 2 cores Spark no fue más rápido que con
  1, y Dask con 2 workers no fue más rápido que con 1. Con 3 corridas por
  configuración y casi 12 s de diferencia entre las corridas 2 y 3 de Spark con 2 cores, esas diferencias
  no son concluyentes. No se determinó la causa; todo corre en la misma
  máquina y comparte la lectura de Mongo, pero eso no se comprobó.
- **Cifras de memoria exactas.** Ver limitaciones.

## Limitaciones

- Solo 3 corridas por configuración. Según cómo se promedie (con o sin la
  primera), la conclusión sobre 1 vs 2 cores de Spark se invierte.
- El muestreo de memoria alarga los jobs (19,92 s y 27,98 s en Dask contra
  11-16 s; 90,5 s en Spark contra 44-56 s). Por eso los tiempos de esas
  corridas no se usan en la tabla de tiempos.
- Cada muestra de `docker stats` tarda 1-2 s. En un job de unos 10 s (Dask)
  hay pocas muestras y puede perderse un pico; la memoria de Dask puede estar
  subestimada.
- No se mide el driver (el proceso que lanza el job), que corre en el dev
  container, en ninguno de los dos motores.
- El pico de Spark incluye la máquina virtual de Java, y no se desglosó.
- La memoria se midió una vez por configuración (sin repeticiones), y el reposo
  de partida varió entre mediciones (por ejemplo, 198 y 213 MiB en el worker de
  Spark).
- Al pasar de 2 a 1 unidad, la memoria de Spark bajó (de 1.331 a 1.119 MiB de
  subida) y la de Dask subió (de 257 y 285 MiB por worker a 424 MiB en el único
  worker); con una medición por configuración no se interpreta.
- Una primera medición de memoria de Dask se descartó: el worker 1 traía
  memoria de corridas anteriores y no era comparable. Se repitió con los
  contenedores recién reiniciados.
- El cliente tiene Python 3.12.14 y el cluster 3.12.15; Dask lo avisa con un
  warning. Es una diferencia de parche y no se observó ningún efecto.

## Cómo reproducirlo

Con el stack levantado, desde la raíz del repo y sin hacer push mientras se
mide (un despliegue de Jenkins puede reiniciar los clusters):

```bash
python3 comparativa/spark_celdas.py 1      # Spark, 1 core
python3 comparativa/spark_celdas.py 2      # Spark, 2 cores
python3 comparativa/dask_celdas.py         # Dask, con los workers que haya
python3 comparativa/medir_memoria.py python3 comparativa/dask_celdas.py
```

Cada script imprime una línea `RESULTADO {...}` con el tiempo y `control_ok`.
