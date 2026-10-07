FROM python:3.12-slim-bookworm

# Mismas versiones que el dev container: Dask compara versiones entre
# cliente, scheduler y workers, y avisa o falla si no coinciden.
RUN pip install --no-cache-dir \
    "dask[complete]==2024.8.0" \
    distributed==2024.8.0 \
    dask-expr==1.1.10 \
    pandas==2.2.2 \
    numpy==2.5.3 \
    pyarrow==25.0.1 \
    cloudpickle==3.1.2 \
    msgpack==1.2.3 \
    tornado==6.5.10 \
    toolz==1.1.0 \
    pymongo==4.8.0

WORKDIR /app
