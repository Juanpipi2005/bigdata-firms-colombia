FROM python:3.12-slim-bookworm

# Java 17 para Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps && \
    rm -rf /var/lib/apt/lists/*

# JAVA_HOME independiente de la arquitectura (amd64 / arm64)
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

# Misma version que requirements-dev.txt: driver y cluster deben coincidir
RUN pip install --no-cache-dir pyspark==3.5.1
ENV SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3

# Conector de MongoDB y sus dependencias, descargadas al construir la imagen
ARG MVN=https://repo1.maven.org/maven2
RUN cd "${SPARK_HOME}/jars" && \
    curl -fsSLO ${MVN}/org/mongodb/spark/mongo-spark-connector_2.12/10.7.0/mongo-spark-connector_2.12-10.7.0.jar && \
    curl -fsSLO ${MVN}/org/mongodb/mongodb-driver-sync/5.1.4/mongodb-driver-sync-5.1.4.jar && \
    curl -fsSLO ${MVN}/org/mongodb/bson/5.1.4/bson-5.1.4.jar && \
    curl -fsSLO ${MVN}/org/mongodb/mongodb-driver-core/5.1.4/mongodb-driver-core-5.1.4.jar && \
    curl -fsSLO ${MVN}/org/mongodb/bson-record-codec/5.1.4/bson-record-codec-5.1.4.jar

WORKDIR /app
