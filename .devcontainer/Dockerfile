FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm

# Herramientas de línea de comandos que vamos a usar seguido
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*
