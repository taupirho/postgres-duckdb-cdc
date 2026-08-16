FROM postgres:17.11-bookworm

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
       postgresql-17-wal2json \
    && rm -rf /var/lib/apt/lists/*
