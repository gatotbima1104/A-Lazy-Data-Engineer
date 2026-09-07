FROM apache/airflow:3.1.6

USER root

RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .

RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install \
    --no-cache-dir \
    -r requirements.txt \
    --no-build-isolation

ENV PYTHONPATH=/opt/airflow/project