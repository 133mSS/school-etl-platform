# File: airflow/Dockerfile

FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        python3-dev \
        libpq-dev \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN pip install apache-airflow-providers-prometheus

USER airflow


COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt

WORKDIR /opt/airflow