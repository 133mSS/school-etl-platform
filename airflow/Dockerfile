# File: airflow/Dockerfile

FROM apache/airflow:2.8.1-python3.11

# Switch to root for system packages
USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        python3-dev \
        libpq-dev \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements and install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt

# Set working directory
WORKDIR /opt/airflow