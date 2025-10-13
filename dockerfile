FROM apache/airflow:2.9.0

# Install PostgreSQL development libraries and OpenJDK 17
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    libpq-dev \
    build-essential \
    openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment for Spark/PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Copy requirements and install additional packages (excluding apache-airflow since it's already in base image)
COPY requirements-airflow.txt .

RUN python3 -m pip install --upgrade pip && \
    pip install -r requirements-airflow.txt

COPY dags /opt/airflow/dags
COPY scripts /opt/airflow/scripts
