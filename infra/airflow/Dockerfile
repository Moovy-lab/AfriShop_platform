FROM apache/airflow:2.9.3-python3.11

ARG AIRFLOW_VERSION=2.9.3
ARG PYSPARK_VERSION=3.5.3
ARG DELTA_VERSION=3.2.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17

USER airflow
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-apache-spark \
      apache-airflow-providers-postgres \
      "pyspark==${PYSPARK_VERSION}" \
      "delta-spark==${DELTA_VERSION}" \
      "pandas>=2.1,<3" pyarrow python-json-logger pytest pytest-cov

# dbt et Great Expectations isolés dans leurs propres environnements
RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install \
         --no-cache-dir \
         --default-timeout=300 \
         --retries=10 \
         "dbt-core~=1.8.0" \
         "dbt-postgres~=1.8.0" \
    && python -m venv /home/airflow/gx_venv \
    && /home/airflow/gx_venv/bin/pip install \
         --no-cache-dir \
         --default-timeout=300 \
         --retries=10 \
         "great-expectations>=1.0,<2" \
         pandas \
         pyarrow \
         "sqlalchemy>=2" \
         psycopg2-binary


# Jars Delta intégrés à l'image (pas de téléchargement au runtime)
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PYSPARK_PYTHON=python3

ARG POSTGRES_JDBC_VERSION=42.7.8

RUN curl -fL --retry 10 --retry-delay 5 --retry-all-errors \
      -o "${SPARK_HOME}/jars/delta-spark_2.12-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" \
    && curl -fL --retry 10 --retry-delay 5 --retry-all-errors \
      -o "${SPARK_HOME}/jars/delta-storage-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    && curl -fL --retry 10 --retry-delay 5 --retry-all-errors \
      -o "${SPARK_HOME}/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar"

