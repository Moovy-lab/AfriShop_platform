FROM python:3.11-slim-bookworm

ARG PYSPARK_VERSION=3.5.3
ARG DELTA_VERSION=3.2.1
ARG SPARK_UID=50000

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17

RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}" "delta-spark==${DELTA_VERSION}" "pandas>=2.1,<3" pyarrow
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
RUN curl -fsSL -o "${SPARK_HOME}/jars/delta-spark_2.12-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" \
    && curl -fsSL -o "${SPARK_HOME}/jars/delta-storage-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"

# Même UID que le conteneur Airflow, pour que les fichiers écrits dans ./warehouse restent lisibles
RUN useradd --create-home --uid ${SPARK_UID} --gid 0 spark
USER spark
ENV PYSPARK_PYTHON=python3 SPARK_WORKER_DIR=/tmp/spark-work
WORKDIR /home/spark