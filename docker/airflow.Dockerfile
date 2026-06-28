FROM apache/airflow:3.2.2

USER root

# Install OpenJDK-17, procps, and curl
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      openjdk-17-jdk-headless \
      procps \
      curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Spark client binaries for spark-submit command
ENV SPARK_VERSION=3.5.6
ENV SPARK_HOME=/opt/spark
RUN curl -sL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

# Install Airflow Spark provider, Postgres driver, and FAB provider for auth
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-common-sql \
    apache-airflow-providers-postgres \
    apache-airflow-providers-fab
