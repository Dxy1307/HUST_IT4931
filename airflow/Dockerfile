FROM apache/airflow:2.9.2

# Set Python and PySpark versions
ARG PYTHON_VERSION=3.9
ARG PYSPARK_VERSION=3.5.3

# Switch to root to install system dependencies
USER root

# Install system dependencies and Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

    # Switch back to airflow user
USER airflow


# Install PySpark and other required packages
RUN pip install --no-cache-dir \
    pyspark==${PYSPARK_VERSION} \
    apache-airflow-providers-apache-spark \
    cassandra-driver \
    elasticsearch

