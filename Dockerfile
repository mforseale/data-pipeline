FROM apache/airflow:slim-3.3.2-python3.12
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless fonts-dejavu-core graphviz curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/jars \
    && curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.7.14.jar -o /opt/jars/postgresql.jar \
    && chmod 644 /opt/jars/postgresql.jar
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow
COPY requirements.txt /requirements-project.txt
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /requirements-project.txt && pip check
ENV PYTHONPATH=/opt/project \
    MPLCONFIGDIR=/tmp/matplotlib \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python
WORKDIR /opt/project
