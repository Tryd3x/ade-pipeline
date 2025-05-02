#!/bin/bash
spark_version="3.4.4"
hadoop_version="3"

export JAVA_HOME="/usr/local/openjdk-11"
export SPARK_HOME="/usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}"
export SPARK_MASTER_HOST="spark-master"
export SPARK_MASTER_PORT="7077"
export SPARK_REST_PORT="6066"
SPARK_LOCAL_IP="spark-master"
export PYSPARK_PYTHON="python3"
export PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
export GOOGLE_APPLICATION_CREDENTIALS="/keys/gcs-credentials.json"
