FROM cluster-base

ARG spark_version=3.4.4
ARG hadoop_version=3

# Install Spark
RUN \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz

# Install GCS connector
RUN \
    curl -fsSL https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.14.jar \
    -o /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/gcs-connector-hadoop3-2.2.14.jar

ENV SPARK_HOME=/usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV SPARK_REST_PORT=6066
ENV PYSPARK_PYTHON=python3
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# -- Runtime
WORKDIR ${SHARED_WORKSPACE}