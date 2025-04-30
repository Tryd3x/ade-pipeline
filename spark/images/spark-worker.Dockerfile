FROM spark-base

# -- Runtime
ARG spark_worker_web_ui=8081
EXPOSE ${spark_worker_web_ui}

# ENTRYPOINT ["bash", "-c", "spark-class org.apache.spark.deploy.worker.Worker spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} >> ${SPARK_HOME}/logs/spark-worker.out"]
