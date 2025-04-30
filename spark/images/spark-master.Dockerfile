FROM spark-base

# -- Runtime

ARG spark_master_web_ui=8080

EXPOSE ${spark_master_web_ui} ${SPARK_MASTER_PORT}
ENTRYPOINT ["bash", "-c", "spark-class org.apache.spark.deploy.master.Master >> ${SPARK_HOME}/logs/spark-master.out"]