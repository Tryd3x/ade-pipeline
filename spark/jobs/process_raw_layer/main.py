import os
import argparse
from google.cloud import storage
from pyspark.sql import SparkSession
from utils import *


JOB = "openfda_transformation"
BUCKET = "ade-pipeline-bucket"
PROMETHEUS_GATEWAY = "pushgateway:9091"
SPARK_MASTER = "spark://spark-master:7077"

spark = (
    SparkSession
    .builder
    .master(SPARK_MASTER)
    .appName("process_type_mismatch")
    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.14.jar") # GCS Connector
    # .config("spark.jars", "/home/hyderreza/codehub/ade-pipeline/spark/jobs/process_raw_layer/gcs-connector-hadoop3-2.2.14.jar") # Local GCS Connector
    .getOrCreate()
)

conf = spark.sparkContext.getConf()

print("Executor Memory:", conf.get("spark.executor.memory"))
print("Executor Cores:", conf.get("spark.executor.cores"))
print("Executor Memory Overhead:", conf.get("spark.executor.memoryOverhead"))

# Google Cloud Service Account Credentials
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))

client = storage.Client()
bucket = client.get_bucket(BUCKET)
dirs = ["data/pq/patient", "data/pq/drug", "data/pq/reaction"]

parser = argparse.ArgumentParser(description="Spark job for transformation")

parser.add_argument("--year", help="List of years to perform the job on")

args = parser.parse_args()
years = []


for dir in dirs:
    schema = dir.split('/')[-1]
    metrics = Metrics(schema=schema, job=JOB, gateway=PROMETHEUS_GATEWAY)

    params = {
        "spark" : spark,
        "bucket" : bucket,
        "schema" : schema,
        "dir" : dir,
        "metrics" : metrics
    }

    if not args.year:
        years = scan_years(list(bucket.list_blobs(prefix=dir)))
    else:
        years = [s for s in args.year.split(',')]
        print(f"Additional arguments defined: {years}")

    for year in years:
        metrics.reset()
        process_parquet(params, year, dedup_strategy="row_hash")
        metrics.publish()

    print(f"Performing threaded deletion for gateway: {schema}")
    metrics.clear()

print("Job complete!")