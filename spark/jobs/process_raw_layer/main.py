import os
import argparse
from google.cloud import storage
from pyspark.sql import SparkSession
from utils import *
SPARK_MASTER = "spark://spark-master:7077"

spark = (
    SparkSession
    .builder
    .master(SPARK_MASTER)
    .appName("process_type_mismatch")
    # .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.14.jar") # GCS Connector
    .getOrCreate()
)

# Google Cloud Service Account Credentials
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))

client = storage.Client()
bucket = client.get_bucket("zoomcamp-454219-ade-pipeline")
dirs = ["data/pq/patient", "data/pq/drug", "data/pq/reaction"]

parser = argparse.ArgumentParser(description="Spark job to fix mixed datatype")

parser.add_argument("--year", help="List of years to perform the job on")

args = parser.parse_args()
years = []

JOB = "openfda_transformation"
PROMETHEUS_GATEWAY = "pushgateway:9091"

for dir in dirs:
    schema = dir.split('/')[-1]

    metrics = Metrics(schema=schema, job=JOB, gateway=PROMETHEUS_GATEWAY)

    if not args.year:
        years = scan_years(list(bucket.list_blobs(prefix=dir)))
    else:
        years = [s for s in args.year.split(',')]
        print(f"Additional arguments defined: {years}")

    for year in years:
        metrics.reset()
        process_parquet(spark, bucket, schema, dir, year, metrics)
        metrics.publish()

    print(f"Performing threaded deletion for gateway: {schema}")
    metrics.clear()

print("Job complete!")