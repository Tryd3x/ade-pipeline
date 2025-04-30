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

parser.add_argument("--only_years", help="List of years to perform the job on")

args = parser.parse_args()
years = []

for dir in dirs:
    if not args.only_years:
        years = scan_years(list(bucket.list_blobs(prefix=dir)))
    else:
        years = [s for s in args.only_years.split(',')]

    for year in years:
        process_parquet(spark, bucket, dir, year)

print("Job complete!")