import os
from pyspark.sql import SparkSession, functions as F
from google.cloud import storage
from pyspark.sql.types import StringType, IntegerType, FloatType
import argparse

def get_year(blob):
    return blob.name.split("/")[3]

def get_filename(blob):
    return blob.name.split('/')[-1].split('.')[0]

def scan_years(blobs): 
    return list({get_year(blob) for blob in blobs})

def process_parquet(bucket, dir, year):
    blobs = list(bucket.list_blobs(prefix=f"{dir}/{year}"))
    for blob in blobs:
        source_blob = f"gs://{bucket.name}/{blob.name}"
        destination_blob = f"gs://{bucket.name}/cleaned/pq/{dir.split('/')[-1]}/{year}/{get_filename(blob)}"

        print(f"Reading file {source_blob}")
        # Read parquet
        df = spark.read.parquet(source_blob)
        
        # Cast to right types
        # df = perform_cast(df)

        # Write to Destination
        df.repartition(4).write.mode("overwrite").parquet(destination_blob)
        print(f"Saved to {destination_blob}")

SPARK_MASTER = "spark://spark-master:7077"

spark = (
    SparkSession
    .builder
    # .master(SPARK_MASTER)
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
        process_parquet(bucket, dir, year)

print("Job complete!")