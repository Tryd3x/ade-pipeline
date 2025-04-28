import os
from pyspark.sql import SparkSession, functions as F
from google.cloud import storage
from pyspark.sql.types import StringType

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
        destination_blob = f"gs://{bucket.name}/processed/pq/{dir.split('/')[-1]}/{year}/{get_filename(blob)}"

        print(f"Reading file {source_blob}")
        # Read parquet
        df = spark.read.parquet(source_blob)

        # Castnig to String
        for col in df.columns:
            df = df.withColumn(col, F.col(col).cast(StringType()))

        # Write to Destination
        df.repartition(4).write.mode("overwrite").parquet(destination_blob) 
        print(f"Saved to {destination_blob}")

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

for dir in dirs:
    years = scan_years(list(bucket.list_blobs(prefix=dir)))
    # years = ['2004']
    for year in years:
        process_parquet(bucket, dir, year)