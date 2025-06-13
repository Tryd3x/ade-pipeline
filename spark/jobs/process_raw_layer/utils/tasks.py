from process_raw_layer.transformations import patient, drug, reaction
from pyspark.sql import functions as F

def get_year(blob):
    return blob.name.split("/")[3]

def get_filename(blob):
    return blob.name.split('/')[-1].split('.')[0]

def scan_years(blobs): 
    return list({get_year(blob) for blob in blobs})

def process_parquet(params, year, dedup_strategy = "row_hash"):

    spark = params['spark']
    bucket = params['bucket']
    schema = params['schema']
    dir = params['dir']
    metrics = params['metrics']

    schema_classes = {
        'patient' : patient.Patient,
        'drug' : drug.Drug,
        'reaction' : reaction.Reaction,
    }
    
    blobs = list(bucket.list_blobs(prefix=f"{dir}/{year}"))

    for blob in blobs:
        source_blob = f"gs://{bucket.name}/{blob.name}"
        destination_blob = f"gs://{bucket.name}/cleaned/pq/{schema}/{year}/"

        print(f"Reading file {source_blob}")
        df = spark.read.parquet(source_blob)

        obj = schema_classes[schema](df)
        obj.cast()
        obj.transform()

        df = obj.get_df()
        metrics.update(obj)

        if dedup_strategy == 'row_hash':
            df = df.withColumn("_dedup_key", F.sha2(F.concat_ws("||", *df.columns), 256))
            df = df.dropDuplicates(["_dedup_key"]).drop("_dedup_key")
        elif dedup_strategy == 'col_key' and 'patientid' in df.columns:
            df = df.dropDuplicates(["patientid"])
        
        # Write to Destination
        df.repartition(4).write.mode("append").parquet(destination_blob)
        print(f"Saved to {destination_blob}")