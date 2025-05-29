from transformations import patient, drug, reaction

def get_year(blob):
    return blob.name.split("/")[3]

def get_filename(blob):
    return blob.name.split('/')[-1].split('.')[0]

def scan_years(blobs): 
    return list({get_year(blob) for blob in blobs})

def process_parquet(spark, bucket, schema, dir, year, metrics):
    schema_classes = {
        'patient' : patient.Patient,
        'drug' : drug.Drug,
        'reaction' : reaction.Reaction,
    }
    
    blobs = list(bucket.list_blobs(prefix=f"{dir}/{year}"))

    for blob in blobs:
        source_blob = f"gs://{bucket.name}/{blob.name}"
        destination_blob = f"gs://{bucket.name}/cleaned/pq/{schema}/{year}/{get_filename(blob)}"

        print(f"Reading file {source_blob}")
        df = spark.read.parquet(source_blob)

        obj = schema_classes[schema](df)
        obj.cast()
        obj.transform()

        df = obj.get_df()
        metrics.update(obj)
        
        # Write to Destination
        df.repartition(4).write.mode("overwrite").parquet(destination_blob)
        print(f"Saved to {destination_blob}")