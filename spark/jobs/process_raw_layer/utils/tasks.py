from transformations import patient, drug, reaction

def get_year(blob):
    return blob.name.split("/")[3]

def get_filename(blob):
    return blob.name.split('/')[-1].split('.')[0]

def scan_years(blobs): 
    return list({get_year(blob) for blob in blobs})

def process_parquet(spark, bucket, dir, year):
    blobs = list(bucket.list_blobs(prefix=f"{dir}/{year}"))
    for blob in blobs:
        source_blob = f"gs://{bucket.name}/{blob.name}"
        schema = dir.split('/')[-1]
        destination_blob = f"gs://{bucket.name}/cleaned/pq/{schema}/{year}/{get_filename(blob)}"

        print(f"Reading file {source_blob}")
        df = spark.read.parquet(source_blob)

        if schema == "patient":
            p = patient.Patient(df)
            p.cast()
            p.transform()
            df = p.get_df()
        elif schema == "drug":
            d = drug.Drug(df)
            d.cast()
            d.transform()
            df = d.get_df()
        elif schema == "reaction":
            r = reaction.Reaction(df)
            r.cast()
            r.transform()
            df = r.get_df()
        
        # Write to Destination
        df.repartition(4).write.mode("overwrite").parquet(destination_blob)
        print(f"Saved to {destination_blob}")