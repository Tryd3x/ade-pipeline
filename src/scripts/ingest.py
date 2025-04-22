import os
import shlex
import requests
import subprocess
from time import sleep
from scripts.ade import ADE
from google.cloud import storage
from dotenv import load_dotenv
load_dotenv()

from utilities import part_size_mb, partition_id_by_year, read_json_file, get_module_logger

logger = get_module_logger(__name__)

def extract_drug_events(json):
    """Restructures JSON object to handle batch processing better"""
    if not json or 'results' not in json:
        raise ValueError("Invalid input: Missing 'results' key.")

    events = json.get('results',{}).get('drug',{}).get('event',{})
    total_records = events.get('total_records')
    partitions = events.get('partitions',[])

    # Generate unique partition_id and its count
    partition_ids = {}
    for p in partitions:
        # Extract year as partition_id
        id = partition_id_by_year(p)

        # Number of occurences
        partition_ids[id] = partition_ids.get(id,0) + 1
    
    # Groups partition by partitionid
    results = []
    for item in partition_ids.items():
        id, count = item
        file_list = []
        counter = 0
        tot_size = 0

        for p in partitions:
            if counter == count:
                break
            if partition_id_by_year(p) == id:
                counter+=1
                file_list.append(p.get('file'))
                tot_size+=part_size_mb(p)
                
        results.append(
            {
                "partition_id": id,
                "count": count,
                "size_mb" : round(tot_size,2),
                "files" : file_list
            }
        )
    
    return {
        "total_records" : total_records,
        "partitions" : results
    }

def create_batch(partitions, max_batch_size_mb=10000):
    """Seggregates partitions as batches based on disksize threshold"""
    batch = []                  # partitions per batch
    batch_partitions = []       # Partitions under the threshold
    big_batch_partitions = []   # Different approach to process bigger partitions
    sum_size = 0                # Size counter

    for p in partitions:
        size = p.get('size_mb', 0)

        if size > max_batch_size_mb:
            # TODO:
            # Handle oversized partititions
            big_batch_partitions.append(p)
            continue
        
        if sum_size + size > max_batch_size_mb:
            # TODO:
            # - Declare batch_partitions as batch #
            # - Reset sum_size
            # - Reset batch_partitions
            batch.append(batch_partitions.copy())
            batch_partitions.clear()
            sum_size = 0
        
        batch_partitions.append(p)
        sum_size += size

    # Flush batch_partitions to schedule as last batch
    if len(batch_partitions) != 0:
        batch.append(batch_partitions.copy())
        batch_partitions.clear()

    logger.info(f"Created {len(batch)} batches, with {len(big_batch_partitions)} oversized partitions.")

    return batch, big_batch_partitions

def upload_to_gcs(local_base_dir, bucket_name, gcs_prefix):
    """Uploads files from local directory to a GCS bucket."""
    
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS not set in .env or environment.")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for root, _, files in os.walk(local_base_dir):
        for file in sorted(files):
            if file.endswith(".parquet"):
                local_file_path = os.path.join(root, file)
                
                # Relative path from the local_base_dir
                relative_path = os.path.relpath(local_file_path, local_base_dir)
                gcs_blob_path = os.path.join(gcs_prefix, relative_path).replace("\\", "/")
                blob = bucket.blob(gcs_blob_path)
                blob.upload_from_filename(local_file_path)
                
                logger.info(f"Uploaded {local_file_path} to gs://{bucket_name}/{gcs_blob_path}")

def process_batch(batch):
    logger.info("Initiating Batch Processing")

    # Directories of temporary files and folder
    RAW_DIR = "./raw"
    PQ_DIR = "./pq"
    tmp_dirs = [RAW_DIR, PQ_DIR]

    # Create temporary directories
    if not os.path.exists(RAW_DIR):
        logger.info("Directory 'raw' missing. Created 'raw'")
        os.makedirs(RAW_DIR,exist_ok=True)

    # Insert batch iteration here
    for i, b in enumerate(batch):
        logger.info('===================================================================')
        logger.info(f'============================= BATCH {i+1} =============================')
        logger.info('===================================================================')

        # Insert partition iteration here
        for j,p in enumerate(b):
            logger.info(f'----------------- Processing partition {j+1} -----------------')  

            files = p.get('files')
            total_count = p.get('count')
            file_count = 1
            for f in files:
                dl_filename = f"drug-event-part-{file_count}-of-{total_count}"
                try:
                    # Download and unzip
                    logger.info(f"Download started: {f}")

                    dl_filepath = os.path.join(RAW_DIR,f"{dl_filename}.json")  
                    result = subprocess.run(
                        f'wget -q -O - {shlex.quote(f)} | gunzip > {shlex.quote(dl_filepath)}',
                        shell=True,
                        check=True,
                        capture_output=True,
                        text=True
                    )

                    # Saved to tmp folder
                    logger.info(f"File saved to: {dl_filepath}")

                    # Load JSON and map to class ADE
                    ade = ADE()
                    temp_json = read_json_file(dl_filepath)
                    ade.extractJSON(temp_json)
                    logger.info(f"Parsed json file to ADE object: {dl_filepath}")

                    # Save ADE object as parquet file
                    ade.save_as_parquet(fname=dl_filename, dir=p.get('partition_id'))

                    # Increment part number
                    file_count+=1

                except subprocess.CalledProcessError as e:
                    logger.error(f"(return {e.returncode}) Failed to download or unzip: {f}")
                    logger.error(f"{e.stderr.strip()}")
                except Exception as e:
                    logger.error(f"Unexpected error occured: {e}")

            # Upload parquet file to GCS bucket
            upload_to_gcs(local_base_dir=PQ_DIR,bucket_name='zoomcamp-454219-ade-pipeline',gcs_prefix="data/pq")
            logger.info(f"Uploaded partition '{p.get('partition_id')}' parquet files to GCS.")

            # Purge tmp folder to prepare for next partition
            for dir_path in tmp_dirs:
                logger.info(f"Purging files in'{dir_path}'")
                wildcard_path = os.path.join(dir_path, "*")
                popen = subprocess.Popen(f"rm -rfv {wildcard_path}", stdout=subprocess.PIPE, shell=True, text=True)

                for o in popen.stdout:
                    logger.info(o.strip())
            
            logger.info("Purge completed")

        logger.info('===================================================================')
        logger.info(f'============================= Batch {i} END =========================')
        logger.info('===================================================================')
    
    # Summary of the batch script
    # Total batch processed
    # Size of each batch processed
    # Etc
    logger.info("Batch Processing Completed!")

if __name__ == '__main__':
    print("Executing ingest.py")
    URL = "https://api.fda.gov/download.json"
    MAX_BATCH_SIZE_MB = 13000

    res = requests.get(URL)
    logger.info(f"Fetching data: {URL}")

    logger.info("Remapping JSON object")
    data = res.json()
    downloads_json = extract_drug_events(data)

    logger.info(f"Creating Batches [max_batch_size={MAX_BATCH_SIZE_MB}]")
    batch, _ = create_batch(
        downloads_json.get('partitions'),
        max_batch_size_mb=MAX_BATCH_SIZE_MB
        )
    
    process_batch(batch)


    print("Terminating in 5 mins...")
    sleep(300)