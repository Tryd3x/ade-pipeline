import os
import sys
import json
import requests
import subprocess
from time import time
from dotenv import load_dotenv
from google.cloud import storage
from argparse import ArgumentParser
from datetime import datetime, timezone

load_dotenv()

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0,parent_dir)

from openfda.utilities.ade import ADE
from openfda.utilities.helper import *
from openfda.utilities.metrics import Metrics
from openfda.utilities.logger_config import get_module_logger


logger = get_module_logger(__name__)

def benchmark(function):
    def wrapper(*args, **kwargs):
        start = time()
        function(*args, **kwargs)
        end = time()
        print(f"Finished in {end-start:.2f} seconds")
    return wrapper

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
        records = 0

        for p in partitions:
            if counter == count:
                break
            if partition_id_by_year(p) == id:
                counter+=1
                file_list.append(p.get('file'))
                tot_size+=part_size_mb(p)
                records+=p.get('records')

        results.append(
            {
                "partition_id": id,
                "records" : records,
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
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_base_dir)
            gcs_blob_path = os.path.join(gcs_prefix, relative_path).replace("\\", "/")
            blob = bucket.blob(gcs_blob_path)
            blob.upload_from_filename(local_file_path)
            
            logger.info(f"Uploaded {local_file_path} to gs://{bucket_name}/{gcs_blob_path}")

def fetch_metadata_from_gcs(schema, year, bucket):
    client = storage.Client()
    bucket = client.bucket(bucket)
    metadata = {}
    logger.info("Fetching metadata")
    for s in schema:
        blob = bucket.get_blob(f"data/pq/{s}/{year}/_METADATA.json")
        if blob is not None:
            metadata[s] = json.loads(blob.download_as_text())
        else:
            logger.info(f"Blob not found")
            return {}
    
    return metadata

def validate_hash(ade, metadata, filename):
    """
    returns
        `True` : Records and Hash match
        `False` : Records and Hash mismatch
    """
    logger.info("Validating Hash...")
    if len(metadata.keys()) < 1:
        logger.info("No metadata found")
        return False
    
    for s, count, hash in zip(metadata.keys(), ade.row_count(), ade.get_hash()):
        metadata[s]['files'].setdefault(f'{filename}.parquet', {})
        file_metadata = metadata[s]['files'][f'{filename}.parquet']
        file_metadata.setdefault('content_hash','')
        file_metadata.setdefault('records',-1)

        if (file_metadata['records'] != count) or (file_metadata['content_hash'] != hash):
            logger.info(f"Detected metadata changes")
            return False
    
    return True

def download_file(url, download_path , filename):
    filepath = os.path.join(download_path,f"{filename}.json")
    if not os.path.exists(download_path):
        logger.info(f"Directory missing: {download_path}")
        logger.info(f"Created directory: {download_path}")
        os.makedirs(download_path, exist_ok=True)

    logger.info(f"Downloading: {url}")

    subprocess.run(
        f'wget -q -O - {url} | gunzip > {filepath}',
        shell=True,
        check=True,
        capture_output=True,
        text=True
    )

    logger.info(f"File saved to: {filepath}")
    return filepath

def save_metadata(save_to, metadata):
    for s in metadata.keys():
        try:
            metadata[s]['generated_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

            meta_path = os.path.join(save_to, s, metadata[s]['year'])

            if not os.path.exists(meta_path):
                logger.info(f"Directory missing: {meta_path}")
                logger.info(f"Created directory: {meta_path}")
                os.makedirs(meta_path, exist_ok=True)

            with open(os.path.join(meta_path,"_METADATA.json"), "w") as f:
                json.dump(metadata[s], f, indent=4)
        
        except Exception as e:
            logger.error(f"Exception caught: {e}")
    
def reset_dir(dir):
    for d in dir:
        logger.info(f"Purging files in '{d}'")
        wildcard_path = os.path.join(d, "*")
        popen = subprocess.Popen(f"rm -rfv {wildcard_path}", stdout=subprocess.PIPE, shell=True, text=True)

        for o in popen.stdout:
            logger.info(o.strip())
    
    logger.info("Purge completed")

def clear_dir(dir):
    logger.info("Deleting temporary directories")
    popen = subprocess.Popen(f"rm -rvf {dir}", stdout=subprocess.PIPE, shell=True,text=True)
    for o in popen.stdout:
        logger.info(o.strip())

@benchmark
def process_batch(batch, metrics, bucket):
    logger.info("Initiating Batch Processing")

    # Create temp directories
    TEMP_DIR = "./temp/"
    RAW_DIR = os.path.join(TEMP_DIR,"raw")
    PQ_DIR = os.path.join(TEMP_DIR,"pq")
    tmp_dirs = [RAW_DIR, PQ_DIR]

    # Batch iteration
    for i, b in enumerate(batch):
        logger.info('===================================================================')
        logger.info(f'============================= BATCH {i+1} =============================')
        logger.info('===================================================================')

        # Reset metrics
        metrics.reset()

        # Partitioon iteration
        for j,p in enumerate(b):
            logger.info(f'----------------- Processing partition {j+1} -----------------')  

            schema = ['patient', 'drug', 'reaction']
            year = p.get('partition_id')
            files = p.get('files')      
            total_count = p.get('count')
            file_count = 1

            metadata = fetch_metadata_from_gcs(schema, year, bucket)

            # URL iteration
            for f in files:
                filename = f"drug-event-part-{file_count}-of-{total_count}"

                try:
                    # Saved to tmp folder ./temp/raw/drug-event-part-1-of-x.json
                    filepath = download_file(url=f, download_path=RAW_DIR, filename=filename)

                    # Load and extract JSON
                    ade = ADE(year)
                    ade.extractJSON(read_json_file(filepath))

                    # Update metrics
                    metrics.update(ade)

                    logger.info(f"Parsed json file to : {filepath}")

                    # Validate hash for change in metadata
                    if validate_hash(ade, metadata, filename):
                        logger.info("Hash and Count validated. No Changes Detected.")
                        file_count+=1
                        continue

                    # Initialize/Update metadata for each schema
                    for s, hash, count in zip(schema, ade.get_hash(), ade.row_count()):
                        metadata.setdefault(s, {})
                        metadata[s]['schema'] = s
                        metadata[s]['year'] = year
                        metadata[s]['total_records'] = metadata[s].get('total_records',0) + count 

                        metadata[s].setdefault('files',{})
                        metadata[s]['files'].setdefault(f'{filename}.parquet',{})
                        metadata[s]['files'][f'{filename}.parquet']['records'] = count
                        metadata[s]['files'][f'{filename}.parquet']['content_hash'] = hash

                    # Save as parquet file to ./temp/pq/<schema>/<year>/drug-event-part-1-of-x.parquet
                    ade.save_as_parquet(save_to=PQ_DIR, fname=filename)
                    file_count+=1

                except subprocess.CalledProcessError as e:
                    logger.error(f"(return {e.returncode}) Failed to download or unzip: {f}")
                    logger.error(f"{e.stderr.strip()}")
                except Exception as e:
                    logger.error(f"Unexpected error occured: {e}")

            # Save updated metadata
            save_metadata(save_to=PQ_DIR, metadata=metadata)

            # Upload metadata and parquet files to GCS bucket
            upload_to_gcs(local_base_dir=PQ_DIR, bucket_name=bucket, gcs_prefix="data/pq")
            logger.info(f"Uploaded partition '{year}' parquet files to GCS.")

            # Purge temp folder to prepare for next partition iteration
            reset_dir(dir=tmp_dirs)

        logger.info('===================================================================')
        logger.info(f'============================= Batch {i+1} END =========================')
        logger.info('===================================================================')
    
        # Publish Metrics
        metrics.publish()

    # Clear temp directories
    clear_dir(dir=TEMP_DIR)
    logger.info("Batch Processing Completed!")

if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_argument("--year",help="Year to perform extraction on")
    parser.add_argument("--max_batch_size_mb",help="Max size of batch in MB")
    parser.add_argument("--metrics_gateway",help="Prometheus pushgateway url -> host:port")
    args = parser.parse_args()
    year = args.year

    JOB = "openfda_ingestion"
    URL = "https://api.fda.gov/download.json"
    BUCKET = "ade-pipeline-bucket"
    MAX_BATCH_SIZE_MB = int(args.max_batch_size_mb) if args.max_batch_size_mb else 13000
    PROMETHEUS_GATEWAY = args.metrics_gateway if args.metrics_gateway else None

    logger.info(f"Fetching data: {URL}")
    res = requests.get(URL)

    logger.info("Remapping JSON object")
    data = res.json()
    downloads_json = extract_drug_events(data)
    partitions = downloads_json.get('partitions')

    metrics = Metrics(job=JOB, gateway=PROMETHEUS_GATEWAY)
    logger.info(f"Metrics gateway: {PROMETHEUS_GATEWAY}")
    logger.info(f"GCS bucket: {BUCKET}")

    if not year:
        logger.info(f"Additional argument: None")
        logger.info(f"Creating Batches [max_batch_size={MAX_BATCH_SIZE_MB}]")
        batch, _ = create_batch(partitions, max_batch_size_mb=MAX_BATCH_SIZE_MB)
        process_batch(batch, metrics, BUCKET)
    else:
        logger.info(f"Additional argument: --year={year}")
        logger.info(f"Creating Batches [max_batch_size={MAX_BATCH_SIZE_MB}]")
        logger.info(f"Filtering partition for year: {year}")
        filtered_parititons = filter_partition(year, partitions) 
        batch, _ = create_batch(filtered_parititons,max_batch_size_mb=MAX_BATCH_SIZE_MB)
        process_batch(batch, metrics, BUCKET)
    
    metrics.close()
    