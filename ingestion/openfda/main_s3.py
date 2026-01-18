import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

import json
import requests
import subprocess
from time import time
from dotenv import load_dotenv
import boto3
from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple, Union, Any

load_dotenv()

from openfda.utilities.ade import ADE
from openfda.utilities.helper import *
from openfda.utilities.logger_config import get_module_logger
from openfda.utilities.custom_types import DrugEventsOutput, Partition

logger = get_module_logger(__name__)

def benchmark(function):
    def wrapper(*args, **kwargs):
        start = time()
        function(*args, **kwargs)
        end = time()
        print(f"Finished in {end-start:.2f} seconds")
    return wrapper

def extract_drug_events(json: Dict[str, Any]) -> DrugEventsOutput:
    """Restructures JSON object to handle batch processing better.

    Parameters:
        json (dict): Raw JSON data containing drug event information.

    Returns:
        dict: A dictionary with total number of records and list of partitions.

    The returned dictionary contains:
        - `total_records` (Optional[int]): Total number of records.
        - `partitions` (List[Dict[str, Any]]): List of partition info dicts, each containing:
            - 'partition_id' (str): Year-based partition identifier.
            - 'records' (int): Number of records in partition.
            - 'count' (int): Number of files in partition.
            - 'size_mb' (float): Total size of files in MB.
            - 'files' (List[str]): List of filenames in partition.
    """


    if not json or 'results' not in json:
        raise ValueError("Invalid input: Missing 'results' key.")

    events = json.get('results',{}).get('drug',{}).get('event',{})
    total_records = events.get('total_records')
    partitions = events.get('partitions',[])

    # Generate unique partition_id and its count
    partition_ids: Dict[str, int] = {}
    for p in partitions:
        # Extract year as partition_id
        id = partition_id_by_year(p)

        # Track count
        partition_ids[id] = partition_ids.get(id,0) + 1
    
    # Groups partition by partitionid
    results: List[Partition] = []
    for item in partition_ids.items():
        id, count = item
        file_list = []
        counter = 0
        tot_size = 0.0
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

def create_batch(partitions: List[Partition], max_batch_size_mb: int = 10000) -> Tuple[List, List]:
    """
    Segregates partitions into batches based on a disk size threshold.

    Partitions with size exceeding max_batch_size_mb are collected separately for special handling.

    Parameters:
        partitions (list): List of partition dictionaries, each expected
            to have a 'size_mb' key indicating its size in megabytes.
        max_batch_size_mb (int): Maximum allowed size (in MB) per batch.
            Defaults to 10000.

    Returns:
        tuple (list, list): A tuple with list of batches, list of oversized partitions
    """

    batch = []                              # partitions per batch
    batch_partitions: List[Partition] = []  # Partitions under the threshold
    big_batch_partitions = []               # Different approach to process bigger partitions
    sum_size = 0.0                          # Size counter

    for p in partitions:
        size = p.get('size_mb', 0)

        # Handle oversized partititions
        if size > max_batch_size_mb:
            big_batch_partitions.append(p)
            continue
        
        # Initiate another batch
        if sum_size + size > max_batch_size_mb:
            batch.append(batch_partitions.copy())
            batch_partitions.clear()
            sum_size = 0
        
        # Append partition to batch
        batch_partitions.append(p)
        sum_size += size

    # Flush batch_partitions to schedule as last batch
    if len(batch_partitions) != 0:
        batch.append(batch_partitions.copy())
        batch_partitions.clear()

    logger.info(f"Created {len(batch)} batches, with {len(big_batch_partitions)} oversized partitions.")

    return (batch, big_batch_partitions)

def upload_to_s3(local_base_dir: str, bucket_name: str, s3_prefix: str) -> None:
    """
    Uploads files from local directory to an AWS S3 bucket.

    Parameters:
        local_base_dir (str): Path to upload files from
        bucket_name (str): AWS S3 Bucket to upload files to
        s3_prefix (str): Key prefix to store the files in AWS S3 Bucket

    """
    
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not aws_access_key or not aws_secret_key:
        raise EnvironmentError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in .env or environment.")
    try:
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

        for root, _, files in os.walk(local_base_dir):
            for file in sorted(files):
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_base_dir)
                s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
                s3_client.upload_file(local_file_path, bucket_name, s3_key)
                
                logger.info(f"Uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to upload: {e}")


def validate_hash(ade: ADE, metadata: Dict[str, Dict[str, Any]], filename: str) -> bool:
    """
    Validate the records and hash of each schema to detect changes

    Parameters:
        ade (ADE): Object of class `ADE`
        metadata (dict): Metadata to check against
        filename (str): Name of t

    Returns
        bool : `True` if records and hash match else `False`
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

def download_file(url: str, download_path: str , filename: str) -> str:
    """
    Download and save file from URL

    Parameters:
        url (str): Download file from `url`
        download_path (str): Path to save downloaded file
        filename(str): Name of the downloaded file
    
    Returns:
        str: Path of downloaded file
    """
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

def save_metadata(save_to: str, metadata: Dict[str, Dict[str, Any]]) -> None:
    """
    Save metadata to local path

    Parameters:
        save_to (str) : Path to save metadata
        metadata (dict) : Metadata

    """
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
    
def reset_dir(dir: List[str]) -> None:
    """
    Prunes contents of the directory (Directory remains intact)

    Paramters:
        dir (str): Directory to prune
    """

    for d in dir:
        logger.info(f"Purging files in '{d}'")
        wildcard_path = os.path.join(d, "*")
        popen = subprocess.Popen(f"rm -rfv {wildcard_path}", stdout=subprocess.PIPE, shell=True, text=True)
    
        if popen.stdout is not None:
            for o in popen.stdout:
                logger.info(o.strip())
    
    logger.info("Purge completed")

def clear_dir(dir: str) -> None:
    """
    Delete contents and structure of the directory

    Parameters:
        dir (str): Directory to delete
    """
    logger.info("Deleting temporary directories")
    popen = subprocess.Popen(f"rm -rvf {dir}", stdout=subprocess.PIPE, shell=True,text=True)
    if popen.stdout is not None:
        for o in popen.stdout:
            logger.info(o.strip())

@benchmark
def process_batch(batch: List, bucket: str) -> None:
    """
    Process batches of partitions by downloading, extracting, validating, converting to Parquet,
    updating metadata, and uploading to S3.

    This function iterates over a batch of partition groups. For each partition, it:
    - Downloads JSON files from URLs.
    - Extracts and parses drug event data.
    - Validates content changes using hashes.
    - Updates metadata for each schema.
    - Saves data as Parquet files locally.
    - Uploads Parquet files and metadata to AWS S3.
    - Manages temporary directories.

    Parameters:
        batch (list):
            A list where each element is a list of partition dictionaries to be processed together.
        bucket (str):
            The name of the AWS S3 bucket where processed files and metadata are uploaded.

    Raises:
        subprocess.CalledProcessError:
            If downloading or unzipping a file fails during processing.
        Exception:
            For any unexpected errors during processing, which are logged and do not stop batch processing.
    """

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

        # Partitioon iteration
        for j,p in enumerate(b):
            logger.info(f'----------------- Processing partition {j+1} -----------------')  

            schema = ['patient', 'drug', 'reaction']
            year = p.get('partition_id')
            files = p.get('files')      
            total_count = p.get('count')
            file_count = 1

            metadata = {}

            # URL iteration
            for f in files:
                filename = f"drug-event-part-{file_count}-of-{total_count}"

                try:
                    # Saved to tmp folder ./temp/raw/drug-event-part-1-of-x.json
                    filepath = download_file(url=f, download_path=RAW_DIR, filename=filename)

                    # Load and extract JSON
                    ade = ADE(year)
                    ade.extractJSON(read_json_file(filepath))

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

            # Upload metadata and parquet files to S3 bucket
            upload_to_s3(local_base_dir=PQ_DIR, bucket_name=bucket, s3_prefix="data/pq")
            logger.info(f"Uploaded partition '{year}' parquet files to S3.")

            # Purge temp folder to prepare for next partition iteration
            reset_dir(dir=tmp_dirs)

        logger.info('===================================================================')
        logger.info(f'============================= Batch {i+1} END =========================')
        logger.info('===================================================================')
    

    # Clear temp directories
    clear_dir(dir=TEMP_DIR)
    logger.info("Batch Processing Completed!")

if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_argument("--year",help="Year to perform extraction on")
    parser.add_argument("--max_batch_size_mb",help="Max size of batch in MB")
    args = parser.parse_args()
    year = args.year

    JOB = "openfda_ingestion"
    URL = "https://api.fda.gov/download.json"
    BUCKET = "ade-pipeline-s3-bucket"
    MAX_BATCH_SIZE_MB = int(args.max_batch_size_mb) if args.max_batch_size_mb else 13000

    logger.info(f"Fetching data: {URL}")
    res = requests.get(URL)

    logger.info("Remapping JSON object")
    data = res.json()
    downloads_json = extract_drug_events(data)
    partitions = downloads_json.get('partitions', [])

    logger.info(f"S3 bucket: {BUCKET}")

    if not year:
        logger.info(f"Additional argument: None")
        logger.info(f"Creating Batches [max_batch_size={MAX_BATCH_SIZE_MB}]")
        batch, _ = create_batch(partitions, max_batch_size_mb=MAX_BATCH_SIZE_MB)
        process_batch(batch, BUCKET)
    else:
        logger.info(f"Additional argument: --year={year}")
        logger.info(f"Creating Batches [max_batch_size={MAX_BATCH_SIZE_MB}]")
        logger.info(f"Filtering partition for year: {year}")
        filtered_partitions = filter_partition(year, partitions) 
        batch, _ = create_batch(filtered_partitions, max_batch_size_mb=MAX_BATCH_SIZE_MB)
        process_batch(batch, BUCKET)