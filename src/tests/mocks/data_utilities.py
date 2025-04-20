""" Data generator for drug_event.json"""

import random

def generate_openfda_urls(year, total_files) -> list:
    """Generate file urls for a partition"""
    quarter = random.randint(1,4)
    files = [f'https://download.open.fda.gov/drug/event/{year}q{quarter}/drug-event-{i:04d}-of-{total_files:04d}.json.zip'
             for i in range(1,total_files+1)]
    return files

def generate_openfda_partition(start_year, end_year):
    """ Sample data generator for drug event json for batch handling"""
    partition_id = random.randint(start_year,end_year)
    count = random.randint(2,100)
    size_mb = round(random.uniform(50.0,3000.0),2)
    files = generate_openfda_urls(partition_id, count)

    return {
        'partition_id' : str(partition_id),
        'count' : count,
        'size_mb' : size_mb,
        'files' : files
    }

def create_drug_events_json(start_year=2007, end_year=2024, num_partitions= 20):
    return {
        'total_records' : random.randint(500000,1500000),
        'partitions' : [generate_openfda_partition(start_year,end_year) for _ in range(random.randint(2,num_partitions))]
    }

