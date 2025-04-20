""" Data generator for drug_event.json"""

from utilities.helper import partition_id_by_year

def generate_openfda_urls(year, total_files) -> list:
    """Generate file urls for a partition"""
    quarter = 1
    files = [f'https://download.open.fda.gov/drug/event/{year}q{quarter}/drug-event-{i:04d}-of-{total_files:04d}.json.zip'
             for i in range(1,total_files+1)]
    return files

# Configure so you know expected results
def generate_openfda_partition(year, count, size_mb):
    """ Sample data generator for drug event json for batch handling"""
    return {
        'partition_id' : str(year),
        'count' : count,
        'size_mb' : round(size_mb,2),
        'files' : generate_openfda_urls(year, count)
    }

def create_drug_events_json(total_records=65000, partition_config=None):
    """
    Generate a mock OpenFDA drug events JSON structure for testing.

    Args: 
        total_records (int) : Total number of records to report in the JSON
        partitions_config (list of dict): List of partition configuration dicts.
            Each dict should contain:
                - partition_id (str): Year of the partition, e.g., '2012'.
                - count (int): Number of files in this partition.
                - size_mb (float): Total size of all files in this partition.

    Returns:
        dict: A JSON-like dictionary with nested structure under 'results.drug.event'.

    Example:
        >>> create_drug_events_json(
        ...     total_records=5,
        ...     partitions_config=[
        ...         {'partition_id': '2020', 'count': 2, 'size_mb': 100.0},
        ...         {'partition_id': '2021', 'count': 3, 'size_mb': 150.0}
        ...     ]
        ... )
    """

    # Default partition config
    if not partition_config:
        partition_config = [{
            'partition_id' : '2012',
            'count' : 2,
            'size_mb' : 85.0,
        }]

    partitions = [
        generate_openfda_partition(year=p['partition_id'],count=p['count'],size_mb=p['size_mb']) 
        for p in partition_config
        ]

    return {
        'total_records' : total_records,
        'partitions' : partitions
    }


def generate_mock_download_json(total_records=12000,partition_config=None):
    if not partition_config:
        partition_config = [{'display_name' : '2012 Q1 (part 1 of 5)', 'size_mb' : '5.0'}]
    
    # {
    #     'total_records' : 12000,
    #     'partitions' : {'display_name': '2016 Q4 (part 11 of 23)',
    #                     'file': 'https://download.open.fda.gov/drug/event/2016q4/drug-event-0011-of-0023.json.zip',
    #                     'size_mb': '9.78'
    #                     }
    # }

    partitions = [
        {
            'display_name' : p['display_name'],
            'size_mb' : p['size_mb'],
            'file' : generate_openfda_urls(partition_id_by_year(p),total_files=1)[0]
        }
        for p in partition_config
    ]

    return {
        'results' : {
            'drug' : {
                'event' : {
                    'total_records' : total_records,
                    'partitions' : partitions
                }
            }
        }
    }
