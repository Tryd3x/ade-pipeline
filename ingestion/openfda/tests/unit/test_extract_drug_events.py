import pytest
from tests.mocks.data_utilities import generate_mock_download_json
from openfda.main import extract_drug_events

def test_empty():
    mock_json = {}
    with pytest.raises(ValueError):
        extract_drug_events(mock_json)

def test_one_partition():
    # Belong to same partition due to same year
    TOTAL_RECORDS = 12000
    CONFIG = [
        {'display_name' : '2012 Q1 (part 1 of 3)', 'records': 6000,'size_mb' : '5.0'},
        {'display_name' : '2012 Q1 (part 2 of 3)', 'records': 4000,'size_mb' : '10.0'},
        {'display_name' : '2012 Q1 (part 3 of 3)', 'records': 2000,'size_mb' : '25.0'},
        ]
    
    mock_json = generate_mock_download_json(total_records=TOTAL_RECORDS, partition_config=CONFIG)
    result = extract_drug_events(mock_json)

    # Total records
    assert result['total_records'] == TOTAL_RECORDS

    # Number of partitions
    assert len(result['partitions']) == 1

    # No of files in partition
    assert len(result['partitions'][0]['files']) == 3

    # Size of partition
    assert result['partitions'][0]['size_mb'] == 40.0

    # Tally sum of records in each partition to total records
    assert sum(p['records'] for p in result['partitions']) == TOTAL_RECORDS

def test_multi_partition():
    TOTAL_RECORDS = 12000
    CONFIG = [
        {'display_name' : '2012 Q1 (part 1 of 2)', 'records': 2000, 'size_mb' : '5.0'},
        {'display_name' : '2012 Q1 (part 2 of 2)', 'records': 4000, 'size_mb' : '10.0'},
        {'display_name' : '2013 Q1 (part 1 of 1)', 'records': 6000, 'size_mb' : '25.0'},
        ]
    
    mock_json = generate_mock_download_json(total_records=TOTAL_RECORDS, partition_config=CONFIG)
    result = extract_drug_events(mock_json)

    # Total records
    assert result['total_records'] == TOTAL_RECORDS

    # Number of partitions
    assert len(result['partitions']) == 2

    # Partition 1
    assert len(result['partitions'][0]['files']) == 2 # Number of files in partition
    assert result['partitions'][0]['size_mb'] == 15.0 # Size of partition

    # Partition 2
    assert len(result['partitions'][1]['files']) == 1 # Number of files in partition
    assert result['partitions'][1]['size_mb'] == 25.0 # Size of partition

    # Tally sum of records in each partition to total records
    assert sum(p['records'] for p in result['partitions']) == TOTAL_RECORDS