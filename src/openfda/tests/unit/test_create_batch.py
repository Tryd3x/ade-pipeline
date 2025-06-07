from main import create_batch
from tests.mocks.data_utilities import create_drug_events_json

def test_under_threshold():
    TOTAL_RECORDS = 10
    MAX_BATCH_SIZE_MB = 10000
    CONFIG = [
        {'partition_id': '2015', 'records' : 4,'count': 1, 'size_mb': 4000.0},
        {'partition_id': '2016', 'records' : 4,'count': 1, 'size_mb': 3000.0},
        {'partition_id': '2017', 'records' : 2,'count': 1, 'size_mb': 2000.0},
    ]

    partitions = create_drug_events_json(
        total_records= TOTAL_RECORDS,
        partition_config= CONFIG
    )['partitions']

    batches, oversized = create_batch(partitions, max_batch_size_mb=MAX_BATCH_SIZE_MB)

    # Assert
    assert len(batches) == 1  # All three partitions can fit in a single batch
    assert len(oversized) == 0 # No oversized partitions
    assert sum(p['size_mb'] for p in batches[0]) <= MAX_BATCH_SIZE_MB # Batch size under threshold
    assert sum(p['records'] for p in batches[0]) == TOTAL_RECORDS # Tally records count

def test_with_oversized_partition():
    TOTAL_RECORDS = 20
    MAX_BATCH_SIZE_MB = 10000
    CONFIG = [
        {'partition_id': '2020', 'records' : 10,'count': 1, 'size_mb': 12000.0},  # Oversized
        {'partition_id': '2021', 'records' : 5,'count': 1, 'size_mb': 4000.0},
        {'partition_id': '2022', 'records' : 5,'count': 1, 'size_mb': 3000.0},
    ]

    partitions = create_drug_events_json(
        total_records=TOTAL_RECORDS,
        partition_config=CONFIG
    )['partitions']

    batches, oversized = create_batch(partitions, max_batch_size_mb=MAX_BATCH_SIZE_MB)

    assert len(batches) == 1 # Number of batches generated
    assert sum(p['size_mb'] for p in batches[0]) <= MAX_BATCH_SIZE_MB
    assert len(batches[0]) == 2  # Two partitions under threshold in batch
    assert len(oversized) == 1 # One oversized partition

    assert sum(p['records'] for p in batches[0]) + sum(op['records'] for op in oversized) == TOTAL_RECORDS

def test_multiple_batches():
    TOTAL_RECORDS = 6
    MAX_BATCH_SIZE_MB = 9000 
    CONFIG = [
        {'partition_id': '2018', 'records': 2,'count': 1, 'size_mb': 5000.0},
        {'partition_id': '2019', 'records': 2,'count': 1, 'size_mb': 4000.0},
        {'partition_id': '2020', 'records': 2,'count': 1, 'size_mb': 2000.0},
    ]
    partitions = create_drug_events_json(
        total_records=TOTAL_RECORDS,
        partition_config=CONFIG
    )['partitions']

    batches, oversized = create_batch(partitions, max_batch_size_mb=MAX_BATCH_SIZE_MB)

    assert len(batches) == 2  # Should split into 2 batches
    assert all((sum(p['size_mb'] for p in batch) <= 9000) for batch in batches)
    assert sum([p['records'] for batch in batches for p in batch]) == TOTAL_RECORDS
    assert len(oversized) == 0

def test_only_oversized():
    TOTAL_RECORDS = 6
    MAX_BATCH_SIZE_MB = 1000 
    CONFIG = [
        {'partition_id': '2018', 'records': 2,'count': 1, 'size_mb': 5000.0},
        {'partition_id': '2019', 'records': 2,'count': 1, 'size_mb': 4000.0},
        {'partition_id': '2020', 'records': 2,'count': 1, 'size_mb': 2000.0},
    ]
    partitions = create_drug_events_json(
        total_records=TOTAL_RECORDS,
        partition_config=CONFIG
    )['partitions']

    batches, oversized = create_batch(partitions, max_batch_size_mb=MAX_BATCH_SIZE_MB)
    
    assert len(batches) == 0
    assert len(oversized) == 3
    assert all(p.get('size_mb') >=1000 for p in oversized) # Each partition exceeds the threshold
    assert sum(p['records'] for p in oversized) == TOTAL_RECORDS
