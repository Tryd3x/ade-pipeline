import pytest
from scripts.ingest import create_batch, extract_drug_events
from tests.mocks.data_utilities import create_drug_events_json

def test_under_threshold():
    partitions = create_drug_events_json(
        total_records=10,
        partition_config=[
            {'partition_id': '2015', 'count': 1, 'size_mb': 4000.0},
            {'partition_id': '2016', 'count': 1, 'size_mb': 3000.0},
            {'partition_id': '2017', 'count': 1, 'size_mb': 2000.0},
        ]
    )['partitions']

    batches, oversized = create_batch(partitions, max_batch_size_mb=10000)

    # Assert
    assert len(batches) == 1  # All three partitions can fit in a single batch
    assert len(oversized) == 0 # No oversized partitions
    assert sum(p['size_mb'] for p in batches[0]) <= 10000

def test_with_oversized_partition():
    partitions = create_drug_events_json(
        total_records=5,
        partition_config=[
            {'partition_id': '2020', 'count': 1, 'size_mb': 12000.0},  # Oversized
            {'partition_id': '2021', 'count': 1, 'size_mb': 4000.0},
            {'partition_id': '2022', 'count': 1, 'size_mb': 3000.0},
        ]
    )['partitions']

    batches, oversized = create_batch(partitions, max_batch_size_mb=10000)

    assert len(batches) == 1 # Number of batches generated
    assert len(batches[0]) == 2  # Two partitions under threshold in batch
    assert len(oversized) == 1 # One oversized partition

def test_multiple_batches():
    partitions = create_drug_events_json(
        total_records=6,
        partition_config=[
            {'partition_id': '2018', 'count': 1, 'size_mb': 5000.0},
            {'partition_id': '2019', 'count': 1, 'size_mb': 4000.0},
            {'partition_id': '2020', 'count': 1, 'size_mb': 2000.0},
        ]
    )['partitions']

    batches, oversized = create_batch(partitions, max_batch_size_mb=9000)

    assert len(batches) == 2  # Should split into 2 batches
    assert all(sum(p['size_mb'] for p in batch) <= 9000 for batch in batches)
    assert len(oversized) == 0

def test_only_oversized():
    partitions = create_drug_events_json(
        total_records=6,
        partition_config=[
            {'partition_id': '2018', 'count': 1, 'size_mb': 5000.0},
            {'partition_id': '2019', 'count': 1, 'size_mb': 4000.0},
            {'partition_id': '2020', 'count': 1, 'size_mb': 2000.0},
        ]
    )['partitions']

    batches, oversized = create_batch(partitions, max_batch_size_mb=1000)
    assert len(batches) == 0
    assert len(oversized) == 3
    assert all(p.get('size_mb') >=1000 for p in oversized) # Each partition exceeds the threshold
