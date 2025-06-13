from typing import TypedDict, List

class Partition(TypedDict):
    partition_id: str
    records: int
    count: int
    size_mb: float
    files: List[str]

class DrugEventsOutput(TypedDict):
    total_records: int
    partitions: List[Partition]