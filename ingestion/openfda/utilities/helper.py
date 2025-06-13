import json
from typing import *
from openfda.utilities.custom_types import Partition
from openfda.utilities.logger_config import get_module_logger

logger = get_module_logger(__name__)

def partition_id_by_year(p: Dict[str, str]) -> str:
    """
    Extract partition_id as YYYY
    """
    return p.get('display_name','').strip().split(" ")[0]

def part_size_mb(p: Dict[str, Any]) -> float:
    """Obtain partition size (mb)"""
    result = p.get('size_mb',-1)
    if result == -1:
        return result
    return float(result)

def read_json_file(json_path: str) -> Dict[str, Any]:
    with open(json_path, "r") as f:
        d = json.load(f)
        return d
    
def filter_partition(years: str= '', partitions: List= []) -> List[Partition]:
    yrs = [y.strip() for y in years.split(",")]
    if not yrs:
        print("No args provided")
        return []
    return [p for p in partitions if p.get('partition_id') in yrs]