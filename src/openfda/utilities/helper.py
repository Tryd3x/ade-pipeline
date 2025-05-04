import json
from utilities.logger_config import get_module_logger

logger = get_module_logger(__name__)

def partition_id_by_year(p):
    """Extract partition id as YYYY"""
    return p.get('display_name','').strip().split(" ")[0]

def part_size_mb(p):
    """Obtain partition size (mb)"""
    result = p.get('size_mb',-1)
    if result == -1:
        return result
    return float(result)

def read_json_file(json_path):
    with open(json_path, "r") as f:
        d = json.load(f)
        return d
    
def filter_partition(years='', partitions=[]):
    years = [y.strip() for y in years.split(",")]
    if not years:
        print("No args provided")
        return
    return [p for p in partitions if p.get('partition_id') in years]