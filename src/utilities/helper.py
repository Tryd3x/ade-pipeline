import json
from utilities.logger_config import get_module_logger

logger = get_module_logger(__name__)

def partition_id_by_year(p):
    """Extract partition id as YYYY"""
    return p.get('display_name').split(" ")[0]

def part_size_mb(p):
    """Obtain partition size (mb)"""
    return float(p.get('size_mb'))

def read_json_file(json_path):
    with open(json_path, "r") as f:
        d = json.load(f)
        return d