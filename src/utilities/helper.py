import json
import os
from utilities.logger_config import get_module_logger

logger = get_module_logger(__name__)


def project_root():
    """Return project root path"""
    current_dir = os.getcwd()
    while current_dir != os.path.dirname(current_dir):
        # Check for marker in current_dir
        if '.git' in os.listdir(current_dir) or '.gitignore' in os.listdir(current_dir):
            return current_dir
        
        # /home/folder -> /home
        current_dir = os.path.dirname(current_dir) # Fetch directory component of path

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