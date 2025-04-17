import logging
from datetime import datetime
import os

# def project_root():
#     current_dir = os.getcwd()
#     while current_dir != os.path.dirname(current_dir):
#         # Check for marker in current_dir
#         if '.git' in os.listdir(current_dir) or '.gitignore' in os.listdir(current_dir):
#             return current_dir
        
#         # /home/folder -> /home
#         current_dir = os.path.dirname(current_dir) # Fetch directory component of path

def get_logger(name=__name__):
    log_file = f"{datetime.now().strftime("%Y-%m-%d_%H%M%S")}.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('[%(asctime)s] %(name)s:%(lineno)d - %(levelname)s - %(message)s')

    # Logs into file
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # Logs into console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Prevent adding handlers multiple times if already exists
    if not logger.hasHandlers():
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
