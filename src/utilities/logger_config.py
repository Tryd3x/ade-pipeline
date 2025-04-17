import logging
from datetime import datetime

def configure_logging():
    # Configure the root logger
    root_logger = logging.getLogger()
    
    # Check if handlers already exist to avoid duplicates
    if root_logger.handlers:
        return
        
    # Create a stream handler for console output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create a time-based log file name
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"ingest_{timestamp}.log"

    # Create a file handler for file output
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    
    # Create formatter and add to the handlers
    formatter = logging.Formatter('[%(asctime)s] %(filename)s:%(lineno)d - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add the handlers to the root logger
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    root_logger.setLevel(logging.INFO)

def get_module_logger(module_name):
    # Make sure logging is configured
    configure_logging()
    
    # Return a logger with the module name
    return logging.getLogger(module_name)
