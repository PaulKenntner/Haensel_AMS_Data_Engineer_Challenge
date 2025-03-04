# attribution-pipeline/config.py
import os
import logging
import sys

# API Configuration
IHC_API_KEY = os.environ.get('IHC_API_KEY', '')
IHC_CONV_TYPE_ID = os.environ.get('IHC_CONV_TYPE_ID', '')

# Database Configuration
DB_PATH = os.environ.get('DB_PATH', 'challenge.db')

# Reporting Configuration
REPORT_OUTPUT_PATH = os.environ.get('REPORT_OUTPUT_PATH', 'channel_reporting.csv')

# API Rate Limiting
API_MAX_RETRIES = int(os.environ.get('API_MAX_RETRIES', '3'))
API_RETRY_DELAY = int(os.environ.get('API_RETRY_DELAY', '2'))

# Logging Configuration
LOG_FILE = os.environ.get('LOG_FILE', 'attribution_pipeline.log')
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

# Configure logging
def setup_logging():
    """Set up logging configuration for the entire application."""
    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create file handler
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, LOG_LEVEL))
    
    # Remove any existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger

# Initialize logging
logger = setup_logging()