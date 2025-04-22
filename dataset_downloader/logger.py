"""Logging module for the dataset downloader."""
import logging
import os
from datetime import datetime


class Logger:
    """Logger for the dataset downloader."""
    
    def __init__(self, log_file):
        """Initialize the logger.
        
        Args:
            log_file: Path to the log file
        """
        self.logger = logging.getLogger("dataset_downloader")
        self.logger.setLevel(logging.INFO)
        
        # Create handlers
        file_handler = logging.FileHandler(log_file)
        console_handler = logging.StreamHandler()
        
        # Create formatters
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def info(self, message):
        """Log an info message."""
        self.logger.info(message)
    
    def error(self, message):
        """Log an error message."""
        self.logger.error(message)
    
    def warning(self, message):
        """Log a warning message."""
        self.logger.warning(message)
    
    def debug(self, message):
        """Log a debug message."""
        self.logger.debug(message)
