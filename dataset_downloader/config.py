"""Configuration module for the dataset downloader."""
import os
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration for the dataset downloader."""
    input_dir: str
    output_dir: str
    max_workers: int = 16
    timeout: int = 30
    retries: int = 3
    batch_size_gb: int = 10
    log_file: str = "download.log"
    
    def __post_init__(self):
        """Validate and create directories if needed."""
        if not os.path.exists(self.input_dir):
            raise ValueError(f"Input directory {self.input_dir} does not exist")
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
