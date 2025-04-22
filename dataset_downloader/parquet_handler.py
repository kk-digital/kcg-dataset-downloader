"""Parquet file handling module."""
import os
import glob
import pandas as pd


class ParquetHandler:
    """Handler for parquet files."""
    
    def __init__(self, input_dir, logger):
        """Initialize the parquet handler.
        
        Args:
            input_dir: Directory containing parquet files
            logger: Logger instance
        """
        self.input_dir = input_dir
        self.logger = logger
    
    def list_parquet_files(self):
        """List all parquet files in the input directory.
        
        Returns:
            List of parquet file paths
        """
        parquet_files = glob.glob(os.path.join(self.input_dir, "*.parquet"))
        # Filter out files that are already being processed or processed
        parquet_files = [f for f in parquet_files 
                         if not (f.endswith("_processing.parquet") or 
                                 f.endswith("_processed.parquet"))]
        
        self.logger.info(f"Found {len(parquet_files)} parquet files to process")
        return parquet_files
    
    def mark_as_processing(self, parquet_file):
        """Mark a parquet file as being processed.
        
        Args:
            parquet_file: Path to the parquet file
            
        Returns:
            Path to the renamed parquet file
        """
        processing_file = parquet_file.replace(".parquet", "_processing.parquet")
        os.rename(parquet_file, processing_file)
        self.logger.info(f"Marked {parquet_file} as processing")
        return processing_file
    
    def mark_as_processed(self, processing_file):
        """Mark a parquet file as processed.
        
        Args:
            processing_file: Path to the processing parquet file
            
        Returns:
            Path to the renamed parquet file
        """
        processed_file = processing_file.replace("_processing.parquet", "_processed.parquet")
        os.rename(processing_file, processed_file)
        self.logger.info(f"Marked {processing_file} as processed")
        return processed_file
    
    def read_parquet(self, parquet_file):
        """Read a parquet file.
        
        Args:
            parquet_file: Path to the parquet file
            
        Returns:
            Pandas DataFrame
        """
        self.logger.info(f"Reading parquet file: {parquet_file}")
        df = pd.read_parquet(parquet_file)
        
        # Initialize status and error columns if they don't exist
        if 'status' not in df.columns:
            df['status'] = None
        if 'error' not in df.columns:
            df['error'] = None
            
        return df
    
    def write_parquet(self, df, parquet_file):
        """Write a DataFrame to a parquet file.
        
        Args:
            df: Pandas DataFrame
            parquet_file: Path to the parquet file
        """
        self.logger.info(f"Writing parquet file: {parquet_file}")
        df.to_parquet(parquet_file, index=False)
    
    def find_incomplete_rows(self, df):
        """Find rows that haven't been processed yet.
        
        Args:
            df: Pandas DataFrame
            
        Returns:
            Pandas DataFrame with unprocessed rows
        """
        return df[df['status'].isna()]
