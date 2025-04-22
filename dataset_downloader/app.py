"""Main application module for KCG Dataset Downloader."""
import os
import time
from tqdm import tqdm

from .config import Config
from .downloader import Downloader
from .parquet_handler import ParquetHandler
from .storage import Storage
from .logger import Logger
from .utils import get_file_extension


class DatasetDownloader:
    """Main application class for downloading datasets."""
    
    def __init__(self, config):
        """Initialize the dataset downloader.
        
        Args:
            config: Configuration object or path to config file
        """
        if isinstance(config, str):
            # Assume it's a path to a config file
            # In a real implementation, you'd load from file
            self.config = Config(
                input_dir=os.path.join(config, "input"),
                output_dir=os.path.join(config, "output"),
            )
        else:
            self.config = config
            
        self.logger = Logger(os.path.join(self.config.output_dir, self.config.log_file))
        self.parquet_handler = ParquetHandler(self.config.input_dir, self.logger)
        self.downloader = Downloader(
            timeout=self.config.timeout,
            retries=self.config.retries,
            max_workers=self.config.max_workers,
            logger=self.logger
        )
        self.storage = Storage(
            output_dir=self.config.output_dir,
            batch_size_gb=self.config.batch_size_gb,
            logger=self.logger
        )
    
    def process_parquet_file(self, parquet_file, url_column='url'):
        """Process a single parquet file.
        
        Args:
            parquet_file: Path to the parquet file
            url_column: Column name containing the URL
            
        Returns:
            Number of successfully downloaded images
        """
        # Mark as processing
        processing_file = self.parquet_handler.mark_as_processing(parquet_file)
        
        # Read the parquet file
        df = self.parquet_handler.read_parquet(processing_file)
        
        # Find rows that haven't been processed yet
        incomplete_df = self.parquet_handler.find_incomplete_rows(df)
        
        if incomplete_df.empty:
            self.logger.info(f"No incomplete rows in {processing_file}")
            self.parquet_handler.mark_as_processed(processing_file)
            return 0
        
        self.logger.info(f"Processing {len(incomplete_df)} incomplete rows in {processing_file}")
        
        # Download images
        results = self.downloader.download_batch(incomplete_df, url_column)
        
        # Update the DataFrame with results
        success_count = 0
        for idx, (img_stream, err) in results.items():
            if img_stream is not None:
                try:
                    # Get file extension from URL
                    extension = get_file_extension(df.loc[idx, url_column])
                    
                    # Save the image
                    filepath = self.storage.save_image(idx, img_stream, extension)
                    
                    # Update status
                    df.loc[idx, 'status'] = 'success'
                    df.loc[idx, 'error'] = None
                    success_count += 1
                except Exception as e:  # pylint: disable=broad-except
                    self.logger.error(f"Failed to save image for row {idx}: {e}")
                    df.loc[idx, 'status'] = 'failed'
                    df.loc[idx, 'error'] = str(e)
            else:
                df.loc[idx, 'status'] = 'failed'
                df.loc[idx, 'error'] = err
        
        # Write the updated DataFrame back to the parquet file
        self.parquet_handler.write_parquet(df, processing_file)
        
        # Mark as processed
        self.parquet_handler.mark_as_processed(processing_file)
        
        return success_count
    
    def run(self, url_column='url'):
        """Run the dataset downloader.
        
        Args:
            url_column: Column name containing the URL
            
        Returns:
            Total number of successfully downloaded images
        """
        self.logger.info("Starting dataset downloader")
        
        # List parquet files
        parquet_files = self.parquet_handler.list_parquet_files()
        
        if not parquet_files:
            self.logger.info("No parquet files found")
            return 0
        
        # Process each parquet file
        total_success = 0
        for parquet_file in tqdm(parquet_files, desc="Processing parquet files"):
            try:
                success_count = self.process_parquet_file(parquet_file, url_column)
                total_success += success_count
                self.logger.info(f"Successfully downloaded {success_count} images from {parquet_file}")
            except Exception as e:  # pylint: disable=broad-except
                self.logger.error(f"Failed to process {parquet_file}: {e}")
        
        self.logger.info(f"Finished dataset downloader. Successfully downloaded {total_success} images")
        return total_success
    
    def resume(self, url_column='url'):
        """Resume downloading from processing files.
        
        Args:
            url_column: Column name containing the URL
            
        Returns:
            Total number of successfully downloaded images
        """
        self.logger.info("Resuming dataset downloader")
        
        # Find processing files
        processing_files = [
            f for f in os.listdir(self.config.input_dir)
            if f.endswith("_processing.parquet")
        ]
        
        if not processing_files:
            self.logger.info("No processing files found, starting from scratch")
            return self.run(url_column)
        
        # Process each processing file
        total_success = 0
        for processing_file in tqdm(processing_files, desc="Resuming processing files"):
            try:
                full_path = os.path.join(self.config.input_dir, processing_file)
                df = self.parquet_handler.read_parquet(full_path)
                
                # Find rows that haven't been processed yet
                incomplete_df = self.parquet_handler.find_incomplete_rows(df)
                
                if incomplete_df.empty:
                    self.logger.info(f"No incomplete rows in {processing_file}")
                    self.parquet_handler.mark_as_processed(full_path)
                    continue
                
                self.logger.info(f"Processing {len(incomplete_df)} incomplete rows in {processing_file}")
                
                # Download images
                results = self.downloader.download_batch(incomplete_df, url_column)
                
                # Update the DataFrame with results
                success_count = 0
                for idx, (img_stream, err) in results.items():
                    if img_stream is not None:
                        try:
                            # Get file extension from URL
                            extension = get_file_extension(df.loc[idx, url_column])
                            
                            # Save the image
                            filepath = self.storage.save_image(idx, img_stream, extension)
                            
                            # Update status
                            df.loc[idx, 'status'] = 'success'
                            df.loc[idx, 'error'] = None
                            df.loc[idx, 'filepath'] = filepath
                            success_count += 1
                        except Exception as e:  # pylint: disable=broad-except
                            self.logger.error(f"Failed to save image for row {idx}: {e}")
                            df.loc[idx, 'status'] = 'failed'
                            df.loc[idx, 'error'] = str(e)
                    else:
                        df.loc[idx, 'status'] = 'failed'
                        df.loc[idx, 'error'] = err
                
                # Write the updated DataFrame back to the parquet file
                self.parquet_handler.write_parquet(df, full_path)
                
                # Mark as processed
                self.parquet_handler.mark_as_processed(full_path)
                
                total_success += success_count
                self.logger.info(f"Successfully downloaded {success_count} images from {processing_file}")
            except Exception as e:  # pylint: disable=broad-except
                self.logger.error(f"Failed to process {processing_file}: {e}")
        
        # Continue with remaining files
        return total_success + self.run(url_column)
