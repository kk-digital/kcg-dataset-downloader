#!/usr/bin/env python
"""Main script for KCG Dataset Downloader."""
import argparse
import os
import sys

from dataset_downloader.config import Config
from dataset_downloader.app import DatasetDownloader


def parse_args():
    """Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description="KCG Dataset Downloader")
    parser.add_argument("--input-dir", required=True, help="Input directory containing parquet files")
    parser.add_argument("--output-dir", required=True, help="Output directory for downloaded images")
    parser.add_argument("--url-column", default="url", help="Column name containing the URL")
    parser.add_argument("--max-workers", type=int, default=16, help="Maximum number of concurrent downloads")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout for each download request")
    parser.add_argument("--retries", type=int, default=3, help="Number of retries for each download")
    parser.add_argument("--batch-size-gb", type=int, default=10, help="Maximum size of each batch folder in GB")
    parser.add_argument("--resume", action="store_true", help="Resume downloading from processing files")
    
    return parser.parse_args()


def main():
    """Main function."""
    args = parse_args()
    
    # Create config
    config = Config(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        max_workers=args.max_workers,
        timeout=args.timeout,
        retries=args.retries,
        batch_size_gb=args.batch_size_gb,
    )
    
    # Create downloader
    downloader = DatasetDownloader(config)
    
    # Run or resume
    if args.resume:
        downloader.resume(args.url_column)
    else:
        downloader.run(args.url_column)


if __name__ == "__main__":
    main()
