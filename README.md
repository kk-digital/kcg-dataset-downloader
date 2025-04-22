# KCG Dataset Downloader

A scalable, fault-tolerant tool for downloading images from URLs stored in parquet files.

## Features

- Processes parquet files containing image URLs
- Downloads images with retry logic
- Updates status and error information in the parquet files
- Organizes images into batch folders of configurable size
- Supports resuming downloads after interruption
- Parallel downloading with thread pool executor

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python main.py --input-dir /path/to/parquet/files --output-dir /path/to/save/images
```

### Command Line Arguments

- `--input-dir`: Input directory containing parquet files (required)
- `--output-dir`: Output directory for downloaded images (required)
- `--url-column`: Column name containing the URL (default: "url")
- `--max-workers`: Maximum number of concurrent downloads (default: 16)
- `--timeout`: Timeout for each download request in seconds (default: 30)
- `--retries`: Number of retries for each download (default: 3)
- `--batch-size-gb`: Maximum size of each batch folder in GB (default: 10)
- `--resume`: Resume downloading from processing files

## Workflow

1. The tool lists all parquet files in the input directory
2. For each parquet file:
   - Rename it to `*_processing.parquet`
   - Read the file and find rows that haven't been processed
   - Download images in parallel
   - Save images to batch folders (e.g., 000001, 000002)
   - Update status and error information in the parquet file
   - Rename the file to `*_processed.parquet` when done
3. If the tool crashes, it can resume from where it left off

## Batch Organization

Images are saved in batch folders, with each folder limited to a configurable size (default: 10 GB).
Folder names follow the pattern `000001`, `000002`, etc.
