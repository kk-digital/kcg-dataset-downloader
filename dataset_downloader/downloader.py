"""Image downloader module."""
import io
import urllib.request
import concurrent.futures
from tqdm import tqdm


class Downloader:
    """Image downloader with retry logic."""
    
    def __init__(self, timeout, retries, max_workers, logger):
        """Initialize the downloader.
        
        Args:
            timeout: Timeout for each download request
            retries: Number of retries for each download
            max_workers: Maximum number of concurrent downloads
            logger: Logger instance
        """
        self.timeout = timeout
        self.retries = retries
        self.max_workers = max_workers
        self.logger = logger
    
    def download_image(self, row, url_column='url'):
        """Download an image with urllib.
        
        Args:
            row: Pandas Series representing a row
            url_column: Column name containing the URL
            
        Returns:
            Tuple of (key, image_stream, error)
        """
        key, url = row.name, row[url_column]
        img_stream = None
        
        try:
            request = urllib.request.Request(
                url,
                data=None,
                headers={
                    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0) Gecko/20100101 Firefox/72.0"
                },
            )
            with urllib.request.urlopen(request, timeout=self.timeout) as r:
                img_stream = io.BytesIO(r.read())
            return key, img_stream, None
        except Exception as err:  # pylint: disable=broad-except
            if img_stream is not None:
                img_stream.close()
            return key, None, str(err)
    
    def download_image_with_retry(self, row, url_column='url'):
        """Download an image with retries.
        
        Args:
            row: Pandas Series representing a row
            url_column: Column name containing the URL
            
        Returns:
            Tuple of (key, image_stream, error)
        """
        for _ in range(self.retries + 1):
            key, img_stream, err = self.download_image(row, url_column)
            if img_stream is not None:
                return key, img_stream, err
        return key, None, err
    
    def download_batch(self, df, url_column='url'):
        """Download images in parallel.
        
        Args:
            df: Pandas DataFrame
            url_column: Column name containing the URL
            
        Returns:
            Dictionary mapping row indices to (image_stream, error) tuples
        """
        results = {}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_idx = {
                executor.submit(self.download_image_with_retry, row, url_column): idx 
                for idx, row in df.iterrows()
            }
            
            for future in tqdm(concurrent.futures.as_completed(future_to_idx), 
                              total=len(future_to_idx),
                              desc="Downloading images"):
                idx = future_to_idx[future]
                try:
                    key, img_stream, err = future.result()
                    results[idx] = (img_stream, err)
                except Exception as exc:  # pylint: disable=broad-except
                    self.logger.error(f"Download failed for row {idx}: {exc}")
                    results[idx] = (None, str(exc))
        
        return results
