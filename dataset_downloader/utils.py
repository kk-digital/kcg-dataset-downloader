"""Utility functions for the dataset downloader."""
import os
import time
from functools import wraps


def retry(max_retries=3, delay=1):
    """Retry decorator for functions.
    
    Args:
        max_retries: Maximum number of retries
        delay: Delay between retries in seconds
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:  # pylint: disable=broad-except
                    retries += 1
                    if retries > max_retries:
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator


def get_file_size(file_path):
    """Get the size of a file in bytes.
    
    Args:
        file_path: Path to the file
        
    Returns:
        File size in bytes
    """
    return os.path.getsize(file_path)


def get_file_extension(url):
    """Get the file extension from a URL.
    
    Args:
        url: URL string
        
    Returns:
        File extension with dot
    """
    path = url.split("?")[0]  # Remove query parameters
    ext = os.path.splitext(path)[1].lower()
    
    # Default to .jpg if no extension or unknown extension
    if not ext or ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
        ext = '.jpg'
    
    return ext
