"""Storage module for saving downloaded images."""
import os
import shutil
from pathlib import Path


class Storage:
    """Storage manager for downloaded images."""
    
    def __init__(self, output_dir, batch_size_gb, logger):
        """Initialize the storage manager.
        
        Args:
            output_dir: Directory to save images
            batch_size_gb: Maximum size of each batch folder in GB
            logger: Logger instance
        """
        self.output_dir = output_dir
        self.batch_size_gb = batch_size_gb
        self.logger = logger
        self.current_batch = self._get_next_batch_number()
        self.current_batch_size = 0
        self._ensure_batch_dir_exists()
    
    def _get_next_batch_number(self):
        """Get the next batch number.
        
        Returns:
            Next batch number
        """
        existing_batches = [
            int(d) for d in os.listdir(self.output_dir)
            if os.path.isdir(os.path.join(self.output_dir, d)) and d.isdigit()
        ]
        
        if not existing_batches:
            return 1
        
        return max(existing_batches) + 1
    
    def _ensure_batch_dir_exists(self):
        """Ensure the current batch directory exists."""
        batch_dir = self._get_batch_dir()
        os.makedirs(batch_dir, exist_ok=True)
    
    def _get_batch_dir(self):
        """Get the current batch directory.
        
        Returns:
            Path to the current batch directory
        """
        return os.path.join(self.output_dir, f"{self.current_batch:06d}")
    
    def _check_batch_size(self):
        """Check if the current batch has reached the size limit.
        
        Returns:
            True if a new batch should be created, False otherwise
        """
        batch_dir = self._get_batch_dir()
        total_size = sum(
            os.path.getsize(os.path.join(batch_dir, f))
            for f in os.listdir(batch_dir)
            if os.path.isfile(os.path.join(batch_dir, f))
        )
        
        # Convert to GB
        total_size_gb = total_size / (1024 ** 3)
        
        return total_size_gb >= self.batch_size_gb
    
    def save_image(self, key, img_stream, extension=".jpg"):
        """Save an image to the current batch directory.
        
        Args:
            key: Image key/identifier
            img_stream: BytesIO stream containing the image
            extension: File extension
            
        Returns:
            Path to the saved image
        """
        if self._check_batch_size():
            self.current_batch += 1
            self._ensure_batch_dir_exists()
            self.logger.info(f"Created new batch directory: {self._get_batch_dir()}")
        
        # Create a safe filename from the key
        safe_key = str(key).replace("/", "_").replace("\\", "_")
        filename = f"{safe_key}{extension}"
        filepath = os.path.join(self._get_batch_dir(), filename)
        
        # Save the image
        with open(filepath, "wb") as f:
            f.write(img_stream.getvalue())
        
        img_stream.close()
        
        return filepath
