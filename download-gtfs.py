import pyptvgtfs
from datetime import datetime
import os
import logging
import sys

# Download the GTFS file into a local folder

if __name__ == '__main__':
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create a handler for logging to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    current_dir = os.getcwd()
    output_dir = os.path.join(current_dir, 'downloads', current_time)
    logger.info(f"Current directory: {current_dir}")
    logger.info(f"Output directory: {output_dir}")
    pyptvgtfs.download_gtfs_zip("http://data.ptv.vic.gov.au/downloads/gtfs.zip", output_dir)
    logger.info("Downloaded GTFS file")