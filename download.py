import components as comp
from datetime import datetime
import os

if __name__ == '__main__':
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    current_dir = os.getcwd()
    output_dir = os.path.join(current_dir, 'downloads', current_time)
    print(f"Current time: {current_time}")
    print(f"Current directory: {current_dir}")
    print(f"Output directory: {output_dir}")
    comp.download_gtfs_zip("http://data.ptv.vic.gov.au/downloads/gtfs.zip", output_dir)