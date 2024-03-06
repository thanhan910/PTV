# !pip install --upgrade gtfs-realtime-bindings
from google.transit import gtfs_realtime_pb2
import requests
import datetime as dt
import time
import os
import logging

if __name__ == '__main__':

    output_dir = '../local/realtime-data'
    os.makedirs(output_dir, exist_ok=True)

    log_dir = '../local/logs'
    os.makedirs(log_dir, exist_ok=True)

    # Set up logging
    logging.basicConfig(filename=f'{log_dir}/realtime-server.log', level=logging.INFO)

    # Log to console as well
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


    env_dirs = ['..', '.', '../notebooks', '../scripts']

    # Construct the full path to the .env file
    env_file_paths = [os.path.join(env_dir, '.env') for env_dir in env_dirs]
    env_file_paths = [path for path in env_file_paths if os.path.exists(path)]

    # Load environment variables from .env file
    for env_file_path in env_file_paths:
        if os.path.exists(env_file_path):
            with open(env_file_path, 'r') as f:
                for line in f:
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value

    URLS = {
        'tram-servicealert': 'https://data-exchange-api.vicroads.vic.gov.au/opendata/gtfsr/v1/tram/servicealert',
        'tram-tripupdates': 'https://data-exchange-api.vicroads.vic.gov.au/opendata/gtfsr/v1/tram/tripupdates',
        'tram-vehicleposition': 'https://data-exchange-api.vicroads.vic.gov.au/opendata/gtfsr/v1/tram/vehicleposition',
        'bus-tripupdates': 'https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrobus-tripupdates',
        'train-servicealerts': 'https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrotrain-servicealerts',
        'train-tripupdates': 'https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrotrain-tripupdates',
        'train-vehicleposition-updates': 'https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrotrain-vehicleposition-updates',
    }
    header = {
        # Request headers
        'Cache-Control': 'no-cache',
        'Ocp-Apim-Subscription-Key': os.environ['PRIMARY_KEY'],
    }

    header2 = {
        # Request headers
        'Cache-Control': 'no-cache',
        'Ocp-Apim-Subscription-Key': os.environ['SECONDARY_KEY'],
    }

    session = requests.Session()

    while True:
        for url_name, url in URLS.items():
            try:
                response = session.get(url, headers=header)
            except:
                try:
                    # print(f'Failed to get {url_name} at {dt.datetime.now()} using primary key')
                    logging.error(f'[ERROR] Failed to get {url_name} at {dt.datetime.now()} using primary key')
                    response = session.get(url, headers=header2)
                except:
                    # print(f'Failed to get {url_name} at {dt.datetime.now()} using secondary key')
                    # print('Waiting for 10 seconds')
                    logging.error(f'[ERROR] Failed to get {url_name} at {dt.datetime.now()} using secondary key')
                    logging.error('[ERROR] Waiting for 10 seconds')
                    time.sleep(10)
                    continue

            # Save the response to a file
            current_time = dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            with open(f'{output_dir}/{url_name}_{current_time}.bin', 'wb') as f:
                f.write(response.content)
            logging.info(f'[{url_name}] Saved {url_name} at {current_time}')
            # Log with tag using logging
                
        current_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # print(f'Updated all at {current_time}')
        logging.info(f'[COMPLETE] Updated all at {current_time}')
        logging.info('[WAIT] Waiting for 30 seconds')
        time.sleep(30)