import os
import json
import logging
import sys
from google.transit import gtfs_realtime_pb2

dataset_dir = '../local/realtime-data'

output_dir = '../local/gtfsr-json'
os.makedirs(output_dir, exist_ok=True)

log_dir = '../local/logs'
os.makedirs(log_dir, exist_ok=True)

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a handler for logging to file
file_handler = logging.FileHandler(f'{log_dir}/gtfsr2json.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Create a handler for logging to console
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

files = [f for f in os.listdir(dataset_dir)]


def parse_helper(entity):
    if 'ListFields' not in dir(entity):
        return entity
    entity_dict = {}
    for field in entity.ListFields():
        field_name = field[0].name
        if field[0].label == field[0].LABEL_REPEATED:
            field_value = [parse_helper(item) for item in field[1]]
        else:
            field_value = parse_helper(field[1])
        entity_dict[field_name] = field_value
    return entity_dict

def parse_gtfs_realtime_feed(feed):
    return [parse_helper(entity) for entity in feed.entity]

# Sort files by date and time
files = sorted(files, key=lambda x: (x.split('_')[1], x.split('_')[2]))

for f in files:
    # Skip files that have already been parsed
    if os.path.exists(f'{output_dir}/{f.split(".")[0]}.json'):
        logger.info(f'Skipping file: {f}')
        continue
    with open(f'{dataset_dir}/{f}', 'rb') as file:
        try:
            file_name = f.split('.')[0]
            logger.info(f'Parsing file: {f}')
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(file.read())
            data = parse_gtfs_realtime_feed(feed)
            logger.info(f'Parse complete: {f}')
            with open(f'{output_dir}/{file_name}.json', 'w') as out_file:
                json.dump(data, out_file)
                logger.info(f'Write complete: {file_name}.json')
        except:
            logger.error(f'Error parsing file: {f}')
            continue