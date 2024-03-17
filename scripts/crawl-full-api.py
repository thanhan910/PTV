from hashlib import sha1
import hmac
import requests
import pandas as pd
import geopandas as gpd
import json
import os
import numpy as np
import logging
import sys
import time
import pyptvgtfs
SESSION = requests.Session()

ENV = json.load(open('../local-env.json'))

DATA_DIR = '../local/ptv-api/data'
os.makedirs(DATA_DIR, exist_ok=True)


# Create a logger

def create_logger(log_filepath, level=logging.INFO, name = "root"):

    os.makedirs(os.path.dirname(log_filepath), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create a handler for logging to file
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Create a handler for logging to console
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def get_ptv_api_url(
        endpoint : str,
        dev_id : str | int, 
        api_key : str | int,
    ):
    """
    Returns the URL to use PTV TimeTable API.

    Generates a signature from dev id (user id), API key, and endpoint.

    See the following for more information:
    - Home page: https://www.ptv.vic.gov.au/footer/data-and-reporting/datasets/ptv-timetable-api/
    - Swagger UI: https://timetableapi.ptv.vic.gov.au/swagger/ui/index
    - Swagger Docs JSON: https://timetableapi.ptv.vic.gov.au/swagger/docs/v3 (You can use this to find the endpoints you want to use.)
    """
    assert endpoint.startswith('/'), f'Endpoint must start with /, got {endpoint}'
    raw = f'{endpoint}{'&' if '?' in endpoint else '?'}devid={dev_id}'
    hashed = hmac.new(api_key.encode('utf-8'), raw.encode('utf-8'), sha1)  # Encode the raw string to bytes
    signature = hashed.hexdigest()
    return f'https://timetableapi.ptv.vic.gov.au{raw}&signature={signature}'


def get_data(endpoint : str, need_auth : bool = True):
    """
    Returns the data from the URL.
    """
    if need_auth:
        url = get_ptv_api_url(endpoint, ENV['PTV_TIMETABLE_DEV_ID'], ENV['PTV_TIMETABLE_API_KEY'])
    else:
        url = f'https://timetableapi.ptv.vic.gov.au{endpoint}'
    response = SESSION.get(url)
    response.raise_for_status()
    return response.json()


logger = create_logger(f'{DATA_DIR}/full-crawl.log', name='ptv-api')


API_DOCS = get_data('/swagger/docs/v3', need_auth=False)

STATIC_API_ENDPOINTS = [k for k in API_DOCS['paths'].keys() if '{' not in k]

API_ROUTES : dict = get_data('/v3/routes')['routes']

API_ROUTE_TYPES : dict = get_data('/v3/route_types')['route_types']

API_DISRUPTIONS : dict = get_data('/v3/disruptions')['disruptions']

API_DISRUPTION_MODES : dict = get_data('/v3/disruptions/modes')['disruption_modes']

API_OUTLETS : dict = get_data('/v3/outlets')['outlets']

# Create dataframes from the data

API_DF_ROUTE_TYPES = pd.DataFrame(API_ROUTE_TYPES)

API_DF_DISRUPTION_MODES = pd.DataFrame(API_DISRUPTION_MODES)

API_DF_OUTLETS = pd.DataFrame(API_OUTLETS)

# There are some faulty data in the outlets data. In particular, the latitude is > 0, which is not possible in Victoria.
API_DF_OUTLETS['outlet_latitude'] = API_DF_OUTLETS['outlet_latitude'].apply(lambda x: -x if x > 0 else x)
for route in API_ROUTES:
    for k, v in route['route_service_status'].items():
        assert k not in route, f'Key {k} already exists in route'
        route[k] = v
    del route['route_service_status']


API_DF_ROUTES = pd.DataFrame(API_ROUTES, dtype=str)

# API_DF_ROUTES['route_id'] = API_DF_ROUTES['route_id'].apply(str)
# API_DF_ROUTES['route_type'] = API_DF_ROUTES['route_type'].apply(lambda x: str(int(x)) if not pd.isna(x) else x)

assert API_DF_ROUTES['route_id'].is_unique, 'route_id is not unique'

assert API_DF_ROUTES['route_gtfs_id'].is_unique, 'route_gtfs_id is not unique'

API_DF_ROUTES = API_DF_ROUTES[['route_type', 'route_id', 'route_name', 'route_number']]

api_all_route_ids = API_DF_ROUTES['route_id'].unique()

logger.info(f'Route ids count: {len(api_all_route_ids)}')

api_directions_endpoints = [f'/v3/directions/route/{route_id}' for route_id in api_all_route_ids]

logger.info(f'Direction ids count: {len(api_directions_endpoints)}')

API_DIRECTIONS_DATA = {}
for i, endpoint in enumerate(api_directions_endpoints):
    route_id = int(endpoint.split('/')[4])
    directions = None
    while directions is None:
        try:
            directions = get_data(endpoint)
            # print(f'[{i}] Got directions for route {route_id}')
            logger.info(f'[{i}] Got directions for route {route_id}')
        except requests.exceptions.HTTPError:
            # print(f'Failed to get directions for route {route_id}. Retrying in 30 seconds...')
            logger.warning(f'[{i}] Failed to get directions for route {route_id}. Retrying in 30 seconds...')
            time.sleep(30)
            continue
    API_DIRECTIONS_DATA[route_id] = directions
API_DIRECTIONS_DATA = { str(k): v['directions'] for k, v in API_DIRECTIONS_DATA.items() }
# 2m - 3m

assert all([str(direction['route_id']) == str(k) for k, v in API_DIRECTIONS_DATA.items() for direction in v])

API_DIRECTIONS_LIST = [direction for k, v in API_DIRECTIONS_DATA.items() for direction in v]

API_DF_DIRECTIONS = pd.DataFrame(API_DIRECTIONS_LIST)[['route_id', 'route_type', 'direction_id', 'direction_name']]

assert API_DF_DIRECTIONS[['route_id', 'route_type']].value_counts().max() <= 2, 'There are more than 2 directions for a route'

API_DF_DIRECTIONS.to_csv(f'{DATA_DIR}/all_directions.csv', index=False)

# API_DF_DIRECTIONS = pd.read_csv(f'{DATA_DIR}/all_directions.csv')

API_route_rtds = [(str(r), str(t), str(d)) for r, t, d in API_DF_DIRECTIONS[['route_id', 'route_type', 'direction_id']].values]

logger.info(f'Route id - Route type - Direction id count: {len(API_route_rtds)}')

API_STOPS_DATA = {}

for i, (route_id, route_type, direction_id) in enumerate(API_route_rtds):
    logger.info(f'[{i}] Getting stops for route {route_id}, route type {route_type}, direction {direction_id}')
    endpoint = f'/v3/stops/route/{route_id}/route_type/{route_type}?direction_id={direction_id}&include_geopath=true'
    stops = None
    while stops is None:
        try:
            stops = get_data(endpoint)
            # print(f'[{i}] Got stops for route {route_id}')
            logger.info(f'[{i}] Got stops for route {route_id}')
        except requests.exceptions.HTTPError:
            # print(f'Failed to get stops for route {route_id}. Retrying in 30 seconds...')
            logger.warning(f'[{i}] Failed to get stops for route {route_id}. Retrying in 30 seconds...')
            time.sleep(30)
            continue
    API_STOPS_DATA[route_id] = API_STOPS_DATA.get(route_id, {})
    API_STOPS_DATA[route_id][direction_id] = stops
# 3m - 5m
    
API_STOPS_DATA = { str(k): { str(k2): v2 for k2, v2 in v.items()} for k, v in API_STOPS_DATA.items() }
with open(f'{DATA_DIR}/route_direction_stops.json', 'w') as f:
    f.write(json.dumps(API_STOPS_DATA))

API_STOPS_STOPS = []
API_STOPS_GEOPATHS = []
for route_id, route_type, direction_id in API_route_rtds:
    stops = API_STOPS_DATA[route_id][direction_id]['stops']
    for stop in stops:
        if 'stop_ticket' not in stop:
            # print(f'Route {route_id} has no ticket key for stop {stop["stop_id"]}')
            continue
        if stop['stop_ticket'] is None:
            # print(f'Route {route_id}: stop {stop["stop_id"]}: stop ticket is None. Skipping...')
            continue
        for mid, v in stop['stop_ticket'].items():
            mid = f'stop_{mid}'
            assert mid not in stop, f'Key {mid} already exists in stop'
            stop[mid] = v
        if 'route_id' not in stop:
            stop['route_id'] = route_id
        if 'route_type' not in stop:
            stop['route_type'] = route_type
        if 'direction_id' not in stop:
            stop['direction_id'] = direction_id
        API_STOPS_STOPS.append(stop)
    geopath = API_STOPS_DATA[route_id][direction_id]['geopath']
    for path in geopath:
        if 'route_id' not in path:
            path['route_id'] = route_id
        if 'route_type' not in path:
            path['route_type'] = route_type
        if 'direction_id' not in path:
            path['direction_id'] = direction_id
        API_STOPS_GEOPATHS.append(path)
        
with open(f'{DATA_DIR}/stops.json', 'w') as f:
    f.write(json.dumps(API_STOPS_STOPS))
with open(f'{DATA_DIR}/stops_geopaths.json', 'w') as f:
    f.write(json.dumps(API_STOPS_GEOPATHS))



API_DF_STOPS = pd.DataFrame(API_STOPS_STOPS, dtype=str)
API_DF_STOPS.drop(columns=['disruption_ids'], inplace=True)
API_DF_STOPS['stop_ticket_zones'] = API_DF_STOPS['stop_ticket_zones'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
API_DF_STOPS.drop(columns=['stop_ticket'], inplace=True)
API_DF_STOPS['stop_is_regional'] = API_DF_STOPS['stop_zone'].apply(lambda x: 'Regional' in x)
API_DF_STOPS['stop_zones'] = API_DF_STOPS['stop_ticket_zones']
API_DF_STOPS.drop(columns=['stop_ticket_zones', 'stop_zone'], inplace=True)
API_DF_STOPS = API_DF_STOPS[['stop_id', 'stop_name', 'stop_suburb', 'stop_latitude', 'stop_longitude', 'stop_sequence', 'route_id', 'direction_id',  'route_type',  'stop_landmark', 'stop_zones', 'stop_ticket_type', 'stop_is_free_fare_zone', 'stop_is_regional', 'stop_ticket_machine', 'stop_ticket_checks', 'stop_vline_reservation']]

API_DF_STOPS.to_csv(f'{DATA_DIR}/all_stops_stops.csv', index=False)
# API_DF_STOPS = pd.read_csv(f'{DATA_DIR}/all_stops_stops.csv')

logger.info(f'Stop ids count: {API_DF_STOPS['stop_id'].nunique()}')

API_all_stop_route_types = API_DF_STOPS[['stop_id', 'route_type']].drop_duplicates().apply(tuple, axis=1).tolist()

API_all_stop_route_types = [(str(stop_id), str(route_type)) for stop_id, route_type in API_all_stop_route_types]

logger.info(f'Stop id - Route types count: {len(API_all_stop_route_types)}')

# os.makedirs(f'{DATA_DIR}/stop_info', exist_ok=True)

API_STOPS_INFO = {}

FAILED_STOPS = []

for i, (stop_id, route_type) in enumerate(API_all_stop_route_types):
    logger.info(f'[{i}] Getting info for stop {stop_id}, route type {route_type}')
    endpoint = f'/v3/stops/{stop_id}/route_type/{route_type}?gtfs=false&stop_location=true&stop_amenities=true&stop_accessibility=true&stop_contact=true&stop_ticket=true&stop_staffing=true&stop_disruptions=false'
    data = None
    while data is None:
        try:
            data = get_data(endpoint)
            API_STOPS_INFO[stop_id] = API_STOPS_INFO.get(stop_id, {})
            # API_STOPS_INFO[stop_id][route_type] = json.load(open(f'{DATA_DIR}/stop_info/{stop_id}_{route_type}.json'))
            API_STOPS_INFO[stop_id][route_type] = data
            # print(f'[{i}] Got data for stop {stop_id}')
            logger.info(f'[{i}] Got data for stop {stop_id}')
            # with open(f'{DATA_DIR}/stop_info/{stop_id}_{route_type}.json', 'w') as f:
            #     f.write(json.dumps(data))
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [403, 503, 504]:
                logger.warning(f'[{i}] [{stop_id} {route_type}] Timeout. Error {e.response.status_code}. Retrying in 30 seconds...')
                time.sleep(30)
                continue
            else:
                logger.warning(f'[{i}] [{stop_id} {route_type}] Got error {e.response.status_code}.')
                FAILED_STOPS.append((stop_id, route_type))
                break

with open(f'{DATA_DIR}/stops_info.json', 'w') as f:
    f.write(json.dumps(API_STOPS_INFO))

with open(f'{DATA_DIR}/failed_stops.json', 'w') as f:
    f.write(json.dumps(FAILED_STOPS))

API_STOPS_INFO_LIST = []

for stop_id, route_type in API_all_stop_route_types:
    assert str(API_STOPS_INFO[stop_id][route_type]['stop']['route_type']) == str(route_type)
    assert str(API_STOPS_INFO[stop_id][route_type]['stop']['stop_id']) == str(stop_id)
    API_STOPS_INFO_LIST.append(API_STOPS_INFO[stop_id][route_type]['stop'])

for stop_info in API_STOPS_INFO_LIST:
    stop_info : dict
    kv_is_dict = [(k, v) for k, v in stop_info.items() if isinstance(v, dict)]
    for k, v in kv_is_dict:
        for k2, v2 in v.items():
            new_key = f'{k}_{k2}'
            assert new_key not in stop_info, f'{new_key} already exists in {stop_info}'
            stop_info[new_key] = v2
        del stop_info[k]

API_DF_STOPS_INFO = pd.DataFrame(API_STOPS_INFO_LIST, dtype=str)

assert API_DF_STOPS_INFO['station_details_id'].unique() == '0'

API_DF_STOPS_INFO.drop(columns=['station_details_id', 'disruption_ids'], inplace=True)


API_DF_STOPS_GTFS_MAP = API_DF_STOPS_INFO[['stop_id', 'route_type', 'point_id']].drop_duplicates()
assert (API_DF_STOPS_GTFS_MAP.groupby(['stop_id', 'point_id'])['route_type'].nunique() == 1).all()

logger.info(f'Start loading GTFS data')
GTFS = pyptvgtfs.process_gtfs_zip('http://data.ptv.vic.gov.au/downloads/gtfs.zip', '')
# 1m - 3m
logger.info(f'Finished loading GTFS data')
GTFS.drop(columns=['version_id'], inplace=True)
GTFS_DFS = GTFS.set_index(['mode_id', 'table_name'])['df'].to_dict()
new_GTFS_DFS = {}
for mid, v in GTFS_DFS.items():
    new_GTFS_DFS[mid[0]] = new_GTFS_DFS.get(mid[0], {})
    new_GTFS_DFS[mid[0]][mid[1]] = v
GTFS_DFS : dict[str, dict[str, pd.DataFrame]] = new_GTFS_DFS
for mid in GTFS_DFS:
    for tn in GTFS_DFS[mid]:
        GTFS_DFS[mid][tn]['mode_id'] = mid

GTFS_DF_STOPS = pd.concat([GTFS_DFS[mid]['stops'] for mid in GTFS_DFS], ignore_index=True)

GA_DF_STOPS = pd.merge(GTFS_DF_STOPS, API_DF_STOPS_GTFS_MAP, left_on='stop_id', right_on='point_id', how='outer', suffixes=('_gtfs', '_api'))

point_gtfs_mode_id_list = GA_DF_STOPS[GA_DF_STOPS['stop_id_api'].isna()][['stop_id_gtfs', 'mode_id']].apply(tuple, axis=1).unique()

GA_MISSING_STOPS = {}
priority_route_types = {
    '1': ['3', '0', '2', '1', '4'],
    '2': ['0', '3', '2', '1', '4'],
    '3': ['1', '2', '3', '0', '4'],
    '4': ['2', '3', '1', '0', '4'],
    '5': ['3', '2', '1', '0', '4'],
    '6': ['2', '3', '1', '0', '4'],
    '7': ['2', '3', '1', '0', '4'],
    '8': ['2', '3', '1', '0', '4'],
    '10': ['0', '3', '2', '1', '4'],
    '11': ['2', '3', '1', '0', '4'],
}
FAILED_STOPS_MISSING = []
logger.info(f'Start getting missing stops')
logger.info(f'Missing stops count: {len(point_gtfs_mode_id_list)}')
for i, (point_id, mode_id) in enumerate(point_gtfs_mode_id_list):
    for route_type in priority_route_types[mode_id]:
        endpoint = f'/v3/stops/{point_id}/route_type/{route_type}?gtfs=true&stop_location=true&stop_amenities=true&stop_accessibility=true&stop_contact=true&stop_ticket=true&stop_staffing=true&stop_disruptions=false'
        data = None
        while data is None:
            try:
                data = get_data(endpoint)
                GA_MISSING_STOPS[point_id] = GA_MISSING_STOPS.get(point_id, {})
                GA_MISSING_STOPS[point_id][route_type] = data['stop']
                logger.info(f'[{i}] [{point_id} {route_type}] Got data for stop {point_id}')
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in [403, 503, 504]:
                    logger.warning(f'[{i}] [{point_id} {route_type}] Timeout. Error {e.response.status_code}. Retrying in 30 seconds...')
                    time.sleep(30)
                    continue
                else:
                    logger.warning(f'[{i}] [{point_id} {route_type}] Got error {e.response.status_code}.')
                    break
    if point_id not in GA_MISSING_STOPS:
        logger.warning(f'[{i}] ERROR: Failed to get data for stop {point_id}')
        FAILED_STOPS_MISSING.append(point_id)

# Full 5 route_types: 1m - 2m per 100. 86 mins total for 7694 stops
# Select 1 route_type only: 17m - 20m total
with open(f'{DATA_DIR}/missing_stops_real.json', 'w') as f:
    f.write(json.dumps(GA_MISSING_STOPS))

with open(f'{DATA_DIR}/failed_stops_missing.json', 'w') as f:
    f.write(json.dumps(FAILED_STOPS_MISSING))
