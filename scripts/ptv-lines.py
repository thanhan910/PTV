
import pandas as pd
import os
import shapely.geometry as sg
import geopandas as gpd
import numpy as np
from shapely.geometry import LineString, MultiLineString, Point, MultiPoint
from shapely.ops import unary_union
data_directory = os.path.join('..', 'data', 'ptv', '20240224')



ROUTE_TYPES = {
    0 : 'Tram',
    1 : 'Metro',
    2 : 'Rail',
    3 : 'Bus',
    4 : 'Ferry',
    5 : 'Cable tram',
    6 : 'Gondola',
    7 : 'Funicular',
    11 : 'Trolleybus',
    12 : 'Monorail',
}
ROUTE_TYPES_LONG = {
    0 : 'Tram, Streetcar, Light rail. Any light rail or street level system within a metropolitan area.',
    1 : 'Subway, Metro. Any underground rail system within a metropolitan area.',
    2 : 'Rail. Used for intercity or long-distance travel.',
    3 : 'Bus. Used for short- and long-distance bus routes.',
    4 : 'Ferry. Used for short- and long-distance boat service.',
    5 : 'Cable tram. Used for street-level rail cars where the cable runs beneath the vehicle, e.g., cable car in San Francisco.',
    6 : 'Aerial lift, suspended cable car (e.g., gondola lift, aerial tramway). Cable transport where cabins, cars, gondolas or open chairs are suspended by means of one or more cables.',
    7 : 'Funicular. Any rail system designed for steep inclines.',
    11 : 'Trolleybus. Electric buses that draw power from overhead wires using poles.',
    12 : 'Monorail. Railway in which the track consists of a single rail or a beam.',
}

BRANCH_IDS_ALL = ['1', '2', '3', '4', '5', '6', '7', '8', '10', '11']
BRANCH_IDS = ['1', '2', '3', '4', '5', '6', '10', '11']
TABLE_NAMES = ['stop_times', 'stops', 'trips', 'routes', 'calendar', 'calendar_dates', 'agency', 'shapes']
# GTFS File Fields
# agency.txt 
# agency_id, agency_name, agency_url, agency_timezone, agency_lang
# calendar.txt 
# service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date
# calendar_dates.txt 
# service_id ,date, exception_type
# routes.txt 
# route_id, agency_id, route_short_name, route_long_name,
# route_type, route_color,route_text_color
# trips.txt 
# route_id, service_id, trip_id, shape_id, trip_headsign, direction_id
# stops.txt 
# stop_id, stop_name, stop_lat, stop_lon
# stop_times.txt 
# trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled
# shapes.txt 
# shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled 
def get_df(branch_id, table_name):
    files = [os.path.join(data_directory, f) for f in os.listdir(data_directory) if f.split('-')[1] == str(branch_id) and f.split('-')[2] == table_name]
    if len(files) == 0:
        return None
    return pd.concat([pd.read_csv(f, keep_default_na=False, na_values=['']) for f in files])

DF = {branch_id: {table_name: get_df(branch_id, table_name) for table_name in TABLE_NAMES} for branch_id in BRANCH_IDS_ALL}
# 15s - 30s


for bid in BRANCH_IDS:
    DF[bid]['shapes'].sort_values(by=['shape_id', 'shape_pt_sequence'], inplace=True)
    DF[bid]['shapes']['points'] = list(zip(DF[bid]['shapes']['shape_pt_lon'], DF[bid]['shapes']['shape_pt_lat']))
    DF[bid]['lines'] = DF[bid]['shapes'].groupby('shape_id')['points'].apply(np.array).rename('line').reset_index()
    DF[bid]['lines']['direction'] = DF[bid]['lines']['shape_id'].transform(lambda x: x.split('.')[-1])
    DF[bid]['lines']['route_id'] = DF[bid]['lines']['shape_id'].transform(lambda x: x.split('.')[0])
    DF[bid]['lines']['route_name'] = DF[bid]['lines']['route_id'].transform(lambda x: ''.join(x.split('-')[1:-2]))
    DF[bid]['lines']['branch'] = DF[bid]['lines']['route_id'].transform(lambda x: x.split('-')[0])
    DF[bid]['lines']['opbranch'] = bid

# Total 2m    
    
DFLINES : pd.DataFrame = pd.concat([DF[bid]['lines'] for bid in BRANCH_IDS])

DFLINES['points_count'] = DFLINES['line'].apply(len)

DFLINES = DFLINES[['route_name', 'direction', 'branch', 'opbranch', 'route_id', 'shape_id', 'line', 'points_count']]

DFLINES.sort_values(
    by=['opbranch', 'route_name', 'branch', 'direction', 'stops_count', 'points_count', 'stop_pattern'], 
    ascending=[True, True, True, True, False, False, True],
    inplace=True, 
)

DFLINES.reset_index(drop=True, inplace=True)

# Total: 1m 30s - 2m

def find_smallest_number_of_lists(lists):
    # Create a set to store all unique elements from all lists
    all_elements = set()
    for lst in lists:
        all_elements.update(lst)

    # Initialize an empty list to store the selected lists
    selected_lists = []

    # Iterate until all elements are covered
    while all_elements:
        # Find the list that covers the maximum number of uncovered elements
        max_covered = set()
        max_list = None
        for lst in lists:
            covered = set(lst).intersection(all_elements)
            if len(covered) > len(max_covered):
                max_covered = covered
                max_list = lst

        # Remove covered elements from the set of all elements
        all_elements.difference_update(max_covered)

        # Add the selected list to the result
        selected_lists.append(max_list)

    return selected_lists


DFLINESMULTI = DFLINES.groupby(['route_name', 'opbranch', 'direction'])['line'].apply(np.array).rename('lines').reset_index()




DFLINESMULTI['geo_full'] = DFLINESMULTI['lines'].apply(lambda x: [LineString(i) for i in x])
DFLINESMULTI['union_full'] = DFLINESMULTI['geo_full'].apply(unary_union)
DFLINESMULTI['geom_full_count'] = DFLINESMULTI['union_full'].apply(lambda x: len(x.geoms) if x.geom_type == 'MultiLineString' else 1)
# 12m - 15m


DFLINESGEOFULL = DFLINESMULTI[['route_name', 'opbranch', 'direction', 'union_full', 'geom_full_count']]

# Convert linestring to array of points
DFLINESGEOFULL['points_full'] = DFLINESGEOFULL['union_full'].apply(lambda x: np.array(x.geoms) if x.geom_type == 'MultiLineString' else np.array([x]))
# 5s - 10s
DFLINESGEOFULL = DFLINESGEOFULL.explode('points_full').reset_index(drop=True)
DFLINESGEOFULL['points_full'] = DFLINESGEOFULL['points_full'].apply(lambda x: np.array(x.coords))
# 5s - 10s
DFLINESGEOFULL['points_len'] = DFLINESGEOFULL['points_full'].apply(len)
DFLINESGEOFULL = DFLINESGEOFULL[['route_name', 'opbranch', 'direction', 'points_full', 'points_len']]

DFLINESGEOFULL = DFLINESGEOFULL.groupby(['route_name', 'opbranch', 'direction'])['points_full'].apply(np.array).reset_index()
DFLINESGEOFULL.rename(columns={'points_full': 'lines'}, inplace=True)
DFLINESGEOFULL['lines_count'] = DFLINESGEOFULL['lines'].apply(len)


def merge_directed_paths(input_paths : np.ndarray[np.ndarray[np.ndarray]]) -> np.ndarray[np.ndarray[np.ndarray]]:
    merged_paths = []
    for path in input_paths:
        merged = False
        for i, existing_path in enumerate(merged_paths):
            if all(path[0] == existing_path[-1]):
                merged_paths[i] = np.concatenate((existing_path, path[1:]))
                merged = True
                break
            elif all(path[-1] == existing_path[0]):
                merged_paths[i] = np.concatenate((path[:-1], existing_path))
                merged = True
                break
        if not merged:
            merged_paths.append(path)
    return merged_paths



DFLINESGEOFULL['paths'] = DFLINESGEOFULL['lines'].transform(lambda x: merge_directed_paths(x))
# 5s -10s
DFLINESGEOFULL['paths_count'] = DFLINESGEOFULL['paths'].apply(len)


DFLINESGEOFULL.sort_values(by='paths_count', ascending=False, inplace=True)
DFLINESGEOFULL.reset_index(drop=True, inplace=True)


DFLINESGEOFULL['geo_lines'] = DFLINESGEOFULL['lines'].apply(lambda x: MultiLineString([LineString(i) for i in x]))
# 10s - 15s


DFLINESGEOFULL['geo_paths'] = DFLINESGEOFULL['paths'].apply(lambda x: MultiLineString([LineString(i) for i in x]) if len(x) > 1 else LineString(x[0]))


GDF_LINES = DFLINESGEOFULL[['route_name', 'opbranch', 'direction', 'geo_paths']]
GDF_LINES = gpd.GeoDataFrame(GDF_LINES, geometry='geo_paths')


GDF_LINES.rename(columns={'opbranch': 'opbranch'}, inplace=True)


GDF_LINES.sort_values(by=['opbranch', 'route_name', 'direction'], inplace=True)


GDF_LINES.to_file('../data/ptv/ptv-lines.geojson', driver='GeoJSON')
# 20s

