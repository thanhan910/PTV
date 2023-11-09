import zipfile
import pandas as pd
import io
import requests
import os
import logging
from scipy.spatial import cKDTree
import math
import numpy as np

from .GtfsDataframe import GtfsDataframe


def process_gtfs_zipfile_obj(gtfs_zip: zipfile.ZipFile, version_id: str) -> pd.DataFrame:
    dfs = []
    for item in gtfs_zip.namelist():
        if item.endswith('/'): # Check if the item is a directory
            branch_id = item.strip('/')
            google_transit_zip_path = f"{branch_id}/google_transit.zip"
            with gtfs_zip.open(google_transit_zip_path) as google_transit_file:
                with zipfile.ZipFile(google_transit_file, 'r') as transit_zip:
                    nested_file_list = transit_zip.namelist()
                    for nested_file_name in nested_file_list:
                        if nested_file_name.endswith('.txt'):

                            table_name = nested_file_name.removesuffix('.txt')
                            logging.info(f"Processing {branch_id} {table_name} {version_id}")

                            with transit_zip.open(nested_file_name) as nested_file:

                                df = pd.read_csv(nested_file, keep_default_na=False, low_memory=False, na_values=[''])
                                
                                df_gtfs = {
                                    'table_name': table_name,
                                    'branch_id': branch_id,
                                    'version_id': version_id,
                                    'df': df
                                }
                                
                                dfs.append(df_gtfs)
                    
            logging.info(f"Processed {branch_id}")

    return pd.DataFrame(dfs)
        

def process_gtfs_zip(url_or_path : str, version_id : str) -> pd.DataFrame:     
    
    if ":" in url_or_path: # If url_or_path is a URL
        
        response = requests.get(url_or_path, stream=True)

        if response.status_code == 200:
            # Create a ZipFile object from the response content
            with zipfile.ZipFile(io.BytesIO(response.content)) as gtfs_zip:
                return process_gtfs_zipfile_obj(gtfs_zip=gtfs_zip, version_id=version_id)
    
    with zipfile.ZipFile(url_or_path, 'r') as gtfs_zip:
        return process_gtfs_zipfile_obj(gtfs_zip=gtfs_zip, version_id=version_id)
    

def download_gtfs_zip(url: str, output_dir: str):
    '''Download a GTFS ZIP file from the URL and save it to the output directory'''
    response = requests.get(url, stream=True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if response.status_code == 200:
        with open(os.path.join(output_dir, 'gtfs.zip'), 'wb') as gtfs_zip_file:
            gtfs_zip_file.write(response.content)
    else:
        logging.error(f"Failed to download GTFS ZIP from {url}")



def create_service_ids_dates_df(calendar_df : pd.DataFrame, calendar_dates_df : pd.DataFrame):
    '''Get the list of dates for each service id'''

    exceptions = calendar_dates_df.groupby('service_id').apply(lambda x: x.groupby('exception_type' ).apply(lambda y:  y['date'].transform(str).tolist()))

    dates = calendar_df.set_index('service_id').apply(lambda row: [date.strftime('%Y%m%d') for date in pd.date_range(start=pd.to_datetime(row['start_date'], format='%Y%m%d'), end=pd.to_datetime(row['end_date'], format='%Y%m%d')) if row[date.strftime('%A').lower()] == 1], axis=1)

    df_dates = dates.to_frame(name='dates').merge(exceptions, how='left', on='service_id')
    
    if 1 in df_dates.columns:
        df_dates['dates'] = df_dates.apply(lambda row: (row['dates'] + row[1]) if (row[1] is not None) else row, axis=1)
    if 2 in df_dates.columns:
        df_dates['dates'] = df_dates.apply(lambda row: [x for x in row['dates'] if x not in row[2]] if (row[2] is not None) else row, axis=1)
    return df_dates['dates']



def haversine(lat1, lon1, lat2, lon2):
    # Radius of the Earth in kilometers
    R = 6371.0

    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Haversine formula
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c

    return distance

def find_nearest_points(lat, lon, max_distance, num_of_points, point_list):
    # Create a list to store distances and points
    distances_and_points = []

    for point in point_list:
        point_lat, point_lon = point
        distance = haversine(lat, lon, point_lat, point_lon)

        # Only consider points within the max_distance
        if distance <= max_distance:
            distances_and_points.append((distance, point))

    # Sort the list by distance
    distances_and_points.sort()

    # Return at most num_of_points nearest points
    return [point for _, point in distances_and_points[:num_of_points]]


def find_nearest_points2(lat, lon, max_distance, num_of_points, point_list):
    # Convert the coordinates to radians
    lat_rad = np.radians(lat)
    lon_rad = np.radians(lon)
    
    # Convert the list of points to radians
    point_list_radians = np.radians(point_list)

    # Create a cKDTree for the points
    kdtree = cKDTree(point_list_radians)

    # Query the kdtree for points within the max_distance
    distances, indices = kdtree.query([lat_rad, lon_rad], k=num_of_points, distance_upper_bound=max_distance)

    # Filter out points with distances exceeding max_distance
    valid_indices = np.where(distances < max_distance)
    valid_points = point_list_radians[indices[valid_indices]]

    # Convert valid points back to degrees
    valid_points_degrees = np.degrees(valid_points)

    return valid_points_degrees


def find_nearest_stops(df_stops: pd.DataFrame, lat, lon, max_distance_km, num_of_points):
    # find_nearest_stop
    tree = cKDTree(df_stops[['stop_lat', 'stop_lon']])
    dist, idx = tree.query([lat, lon], distance_upper_bound = max_distance_km, k=num_of_points)
    # Convert dist to km
    dist = dist * 111.139
    
    return df_stops.iloc[idx], dist