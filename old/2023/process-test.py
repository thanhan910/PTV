# %%
import components as comp
import pandas as pd
import numpy as np
import os
import logging
import geopandas as gpd
import shapely
import geopy.distance
from scipy.spatial import cKDTree
geopy.distance.great_circle
from shapely.geometry import Point
from shapely.ops import nearest_points

# %%
# Given lat, lon, find the nearest stop
# Given a dataframe of stops, find the nearest stop for each stop

def find_nearest_stop(df_stops, lat, lon, k=1):
    tree = cKDTree(df_stops[['stop_lat', 'stop_lon']])
    dist, idx = tree.query([lat, lon], k=k)
    return df_stops.iloc[idx], dist

def find_nearest_stops(df, df_stops):
    tree = cKDTree(df_stops[['stop_lat', 'stop_lon']])
    dist, idx = tree.query(df[['stop_lat', 'stop_lon']], k=1)
    return df_stops.iloc[idx]



# %%
dfs = comp.process_gtfs_zip('downloads/20231030_224547/gtfs.zip', version_id='20231030_224547')
        

# %%
dft = dfs[dfs['branch_id'] == '4'].set_index('table_name')['df']

# %%
def create_service_ids_dates_df(calendar_df : pd.DataFrame, calendar_dates_df : pd.DataFrame):
    '''Get the list of dates for each service id'''

    exceptions = calendar_dates_df.groupby('service_id').apply(lambda x: x.groupby('exception_type' ).apply(lambda y:  y['date'].transform(str).tolist()))

    dates = calendar_df.set_index('service_id').apply(lambda row: [date.strftime('%Y%m%d') for date in pd.date_range(start=pd.to_datetime(row['start_date'], format='%Y%m%d'), end=pd.to_datetime(row['end_date'], format='%Y%m%d')) if row[date.strftime('%A').lower()] == 1], axis=1)

    df_dates = dates.to_frame(name='dates').merge(exceptions, how='left', on='service_id')
    
    if 1 in df_dates.columns:
        df_dates['dates'] = df_dates.apply(lambda row: (row['dates'] + row[1]) if (row[1] is not None) else row, axis=1)
    if 2 in df_dates.columns:
        df_dates['dates'] = df_dates.apply(lambda row: [x for x in row['dates'] if x not in row[2]] if (row[2] is not None) else row, axis=1)
    return df_dates['dates'].to_frame('date').explode('date').sort_values('date').reset_index()
    

d = create_service_ids_dates_df(dft['calendar'], dft['calendar_dates'])

# %%
# 8195, 6424
dk = dft["stop_times"][dft["stop_times"]["stop_id"] == 8195][
    ["trip_id", "arrival_time", "departure_time", "stop_id"]
].merge(
    dft["trips"]
    [["trip_id", "service_id", "route_id", "trip_headsign", "direction_id"]]
    ,
    how="left",
    on="trip_id",
).merge(
    d, how="left", on="service_id"
).merge(
    dft["stops"]
    [["stop_id", "stop_name"]]
    , 
    how="left", 
    on="stop_id"
).merge(
    dft["routes"]
    [["route_id", "route_short_name", "route_long_name"]]
    ,
    how="left",
    on="route_id",
).merge(
    dft["calendar"],
    how="left",
    on="service_id",
)[
    [
        "stop_id",
        "date",
        "arrival_time",
        "departure_time",
        "stop_name",
        "route_short_name",
        "route_long_name",
        "trip_headsign",
        "direction_id",
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
].sort_values(
    ["date", "arrival_time"]
)
dk["arrival_unix"] = pd.to_datetime(dk["date"], format="%Y%m%d") + pd.to_timedelta(dk["arrival_time"].apply(lambda x: int(x.split(":")[0])), unit="h") + pd.to_timedelta(dk["arrival_time"].apply(lambda x: int(x.split(":")[1])), unit="m") + pd.to_timedelta(dk["arrival_time"].apply(lambda x: int(x.split(":")[2])), unit="s")
dk["departure_unix"] = pd.to_datetime(dk["date"], format="%Y%m%d") + pd.to_timedelta(dk["departure_time"].apply(lambda x: int(x.split(":")[0])), unit="h") + pd.to_timedelta(dk["departure_time"].apply(lambda x: int(x.split(":")[1])), unit="m") + pd.to_timedelta(dk["departure_time"].apply(lambda x: int(x.split(":")[2])), unit="s")
dk.sort_values(["arrival_unix", "departure_unix"], inplace=True)
dk["wait_time"] = dk["departure_unix"] - dk["arrival_unix"].shift(1)
dk["next_time"] = dk["arrival_unix"].shift(-1) - dk["departure_unix"]

# %%
dk[dk['date'] == '20231109'][[
        "stop_id",
        "date",
        "arrival_time",
        "departure_time",
        "wait_time",
        "next_time",
        "stop_name",
        "route_short_name",
        "route_long_name",
        "trip_headsign",
        "direction_id",
    ]]

# %%

lat = -37.90891157469941 
lon = 145.12268025614884

# find_nearest_stop
tree = cKDTree(dft['stops'][['stop_lat', 'stop_lon']])
dist, idx = tree.query([lat, lon], distance_upper_bound = 100, k=10)
dft['stops'].iloc[idx]


