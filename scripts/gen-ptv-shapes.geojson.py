import pandas as pd
import os
import shapely.geometry as sg
import geopandas as gpd
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

# https://data.ptv.vic.gov.au/downloads/GTFSReleaseNotes.pdf
BRANCH_NAMES = {
    '1' : 'Regional Train',
    '2' : 'Metropolitan Train',
    '3' : 'Metropolitan Tram',
    '4' : 'Metropolitan Bus',
    '5' : 'Regional Coach',
    '6' : 'Regional Bus',
    '7' : 'TeleBus',
    '8' : 'Night Bus',
    '10' : 'Interstate',
    '11' : 'SkyBus',
}


branch_ids_all = ['1', '2', '3', '4', '5', '6', '7', '8', '10', '11']
branch_ids = ['1', '2', '3', '4', '5', '6', '10', '11']
table_names = ['stop_times', 'stops', 'trips', 'routes', 'calendar', 'calendar_dates', 'agency', 'shapes']

def get_df(branch_id, table_name):
    files = [os.path.join(data_directory, f) for f in os.listdir(data_directory) if f.split('-')[1] == str(branch_id) and f.split('-')[2] == table_name]
    if len(files) == 0:
        return None
    return pd.concat([pd.read_csv(f) for f in files])

df = {branch_id: {table_name: get_df(branch_id, table_name) for table_name in table_names} for branch_id in branch_ids_all}

os.makedirs(os.path.join('..', 'data', 'ptv', 'shapes'), exist_ok=True)

for bid in branch_ids:
    df_shapes = df[bid]['shapes'].groupby('shape_id')[['shape_pt_lon', 'shape_pt_lat']].apply(lambda x: x.to_numpy())
    df_shapes = df_shapes.transform(lambda x: sg.LineString(x))
    df_shapes.rename('geometry', inplace=True)
    df_shapes = df_shapes.reset_index()
    gdf = gpd.GeoDataFrame(df_shapes, geometry='geometry')
    # gdf = gpd.GeoDataFrame(df_shapes, geometry='geometry')
    gdf.to_file(os.path.join('..', 'data', 'ptv', 'shapes', f'{bid}.geojson'))
# 1m 30s -3m 30s

df5_shapes = df['5']['shapes'].groupby('shape_id')[['shape_pt_lon', 'shape_pt_lat']].apply(lambda x: x.to_numpy())
df5_shapes = df5_shapes.transform(lambda x: sg.LineString(x))
df5_shapes.rename('geometry', inplace=True)
df5_shapes = df5_shapes.reset_index()
df5_shapes['route_name'] = df5_shapes['shape_id'].apply(lambda x: ''.join(x.split('-')[1:-2]))
df5_shapes['file'] = df5_shapes.index.map(lambda x: x // (len(df5_shapes) // 5))
df5_shapefiles = df5_shapes.groupby('route_name')['file'].unique().apply(lambda x: x[0]).rename('file').reset_index()
df5_shapes.drop(columns='file', inplace=True)
df5_shapes = pd.merge(df5_shapes, df5_shapefiles, on='route_name')
gdf5 = gpd.GeoDataFrame(df5_shapes, geometry='geometry')
# 1m

gdf5.groupby('file').apply(lambda x: x[['shape_id', 'geometry']].to_file(os.path.join('..', 'data', 'ptv', 'shapes', f'5-{x.name}.geojson')))
# 30s - 1m