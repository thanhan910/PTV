import numpy as np

# Source: https://developers.google.com/transit/gtfs/reference
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

# All mode IDs
MODE_IDS_ALL = ['1', '2', '3', '4', '5', '6', '7', '8', '10', '11']

# Operational MODE IDs except IDs that contain empty data
MODE_IDS = ['1', '2', '3', '4', '5', '6', '10', '11']

# Mode names
# Source: 
# https://discover.data.vic.gov.au/dataset/timetable-and-geographic-information-gtfs
# https://data.ptv.vic.gov.au/downloads/GTFSReleaseNotes.pdf
MODE_NAMES = {
    '1': 'Regional Train',
    '2': 'Metropolitan Train',
    '3': 'Metropolitan Tram',
    '4': 'Metropolitan Bus',
    '5': 'Regional Coach',
    '6': 'Regional Bus',
    '7': 'TeleBus',
    '8': 'Night Bus',
    '10': 'Interstate',
    '11': 'SkyBus',
}

# All GTFS Table Names
TABLE_NAMES = ['stop_times', 'stops', 'trips', 'routes', 'calendar', 'calendar_dates', 'agency', 'shapes']

GTFS_FILE_FIELDS = {
    'agency': ['agency_id', 'agency_name', 'agency_url', 'agency_timezone', 'agency_lang'],
    'calendar': ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date'],
    'calendar_dates': ['service_id', 'date', 'exception_type'],
    'routes': ['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type', 'route_color', 'route_text_color'],
    'trips': ['route_id', 'service_id', 'trip_id', 'shape_id', 'trip_headsign', 'direction_id'],
    'stops': ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'],
    'stop_times': ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence', 'stop_headsign', 'pickup_type', 'drop_off_type', 'shape_dist_traveled'],
    'shapes': ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence', 'shape_dist_traveled']
}

GTFS_FILE_FIELDS_TYPES = {
    'agency': {'agency_id': str, 'agency_name': str, 'agency_url': str, 'agency_timezone': str, 'agency_lang': str},
    'calendar': {'service_id': str, 'monday': int, 'tuesday': int, 'wednesday': int, 'thursday': int, 'friday': int, 'saturday': int, 'sunday': int, 'start_date': str, 'end_date': str},
    'calendar_dates': {'service_id': str, 'date': str, 'exception_type': str},
    'routes': {'route_id': str, 'agency_id': str, 'route_short_name': str, 'route_long_name': str, 'route_type': str, 'route_color': str, 'route_text_color' : str},
    'trips': {'route_id': str, 'service_id': str, 'trip_id': str, 'shape_id': str, 'trip_headsign': str, 'direction_id' : str},
    'stops': {'stop_id': str, 'stop_name': str, 'stop_lat': np.float64, 'stop_lon': np.float64},
    'stop_times': {'trip_id': str, 'arrival_time': str, 'departure_time': str, 'stop_id': str, 'stop_sequence': int, 'stop_headsign': str, 'pickup_type': str, 'drop_off_type': str, 'shape_dist_traveled': np.float64},
    'shapes': {'shape_id': str, 'shape_pt_lat': np.float64, 'shape_pt_lon': np.float64, 'shape_pt_sequence': int, 'shape_dist_traveled': np.float64},
}

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