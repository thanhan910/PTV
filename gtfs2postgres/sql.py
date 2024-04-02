import os

create_gtfs_sql = """
CREATE TABLE agency (
    agency_id TEXT,
    agency_name TEXT,
    agency_url TEXT,
    agency_timezone TEXT,
    agency_lang TEXT,
    PRIMARY KEY (agency_id)
);
CREATE TABLE calendar (
    service_id TEXT,
    monday INTEGER,
    tuesday INTEGER,
    wednesday INTEGER,
    thursday INTEGER,
    friday INTEGER,
    saturday INTEGER,
    sunday INTEGER,
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY (service_id)
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date TEXT,
    exception_type INTEGER,
    PRIMARY KEY (service_id, date),
    FOREIGN KEY (service_id) REFERENCES calendar(service_id)
);
CREATE TABLE routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER,
    route_color TEXT,
    route_text_color TEXT,
    PRIMARY KEY (route_id)
    -- FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
);
CREATE TABLE shapes (
    shape_id TEXT,
    shape_pt_lat REAL,
    shape_pt_lon REAL,
    shape_pt_sequence INTEGER,
    shape_dist_traveled REAL,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);
CREATE TABLE stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat REAL,
    stop_lon REAL,
    PRIMARY KEY (stop_id)
);
CREATE TABLE trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    shape_id TEXT,
    trip_headsign TEXT,
    direction_id INTEGER,
    PRIMARY KEY (trip_id),
    FOREIGN KEY (route_id) REFERENCES routes(route_id),
    FOREIGN KEY (service_id) REFERENCES calendar(service_id)
    -- FOREIGN KEY (shape_id) REFERENCES shapes(shape_id)
);
CREATE TABLE stop_times (
    trip_id TEXT,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT,
    stop_sequence INTEGER,
    stop_headsign TEXT,
    pickup_type INTEGER,
    drop_off_type INTEGER,
    shape_dist_traveled REAL,
    PRIMARY KEY (trip_id, stop_sequence),
    FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
);
"""
full_sql_script = ""
for mode_id in [1, 2, 3, 4, 5, 6, 7, 8, 10, 11]:
    schema_name = f"gtfs_{mode_id}"
    table_create_sql = f"""
DROP SCHEMA IF EXISTS {schema_name} CASCADE;
CREATE SCHEMA IF NOT EXISTS {schema_name};
SET search_path TO {schema_name};
{create_gtfs_sql}
"""
    full_sql_script += table_create_sql

sql_file_path = "../sql/gtfs.sql"

sql_file_path = os.path.join(os.path.dirname(__file__), sql_file_path)

with open(sql_file_path, "w") as f:
    f.write(full_sql_script)

