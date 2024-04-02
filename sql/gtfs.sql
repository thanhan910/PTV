
DROP SCHEMA IF EXISTS gtfs_1 CASCADE;
CREATE SCHEMA IF NOT EXISTS gtfs_1;
SET search_path TO gtfs_1;
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
    monday TEXT,
    tuesday TEXT,
    wednesday TEXT,
    thursday TEXT,
    friday TEXT,
    saturday TEXT,
    sunday TEXT,
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY (service_id)
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date TEXT,
    exception_type TEXT,
    PRIMARY KEY (service_id, date),
    FOREIGN KEY (service_id) REFERENCES calendar(service_id)
);
CREATE TABLE routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type TEXT,
    route_color TEXT,
    route_text_color TEXT,
    PRIMARY KEY (route_id)
    -- FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
);
CREATE TABLE shapes (
    shape_id TEXT,
    shape_pt_lat TEXT,
    shape_pt_lon TEXT,
    shape_pt_sequence TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);
CREATE TABLE stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat TEXT,
    stop_lon TEXT,
    PRIMARY KEY (stop_id)
);
CREATE TABLE trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    shape_id TEXT,
    trip_headsign TEXT,
    direction_id TEXT,
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
    stop_sequence TEXT,
    stop_headsign TEXT,
    pickup_type TEXT,
    drop_off_type TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (trip_id, stop_sequence)
    -- FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    -- FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
);

DROP SCHEMA IF EXISTS gtfs_2 CASCADE;
CREATE SCHEMA IF NOT EXISTS gtfs_2;
SET search_path TO gtfs_2;
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
    monday TEXT,
    tuesday TEXT,
    wednesday TEXT,
    thursday TEXT,
    friday TEXT,
    saturday TEXT,
    sunday TEXT,
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY (service_id)
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date TEXT,
    exception_type TEXT,
    PRIMARY KEY (service_id, date),
    FOREIGN KEY (service_id) REFERENCES calendar(service_id)
);
CREATE TABLE routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type TEXT,
    route_color TEXT,
    route_text_color TEXT,
    PRIMARY KEY (route_id)
    -- FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
);
CREATE TABLE shapes (
    shape_id TEXT,
    shape_pt_lat TEXT,
    shape_pt_lon TEXT,
    shape_pt_sequence TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);
CREATE TABLE stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat TEXT,
    stop_lon TEXT,
    PRIMARY KEY (stop_id)
);
CREATE TABLE trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    shape_id TEXT,
    trip_headsign TEXT,
    direction_id TEXT,
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
    stop_sequence TEXT,
    stop_headsign TEXT,
    pickup_type TEXT,
    drop_off_type TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (trip_id, stop_sequence)
    -- FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    -- FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
);

DROP SCHEMA IF EXISTS gtfs_3 CASCADE;
CREATE SCHEMA IF NOT EXISTS gtfs_3;
SET search_path TO gtfs_3;
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
    monday TEXT,
    tuesday TEXT,
    wednesday TEXT,
    thursday TEXT,
    friday TEXT,
    saturday TEXT,
    sunday TEXT,
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY (service_id)
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date TEXT,
    exception_type TEXT,
    PRIMARY KEY (service_id, date),
    FOREIGN KEY (service_id) REFERENCES calendar(service_id)
);
CREATE TABLE routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type TEXT,
    route_color TEXT,
    route_text_color TEXT,
    PRIMARY KEY (route_id)
    -- FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
);
CREATE TABLE shapes (
    shape_id TEXT,
    shape_pt_lat TEXT,
    shape_pt_lon TEXT,
    shape_pt_sequence TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);
CREATE TABLE stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat TEXT,
    stop_lon TEXT,
    PRIMARY KEY (stop_id)
);
CREATE TABLE trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    shape_id TEXT,
    trip_headsign TEXT,
    direction_id TEXT,
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
    stop_sequence TEXT,
    stop_headsign TEXT,
    pickup_type TEXT,
    drop_off_type TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (trip_id, stop_sequence)
    -- FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    -- FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
);

DROP SCHEMA IF EXISTS gtfs_4 CASCADE;
CREATE SCHEMA IF NOT EXISTS gtfs_4;
SET search_path TO gtfs_4;
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
    monday TEXT,
    tuesday TEXT,
    wednesday TEXT,
    thursday TEXT,
    friday TEXT,
    saturday TEXT,
    sunday TEXT,
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY (service_id)
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date TEXT,
    exception_type TEXT,
    PRIMARY KEY (service_id, date),
    FOREIGN KEY (service_id) REFERENCES calendar(service_id)
);
CREATE TABLE routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type TEXT,
    route_color TEXT,
    route_text_color TEXT,
    PRIMARY KEY (route_id)
    -- FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
);
CREATE TABLE shapes (
    shape_id TEXT,
    shape_pt_lat TEXT,
    shape_pt_lon TEXT,
    shape_pt_sequence TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);
CREATE TABLE stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat TEXT,
    stop_lon TEXT,
    PRIMARY KEY (stop_id)
);
CREATE TABLE trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    shape_id TEXT,
    trip_headsign TEXT,
    direction_id TEXT,
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
    stop_sequence TEXT,
    stop_headsign TEXT,
    pickup_type TEXT,
    drop_off_type TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (trip_id, stop_sequence)
    -- FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    -- FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
);

DROP SCHEMA IF EXISTS gtfs_5 CASCADE;
CREATE SCHEMA IF NOT EXISTS gtfs_5;
SET search_path TO gtfs_5;
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
    monday TEXT,
    tuesday TEXT,
    wednesday TEXT,
    thursday TEXT,
    friday TEXT,
    saturday TEXT,
    sunday TEXT,
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY (service_id)
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date TEXT,
    exception_type TEXT,
    PRIMARY KEY (service_id, date),
    FOREIGN KEY (service_id) REFERENCES calendar(service_id)
);
CREATE TABLE routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type TEXT,
    route_color TEXT,
    route_text_color TEXT,
    PRIMARY KEY (route_id)
    -- FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
);
CREATE TABLE shapes (
    shape_id TEXT,
    shape_pt_lat TEXT,
    shape_pt_lon TEXT,
    shape_pt_sequence TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);
CREATE TABLE stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat TEXT,
    stop_lon TEXT,
    PRIMARY KEY (stop_id)
);
CREATE TABLE trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    shape_id TEXT,
    trip_headsign TEXT,
    direction_id TEXT,
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
    stop_sequence TEXT,
    stop_headsign TEXT,
    pickup_type TEXT,
    drop_off_type TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (trip_id, stop_sequence)
    -- FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    -- FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
);

DROP SCHEMA IF EXISTS gtfs_6 CASCADE;
CREATE SCHEMA IF NOT EXISTS gtfs_6;
SET search_path TO gtfs_6;
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
    monday TEXT,
    tuesday TEXT,
    wednesday TEXT,
    thursday TEXT,
    friday TEXT,
    saturday TEXT,
    sunday TEXT,
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY (service_id)
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date TEXT,
    exception_type TEXT,
    PRIMARY KEY (service_id, date),
    FOREIGN KEY (service_id) REFERENCES calendar(service_id)
);
CREATE TABLE routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type TEXT,
    route_color TEXT,
    route_text_color TEXT,
    PRIMARY KEY (route_id)
    -- FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
);
CREATE TABLE shapes (
    shape_id TEXT,
    shape_pt_lat TEXT,
    shape_pt_lon TEXT,
    shape_pt_sequence TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);
CREATE TABLE stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat TEXT,
    stop_lon TEXT,
    PRIMARY KEY (stop_id)
);
CREATE TABLE trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    shape_id TEXT,
    trip_headsign TEXT,
    direction_id TEXT,
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
    stop_sequence TEXT,
    stop_headsign TEXT,
    pickup_type TEXT,
    drop_off_type TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (trip_id, stop_sequence)
    -- FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    -- FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
);

DROP SCHEMA IF EXISTS gtfs_7 CASCADE;
CREATE SCHEMA IF NOT EXISTS gtfs_7;
SET search_path TO gtfs_7;
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
    monday TEXT,
    tuesday TEXT,
    wednesday TEXT,
    thursday TEXT,
    friday TEXT,
    saturday TEXT,
    sunday TEXT,
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY (service_id)
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date TEXT,
    exception_type TEXT,
    PRIMARY KEY (service_id, date),
    FOREIGN KEY (service_id) REFERENCES calendar(service_id)
);
CREATE TABLE routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type TEXT,
    route_color TEXT,
    route_text_color TEXT,
    PRIMARY KEY (route_id)
    -- FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
);
CREATE TABLE shapes (
    shape_id TEXT,
    shape_pt_lat TEXT,
    shape_pt_lon TEXT,
    shape_pt_sequence TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);
CREATE TABLE stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat TEXT,
    stop_lon TEXT,
    PRIMARY KEY (stop_id)
);
CREATE TABLE trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    shape_id TEXT,
    trip_headsign TEXT,
    direction_id TEXT,
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
    stop_sequence TEXT,
    stop_headsign TEXT,
    pickup_type TEXT,
    drop_off_type TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (trip_id, stop_sequence)
    -- FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    -- FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
);

DROP SCHEMA IF EXISTS gtfs_8 CASCADE;
CREATE SCHEMA IF NOT EXISTS gtfs_8;
SET search_path TO gtfs_8;
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
    monday TEXT,
    tuesday TEXT,
    wednesday TEXT,
    thursday TEXT,
    friday TEXT,
    saturday TEXT,
    sunday TEXT,
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY (service_id)
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date TEXT,
    exception_type TEXT,
    PRIMARY KEY (service_id, date),
    FOREIGN KEY (service_id) REFERENCES calendar(service_id)
);
CREATE TABLE routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type TEXT,
    route_color TEXT,
    route_text_color TEXT,
    PRIMARY KEY (route_id)
    -- FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
);
CREATE TABLE shapes (
    shape_id TEXT,
    shape_pt_lat TEXT,
    shape_pt_lon TEXT,
    shape_pt_sequence TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);
CREATE TABLE stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat TEXT,
    stop_lon TEXT,
    PRIMARY KEY (stop_id)
);
CREATE TABLE trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    shape_id TEXT,
    trip_headsign TEXT,
    direction_id TEXT,
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
    stop_sequence TEXT,
    stop_headsign TEXT,
    pickup_type TEXT,
    drop_off_type TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (trip_id, stop_sequence)
    -- FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    -- FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
);

DROP SCHEMA IF EXISTS gtfs_10 CASCADE;
CREATE SCHEMA IF NOT EXISTS gtfs_10;
SET search_path TO gtfs_10;
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
    monday TEXT,
    tuesday TEXT,
    wednesday TEXT,
    thursday TEXT,
    friday TEXT,
    saturday TEXT,
    sunday TEXT,
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY (service_id)
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date TEXT,
    exception_type TEXT,
    PRIMARY KEY (service_id, date),
    FOREIGN KEY (service_id) REFERENCES calendar(service_id)
);
CREATE TABLE routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type TEXT,
    route_color TEXT,
    route_text_color TEXT,
    PRIMARY KEY (route_id)
    -- FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
);
CREATE TABLE shapes (
    shape_id TEXT,
    shape_pt_lat TEXT,
    shape_pt_lon TEXT,
    shape_pt_sequence TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);
CREATE TABLE stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat TEXT,
    stop_lon TEXT,
    PRIMARY KEY (stop_id)
);
CREATE TABLE trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    shape_id TEXT,
    trip_headsign TEXT,
    direction_id TEXT,
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
    stop_sequence TEXT,
    stop_headsign TEXT,
    pickup_type TEXT,
    drop_off_type TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (trip_id, stop_sequence)
    -- FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    -- FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
);

DROP SCHEMA IF EXISTS gtfs_11 CASCADE;
CREATE SCHEMA IF NOT EXISTS gtfs_11;
SET search_path TO gtfs_11;
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
    monday TEXT,
    tuesday TEXT,
    wednesday TEXT,
    thursday TEXT,
    friday TEXT,
    saturday TEXT,
    sunday TEXT,
    start_date TEXT,
    end_date TEXT,
    PRIMARY KEY (service_id)
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date TEXT,
    exception_type TEXT,
    PRIMARY KEY (service_id, date),
    FOREIGN KEY (service_id) REFERENCES calendar(service_id)
);
CREATE TABLE routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type TEXT,
    route_color TEXT,
    route_text_color TEXT,
    PRIMARY KEY (route_id)
    -- FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
);
CREATE TABLE shapes (
    shape_id TEXT,
    shape_pt_lat TEXT,
    shape_pt_lon TEXT,
    shape_pt_sequence TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);
CREATE TABLE stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat TEXT,
    stop_lon TEXT,
    PRIMARY KEY (stop_id)
);
CREATE TABLE trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    shape_id TEXT,
    trip_headsign TEXT,
    direction_id TEXT,
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
    stop_sequence TEXT,
    stop_headsign TEXT,
    pickup_type TEXT,
    drop_off_type TEXT,
    shape_dist_traveled TEXT,
    PRIMARY KEY (trip_id, stop_sequence)
    -- FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    -- FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
);
