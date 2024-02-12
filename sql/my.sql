SELECT string_agg(column_name, ',') 
FROM information_schema.columns
WHERE table_name = 'table_name' AND column_name != 'version_id';

CREATE TABLE a_agency 
AS SELECT 
    agency_id,
    agency_name,
    agency_url,
    agency_timezone,
    agency_lang,
    branch_id,
    array_agg(version_id) AS version_ids 
FROM public.agency
GROUP BY
    agency_id,
    agency_name,
    agency_url,
    agency_timezone,
    agency_lang,
    branch_id
ORDER BY branch_id;


CREATE TABLE a_calendar_dates
AS SELECT 
    service_id,
    date,
    exception_type,
    branch_id,
    array_agg(version_id) AS version_ids
FROM public.calendar_dates
GROUP BY
    service_id,
    date,
    exception_type,
    branch_id
ORDER BY branch_id;


CREATE TABLE a_calendar
AS SELECT 
    service_id,
    monday,
    tuesday,
    wednesday,
    thursday,
    friday,
    saturday,
    sunday,
    start_date,
    end_date,
    branch_id,
    array_agg(version_id) AS version_ids
FROM public.calendar
GROUP BY
    service_id,
    monday,
    tuesday,
    wednesday,
    thursday,
    friday,
    saturday,
    sunday,
    start_date,
    end_date,
    branch_id
ORDER BY branch_id;


CREATE TABLE a_routes
AS SELECT 
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    route_type,
    route_color,
    route_text_color,
    branch_id,
    array_agg(version_id) AS version_ids
FROM public.routes
GROUP BY
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    route_type,
    route_color,
    route_text_color,
    branch_id
ORDER BY branch_id;


CREATE TABLE a_shapes
AS SELECT 
    shape_id,
    shape_pt_lat,
    shape_pt_lon,
    shape_pt_sequence,
    shape_dist_traveled,
    branch_id,
    array_agg(version_id) AS version_ids
FROM public.shapes
GROUP BY
    shape_id,
    shape_pt_lat,
    shape_pt_lon,
    shape_pt_sequence,
    shape_dist_traveled,
    branch_id
ORDER BY branch_id;


CREATE TABLE a_stops 
AS SELECT 
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    branch_id,
    array_agg(version_id) AS version_ids 
FROM public.stops
GROUP BY
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    branch_id
ORDER BY branch_id;


CREATE TABLE a_trips 
AS SELECT 
    route_id,
    service_id,
    trip_id,
    shape_id,
    trip_headsign,
    direction_id,
    branch_id,
    array_agg(version_id) AS version_ids 
FROM public.trips
GROUP BY
    route_id,
    service_id,
    trip_id,
    shape_id,
    trip_headsign,
    direction_id,
    version_id,
    branch_id
ORDER BY branch_id;


CREATE TABLE a_stop_times
AS SELECT 
    trip_id,
    arrival_time,
    departure_time,
    stop_id,
    stop_sequence,
    stop_headsign,
    pickup_type,
    drop_off_type,
    shape_dist_traveled,
    branch_id,
    array_agg(version_id) AS version_ids
FROM public.stop_times
GROUP BY
    trip_id,
    arrival_time,
    departure_time,
    stop_id,
    stop_sequence,
    stop_headsign,
    pickup_type,
    drop_off_type,
    shape_dist_traveled,
    branch_id
ORDER BY branch_id;

SELECT * FROM public.a_calendar
WHERE '20231021_105623' = ANY(a_calendar.version_ids);