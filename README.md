# PTV GTFS Data 

- Project started on 3 April 2022.
- Project updated in July 2022, August 2023, October 2023, and so on.

This project focuses on analyzing the State of Victoria's Public Transport System. (PTV) through its GTFS data.

## Data source


Source: Licensed from Public Transport Victoria under a Creative Commons Attribution 4.0 International Licence.

- Source: https://discover.data.vic.gov.au/dataset/timetable-and-geographic-information-gtfs
- Download link: http://data.ptv.vic.gov.au/downloads/gtfs.zip


## Resources

- https://discover.data.vic.gov.au/dataset/timetable-and-geographic-information-gtfs
- https://github.com/TransportVic/TransportVic2
- https://developers.google.com/transit/gtfs/reference


## Data dictionary

GTFS File Fields

agency.txt
- agency_id,
- agency_name,
- agency_url,
- agency_timezone,
- agency_lang

calendar.txt
- service_id,
- monday,
- tuesday,
- wednesday,
- thursday,
- friday,
- saturday,
- sunday,
- start_date,
- end_date

calendar_dates.txt
- service_id,
- date,
- exception_type

routes.txt
- route_id,
- agency_id,
- route_short_name,
- route_long_name,
- route_type,
- route_color,
- route_text_color

trips.txt
- route_id,
- service_id,
- trip_id,
- shape_id,
- trip_headsign,
- direction_id

stops.txt
- stop_id,
- stop_name,
- stop_lat,
- stop_lon

stop_times.txt
- trip_id,
- arrival_time,
- departure_time,
- stop_id,
- stop_sequence,
- stop_headsign,
- pickup_type,
- drop_off_type,
- shape_dist_traveled

shapes.txt
- shape_id,
- shape_pt_lat,
- shape_pt_lon,
- shape_pt_sequence,
- shape_dist_traveled 