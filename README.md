# PTV

This project focuses on analyzing Victoria's Geography, Suburb Boundaries, Public Transport System, and road network through publicly available data from PTV, ABS, Vicmap, GNAF, and other sources.

## Related repositories
- https://github.com/thanhan910/PTV
- https://github.com/thanhan910/pyptvgtfs
- https://github.com/thanhan910/pyptvdata
- https://github.com/thanhan910/vic_db
- https://github.com/thanhan910/vicpathfinding
- https://github.com/thanhan910/ausgeo
- https://github.com/thanhan910/SuburbSelect


## Old README: PTV Data 

This project focuses on analyzing the State of Victoria's Public Transport System (PTV) through its publicly available data.

## Data sources

Source: Licensed from Public Transport Victoria under a Creative Commons Attribution 4.0 International Licence.

- https://www.ptv.vic.gov.au/footer/data-and-reporting/datasets/

### PTV Timetable API

To register for a Developer ID and an API key, you may need to send an email to PTV. Refer to https://www.ptv.vic.gov.au/footer/data-and-reporting/datasets/ptv-timetable-api/ for more information.

- Home page: https://www.ptv.vic.gov.au/footer/data-and-reporting/datasets/ptv-timetable-api/
- Swagger UI: https://timetableapi.ptv.vic.gov.au/swagger/ui/index
- Swagger Docs JSON: https://timetableapi.ptv.vic.gov.au/swagger/docs/v3 (You can use this to find the endpoints you want to use.)

### PTV Spatial Datasets

- Home page: https://www.ptv.vic.gov.au/footer/data-and-reporting/datasets/
- Combined datasets collection source: https://datashare.maps.vic.gov.au/search?q=uuid%3D1792cbe0-25e5-52a0-8bc2-cc2294051634

You may need to add it to your cart and download it from the cart. Then the dataset will be sent to your email.

If you are using Python and geopandas, you can choose the SHP format.

### PTV's GTFS Data

- Home page: https://discover.data.vic.gov.au/dataset/timetable-and-geographic-information-gtfs
- Download link: http://data.ptv.vic.gov.au/downloads/gtfs.zip

The assumption is that the GTFS data is updated every week, on a Friday, at a time between 2 pm and 2:45 pm (seems like it is not 3 pm).

### PTV's GTFS Real-Time Data

- Data available at: https://data-exchange.vicroads.vic.gov.au/
- Data sources listed in: https://www.ptv.vic.gov.au/footer/data-and-reporting/datasets/

Refer to https://vicroadsopendatastorehouse.vicroads.vic.gov.au/opendata/GTFS-Realtime/GTFS-R%20Technical%20Feed%20Specification.pdf for how to access PTV's GTFS-R feed from https://data-exchange.vicroads.vic.gov.au/.

### Train station codes

- https://vicsig.net/index.php?page=infrastructure&section=codes&sort=n

## Resources

- https://www.ptv.vic.gov.au/footer/data-and-reporting/datasets/
- https://discover.data.vic.gov.au/dataset/timetable-and-geographic-information-gtfs
- https://developers.google.com/transit/gtfs/reference
- https://github.com/TransportVic/TransportVic2
- https://anytrip.com.au/


## Project information

- Project started on 3 April 2022.


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