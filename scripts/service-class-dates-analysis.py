
import pyptvgtfs
from pyptvgtfs import GTFS_FILE_FIELDS_TYPES, TABLE_NAMES, MODE_IDS, MODE_IDS_ALL
import pandas as pd
import os
import datetime as dt
import re
import numpy as np


VERSIONS = [
    '20220403_025040',
    '20230805_030129',
    '20231021_105623',
    '20240229_224711'
]

# VERSIONS x MODES
VERSIONS_MODES = [(v, b) for v in VERSIONS for b in MODE_IDS]
VERSIONS_MODES_ALL = [(v, b) for v in VERSIONS for b in MODE_IDS_ALL]


DFS_LIST = [pyptvgtfs.process_gtfs_zip(f'../downloads/{f}/gtfs.zip', f) for f in VERSIONS]
# Per file: 40s - 1m - 3m. 5 files: 2m - 5m. 7 files: 3m - 5sm. 2 files: 1m 30s - 2m

DFK : dict[tuple, pd.DataFrame] = pd.concat(DFS_LIST, axis=0).set_index(['version_id', 'mode_id', 'table_name'])['df'].to_dict()

DF : dict[str, dict[str, dict[str, pd.DataFrame]]] = {}
for (vid, mid, table_name), df in DFK.items():
    DF[vid] = DF.get(vid, {})
    DF[vid][mid] = DF[vid].get(mid, {})
    DF[vid][mid][table_name] = df


# It seems that after at least August 2023 and since at least October 2023, the service_id for buses ('4') has changed from normal service names like all others (T2_1) to specific service names to each route (MF1-12-831-aus). For trip id, before, it's 2-831--2-T2-1, now, it's 12-831--1-MF1-102. Seems like the format of trip_ids in 4 (use '-' instead of '.') being different from other modes (1,2...) is still consistent among versions.
# On one hand this change makes 'calendar' and 'calendar_dates' tables become longer, have more redundant data points, and the service_id becomes longer. On the other hand, more specific service patterns to each route can be more informative and useful for table joining or querying about the service time of one specific route.
# The change might make the 'trips table becomes somewhat longer, but an analysis of trips len for mode_id=4 for each version shows that length of trips table was not significantly too different.

# Proof:
DFK['20230805_030129', '4', 'trips'].head()
DFK['20231021_105623', '4', 'trips'].head()
for version in VERSIONS:
    print(version, len(DFK[version, '4', 'trips']))


# Assert all shape_id contains route_id
for vid in VERSIONS:
    for mid in MODE_IDS:
        assert DF[vid][mid]['trips'].dropna(subset=['route_id', 'shape_id']).apply(lambda x: x['route_id'] in x['shape_id'], axis=1).all(), (vid, mid)

# Assert all shape start with distance = 0
for vid in VERSIONS:
    for mid in MODE_IDS:
        assert DF[vid][mid]['shapes'][DF[vid][mid]['shapes']['shape_pt_sequence'] == 1]['shape_dist_traveled'].unique() == [0], (vid, mid)



def get_dates(monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date):
    # Get list of dates based on week pattern and date range
    week_pattern = [bool(int(monday)), bool(int(tuesday)), bool(int(wednesday)), bool(int(thursday)), bool(int(friday)), bool(int(saturday)), bool(int(sunday))]
    start_date = pd.to_datetime(start_date, format='%Y%m%d')
    end_date = pd.to_datetime(end_date, format='%Y%m%d')
    dates = pd.date_range(start_date, end_date)
    return dates[[week_pattern[i] for i in dates.dayofweek]]
    
def get_dates_df_calendar(df_calendar: pd.DataFrame, df_calendar_dates: pd.DataFrame):
    
    weekdate_columns = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    daterange_columns = ['start_date', 'end_date']
    pattern_columns = weekdate_columns + daterange_columns
    
    # Drop duplicates to reduce the number of rows to be processed
    df_dates = df_calendar[pattern_columns].drop_duplicates()
    
    # Get date list based on week pattern and date range
    df_dates['date'] = df_dates.apply(lambda x: get_dates(x['monday'], x['tuesday'], x['wednesday'], x['thursday'], x['friday'], x['saturday'], x['sunday'], x['start_date'], x['end_date']), axis=1)

    df_dates['date'] = df_dates['date'].apply(lambda x: [y.strftime('%Y%m%d') for y in x])
    
    # Join the date list with the original calendar table
    df_dates = pd.merge(df_calendar, df_dates, on=pattern_columns, how='left')
    
    # Explode the date list into separate rows
    df_dates = df_dates[['service_id', 'date']].explode('date')

    # Join the date df with the calendar_dates df
    df_dates = pd.merge(df_dates, df_calendar_dates.astype({'date': str, 'exception_type': str}), on=['service_id', 'date'], how='outer')
    
    # Drop 2 and keep 1 and NaN
    df_dates = df_dates[df_dates['exception_type'] != '2'].reset_index(drop=True)

    return df_dates



for vid, mid in VERSIONS_MODES:
    DF[vid][mid]['service_ids'] = pd.DataFrame(DF[vid][mid]['calendar']['service_id'].drop_duplicates().reset_index(drop=True))
    DF[vid][mid]['route_ids'] = pd.DataFrame(DF[vid][mid]['routes']['route_id'].drop_duplicates().reset_index(drop=True))
    DF[vid][mid]['trip_ids'] = pd.DataFrame(DF[vid][mid]['trips']['trip_id'].drop_duplicates().reset_index(drop=True))
    DF[vid][mid]['shape_ids'] = pd.DataFrame(DF[vid][mid]['shapes']['shape_id'].drop_duplicates().reset_index(drop=True))
# 1s - 2s

for vid, mid in VERSIONS_MODES:
    # Get all types of delimiters
    DF[vid][mid]['patterns'] = {}
    DF[vid][mid]['patterns'] = {}
    DF[vid][mid]['delimiters'] = {}
    for id_name in ['service_id', 'route_id', 'trip_id', 'shape_id']:
        id_pattern = DF[vid][mid][f'{id_name}s'][id_name].str.replace(r'[a-zA-Z0-9]+', '0', regex=True).drop_duplicates()
        DF[vid][mid]['patterns'][id_name] = id_pattern.unique()
        id_pattern = id_pattern.str.replace(r'[0]', '', regex=True).unique()
        # Sum all in DF[vid][mid]['patterns'][id_name] and remove duplicates
        DF[vid][mid]['delimiters'][id_name] = set(''.join(id_pattern))
# 2s - 5s
        
ID_PATTERNS = {
    k: pd.DataFrame(
        data=[(vid, mid, DF[vid][mid]["patterns"][k]) for vid, mid in VERSIONS_MODES],
        columns=["version_id", "mode_id", "pattern"],
    )
    .explode("pattern")
    .groupby("pattern")["mode_id"]
    .apply(lambda x: sorted(int(i) for i in x.unique()))
    .to_dict()
    for k in ["service_id", "route_id", "trip_id", "shape_id"]
}


ID_PATTERNS == {
    "service_id": {
        "0": [1, 2, 3, 4, 5, 6, 10, 11],
        "0+0": [1, 2, 3, 4, 5, 6, 10],
        "0+0_0": [1, 2, 3, 4, 5, 6, 10],
        "0-0-0-0": [4],
        "0-0-0-0-0": [4],
        "0_0": [1, 2, 3, 4, 5, 6, 10],
    },
    "route_id": {
        "0-0-0-0": [1, 2, 3, 4, 5, 6, 10, 11],
        "0-0-0-0-0": [1, 2, 3, 4, 5, 6, 10],
    },
    "trip_id": {
        "0-0--0-0-0": [4],
        "0-0-0-0-0-0": [4],
        "0.0.0-0-0-0-0.0.0": [1, 2, 3, 5, 6, 10],
        "0.0.0-0-0-0.0.0": [1, 2, 3, 5, 6, 10, 11],
    },
    "shape_id": {
        "0-0-0-0-0.0.0": [1, 2, 3, 4, 5, 6, 10],
        "0-0-0-0.0.0": [1, 2, 3, 4, 5, 6, 10, 11],
    },
}


for vid, mid in VERSIONS_MODES:
    DF[vid][mid]['service_ids']['service_class'] = DF[vid][mid]['service_ids']['service_id'].apply(lambda x: x.split('-')[0].split('+')[0].split('_')[0])

for vid, mid in VERSIONS_MODES:
    df_route_idx = DF[vid][mid]['route_ids']['route_id'].apply(lambda x: x.split('-'))
    DF[vid][mid]['route_ids']['route_code'] = df_route_idx.apply(lambda x: x[1])
    DF[vid][mid]['route_ids']['route_code_extra'] = df_route_idx.apply(lambda x: x[2] if len(x) >= 5 else '')
    DF[vid][mid]['route_ids']['route_no'] = df_route_idx.apply(lambda x: x[-1])
    DF[vid][mid]['route_ids']['operator'] = df_route_idx.apply(lambda x: x[0])
    DF[vid][mid]['route_ids']['range'] = df_route_idx.apply(lambda x: x[-2])


for vid, mid in VERSIONS_MODES:
    df_shape_idx = DF[vid][mid]['shape_ids']['shape_id'].apply(lambda x: x.split('.'))
    df_route_id = df_shape_idx.apply(lambda x: x[0])
    df_route_idx = df_route_id.apply(lambda x: x.split('-'))

    DF[vid][mid]['shape_ids']['route_id'] = df_route_id
    DF[vid][mid]['shape_ids']['route_code'] = df_route_idx.apply(lambda x: x[1])
    DF[vid][mid]['shape_ids']['route_code_extra'] = df_route_idx.apply(lambda x: x[2] if len(x) >= 5 else '')
    DF[vid][mid]['shape_ids']['route_no'] = df_route_idx.apply(lambda x: x[-1])
    DF[vid][mid]['shape_ids']['operator'] = df_route_idx.apply(lambda x: x[0])
    DF[vid][mid]['shape_ids']['direction'] = df_shape_idx.apply(lambda x: x[2])
    DF[vid][mid]['shape_ids']['range'] = df_route_idx.apply(lambda x: x[-2])
    DF[vid][mid]['shape_ids']['shape_no'] = df_shape_idx.apply(lambda x: x[1])
# 1s
    
for vid, mid in VERSIONS_MODES:
    if mid == '4':
        df_trip_idx = DF[vid][mid]['trip_ids']['trip_id'].apply(lambda x: x.split('-'))
        DF[vid][mid]['trip_ids']['route_code'] = df_trip_idx.apply(lambda x: x[1])
        DF[vid][mid]['trip_ids']['route_code_extra'] = df_trip_idx.apply(lambda x: x[2])
        DF[vid][mid]['trip_ids']['route_no'] = df_trip_idx.apply(lambda x: x[3])
        DF[vid][mid]['trip_ids']['operator'] = df_trip_idx.apply(lambda x: x[0])
        DF[vid][mid]['trip_ids']['service_class'] = df_trip_idx.apply(lambda x: x[4])
        DF[vid][mid]['trip_ids']['trip_no'] = df_trip_idx.apply(lambda x: x[5])
    else:
        df_trip_idx = DF[vid][mid]['trip_ids']['trip_id'].apply(lambda x: x.split('.'))
        df_route_id = df_trip_idx.apply(lambda x: x[2])
        df_route_idx = df_route_id.apply(lambda x: x.split('-'))

        # DF[vid][mid]['trip_ids']['route_id'] = df_route_id
        # DF[vid][mid]['trip_ids']['direction'] = df_trip_idx.apply(lambda x: x[4])
        # DF[vid][mid]['trip_ids']['shape_no'] = df_trip_idx.apply(lambda x: x[3])
        # DF[vid][mid]['trip_ids']['range'] = df_route_idx.apply(lambda x: x[-2])
        
        DF[vid][mid]['trip_ids']['route_code'] = df_route_idx.apply(lambda x: x[1])
        DF[vid][mid]['trip_ids']['route_code_extra'] = df_route_idx.apply(lambda x: x[2] if len(x) >= 5 else '')
        DF[vid][mid]['trip_ids']['route_no'] = df_route_idx.apply(lambda x: x[-1])
        DF[vid][mid]['trip_ids']['operator'] = df_route_idx.apply(lambda x: x[0])
        DF[vid][mid]['trip_ids']['service_class'] = df_trip_idx.apply(lambda x: x[1])
        DF[vid][mid]['trip_ids']['trip_no'] = df_trip_idx.apply(lambda x: x[0])
# 7s - 20s


for vid, mid in VERSIONS_MODES:
    DF[vid][mid]['dates'] = get_dates_df_calendar(DF[vid][mid]['calendar'], DF[vid][mid]['calendar_dates'])
# 1s - 5s


for vid, mid in VERSIONS_MODES:
    DF[vid][mid]['route_services'] = DF[vid][mid]['trips'][['route_id', 'service_id']].drop_duplicates().reset_index(drop=True)
    DF[vid][mid]['route_services'] = pd.merge(DF[vid][mid]['route_services'], DF[vid][mid]['route_ids'], on='route_id', how='left')
    DF[vid][mid]['route_services'] = pd.merge(DF[vid][mid]['route_services'], DF[vid][mid]['service_ids'], on='service_id', how='left')


for vid, mid in VERSIONS_MODES:
    DF[vid][mid]['route_service_dates'] = pd.merge(DF[vid][mid]['route_services'], DF[vid][mid]['dates'], on='service_id', how='left')



for vid, mid in VERSIONS_MODES:
    DF[vid][mid]['route_date_services'] = DF[vid][mid]['route_service_dates'].groupby(['route_id', 'date']).aggregate({'service_id': 'unique', 'service_class': 'unique'}).reset_index()
    # 7s - 20s


for vid, mid in VERSIONS_MODES:
    # Assert that for each route_id, for each date, there is only one service_class
    assert DF[vid][mid]['route_date_services']['service_class'].apply(lambda x: len(x) == 1).all(), (vid, mid)
    # Some route_id - date has more than 1 service_id. These occurs in 1 2 5 6, and 4 prior to 4's service_id format change after August 2023. Other than that, all route_id - date has only 1 service_id.
    ans = DF[vid][mid]['route_date_services']['service_id'].apply(lambda x: len(x) == 1).all()
    print(vid, mid, (ans if ans else ''))


for vid, mid in VERSIONS_MODES:
    DF[vid][mid]['route_date_services_2'] = DF[vid][mid]['route_service_dates'].groupby(['route_code', 'route_code_extra', 'date']).aggregate({'service_id': 'unique', 'service_class': 'unique'}).reset_index()
    # 7s - 20s


for vid, mid in VERSIONS_MODES:
    # Some route_code - date has more than 1 service_class. Most of these only occur in 4.
    if not DF[vid][mid]['route_date_services_2']['service_class'].apply(lambda x: len(x) == 1).all():
        print(vid, mid)


for vid, mid in VERSIONS_MODES:
    DF[vid][mid]['route_date_services_3'] = DF[vid][mid]['route_service_dates'].groupby(['route_code', 'route_code_extra', 'operator', 'date']).aggregate({'service_id': 'unique', 'service_class': 'unique'}).reset_index()
    # 7s - 20s

# Proof that route_code - operator - date has only 1 service_class.
for vid, mid in VERSIONS_MODES:
    # Assert route_code - operator - date has only 1 service_class.
    assert DF[vid][mid]['route_date_services_3']['service_class'].apply(lambda x: len(x) == 1).all(), (vid, mid)
