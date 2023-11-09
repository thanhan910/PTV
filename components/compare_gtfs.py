import pandas as pd
import logging

from .GtfsDataframe import GtfsDataframe, convert_gtfs_df_list_to_df

def compare_two_df(df1: pd.DataFrame, df2: pd.DataFrame, table_name = None, branch_id = None, columns = None) -> tuple[int, int, int]:
    '''Return a touple of (num_rows_in_df1_and_df2, num_rows_in_df1_not_df2, num_rows_in_df2_not_df1)'''
    if columns is None:
        columns = list(df1.columns)

    logging.debug(f'Comparing {len(df1)} rows in {table_name} to {len(df2)} rows in {table_name}, branch_id={branch_id}')
    logging.info(f'Comparing {len(df1)} rows in {table_name} to {len(df2)} rows in {table_name}, branch_id={branch_id}')

    # if df1 is empty, return (0, 0, len(df2))
    # if df2 is empty, return (0, len(df1), 0)
    # if both are empty, return (0, 0, 0)
    if len(df1) == 0:
        if len(df2) == 0:
            return (0, 0, 0)
        else:
            return (0, 0, len(df2))
    elif len(df2) == 0:
        return (0, len(df1), 0)

    df1_and_df2 = len(pd.merge(df1, df2, how='inner', on=columns))
    df1_not_df2 = len(df1) - df1_and_df2
    df2_not_df1 = len(df2) - df1_and_df2
    return (df1_and_df2, df1_not_df2, df2_not_df1)

def compare_ptv_gtfs_versions(gtfs_v1 : list[GtfsDataframe], gtfs_v2 : list[GtfsDataframe]):
    '''Compare two versions of PTV GTFS data and return a list of differences'''
    df_v1 = convert_gtfs_df_list_to_df(gtfs_v1)
    df_v2 = convert_gtfs_df_list_to_df(gtfs_v2)
    version_id_1 = gtfs_v1[0].version_id
    version_id_2 = gtfs_v2[0].version_id
    df_v1['version_id'] = version_id_1
    df_v2['version_id'] = version_id_2
    dfm = pd.merge(df_v1, df_v2, how='outer', on=['table_name', 'branch_id'], suffixes=('_v1', '_v2'))
    dfm['diff'] = dfm.apply(lambda x: pd.merge(x['df_v1'], x['df_v2'], how='inner'), axis=1)
    return dfm