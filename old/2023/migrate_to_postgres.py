import zipfile
from sqlalchemy import create_engine
import os
import pandas as pd
import concurrent.futures
import io
import time
import psycopg2
import requests
from datetime import datetime

import components as comp

# Extract data from the nested ZIP files

dfs : list[comp.GtfsDataframe] = []

for dirpath, dirnames, filenames in os.walk('downloads'):
    for filename in filenames:
        gtfs_zip_path = os.path.join(dirpath, filename)
        version_id = gtfs_zip_path.split(os.sep)[-2]
        dfs1 = comp.process_gtfs_zip(gtfs_zip_path, version_id)
        dfs.extend(dfs1)



dfs.extend(dfs1)

dfs.sort(key=lambda x: len(x.df), reverse=False)

pgdb = comp.PostGreSQLWrapper(db_name="gtfs")
    
with pgdb.engine.connect() as engine, pgdb.connection as conn, pgdb.cursor as cursor:

    def insert_to_pg(df : pd.DataFrame, version_id, branch_id, table_name, debug_message=''):
        start_time = time.perf_counter()
        print('START', debug_message, start_time)
        # Count the number of rows in the PostgreSQL table where the version_id and branch_id match
        count_query = f"SELECT COUNT(*) FROM {table_name} WHERE version_id='{version_id}' AND branch_id='{branch_id}'"
        count = pd.read_sql(count_query, engine).iloc[0, 0]
        print('COUNT', table_name, version_id, branch_id, count, len(df))
        if count > 0 and count != len(df):
            # Delete the rows where the version_id and branch_id match
            delete_query = f"DELETE FROM {table_name} WHERE version_id='{version_id}' AND branch_id='{branch_id}'"
            cursor.execute(delete_query)
            conn.commit()
            print('DELETE', table_name, version_id, branch_id, count, len(df))
            # df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000, method='multi')
            # print('INSERTED', table_name, version_id, branch_id, count, len(df))
        # elif count == 0:
            # df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000, method='multi')
            # print('INSERTED', table_name, version_id, branch_id, count, len(df))

        end_time = time.perf_counter()
        print('END', debug_message, end_time)
        print('Time elapsed', end_time-start_time)

    tables = {}

    # with concurrent.futures.ThreadPoolExecutor() as executor:
    for counter, dfx in enumerate(dfs):
        table_name = dfx.table_name
        version_id = dfx.version_id
        branch_id = dfx.branch_id
        df = dfx.df
        if df is None:
            continue
        if table_name not in tables:
            tables[table_name] = []
        tables[table_name].append(df)
        # df.to_csv(f"output/{table_name}-{version_id}-{branch_id}.csv", index=False)
        debug_message = f"Loop {counter} / {len(dfs)}"
        # executor.submit(insert_to_pg, df, version_id, branch_id, table_name, debug_message)
        insert_to_pg(df, version_id, branch_id, table_name, debug_message)

    for table_name, df_list in tables.items():
        pd.concat(df_list).to_csv(f"output_all/{table_name}.csv", index=False)
