import os
import pandas as pd

from PTVdb import PTVdb

PTV = PTVdb(database_directory=os.getcwd())

SERVICE = 2

DATABASE_BUILT = True

if(not DATABASE_BUILT):
    PTV.build_database()

DFS = {}
for record in PTV.PRIMARYKEYS.keys():
    DFS[record] = PTV.select(service=SERVICE, record=record)


df : pd.DataFrame = DFS["stop_times"]

df = pd.merge(df, DFS["trips"], on="trip_id")

df = pd.merge(df, DFS["routes"], on="route_id")

df = df[["route_short_name", "stop_id"]]

# df["route_id"] = df["route_id"].apply(lambda x: x[0:5])

# for index, value in df.iterrows():
#     print(value)

df = df.drop_duplicates()

print(len(df))

# print(df.value_counts())

df.to_csv("works/route_stops_3.csv")