import os
import pandas as pd
import json

from PTVdb import PTVdb

PTV = PTVdb(database_directory=os.getcwd())

SERVICE = 2

DATABASE_BUILT = True

if(not DATABASE_BUILT):
    PTV.build_database()

DFS = {}
for record in PTV.PRIMARYKEYS.keys():
    DFS[record] = PTV.select(service=SERVICE, record=record)


def stops_to_geojson(load_to_file = None, properties_key = None):

    geojson = {}
    geojson["type"] = "FeatureCollection"

    features = []

    for index, value in DFS["stops"].iterrows():
        feature = {}
        feature["type"] = "Feature"
        feature["geometry"] = {
            "type": "Point",
            "coordinates": [
                value["stop_lon"],
                value["stop_lat"]
            ]
        }
        feature["id"] = str(value["stop_id"])

        if(properties_key != None):
            feature["properties"] = {}
            for key in properties_key:
                feature["properties"][key] = value[key]

        features.append(feature)

    geojson["features"] = features

    if(load_to_file != None):
        open(load_to_file, "w").write(json.dumps(geojson, indent="\t", sort_keys=False))

    return geojson

    
def stops_to_geojson_multipoint(load_to_file = None):

    geojson = {}
    geojson["type"] = "FeatureCollection"

    features = []

    feature = {}
    feature["type"] = "Feature"
    feature["geometry"] = {
        "type": "MultiPoint",
        "coordinates": []
    }

    for index, value in DFS["stops"].iterrows():
        feature["geometry"]["coordinates"].append([value["stop_lon"],value["stop_lat"]])

    features.append(feature)

    geojson["features"] = features

    if(load_to_file != None):
        open(load_to_file, "w").write(json.dumps(geojson, indent="\t", sort_keys=False))

    return geojson


def shapes_to_geojson(load_to_file = None):

    mapfeatures = {}

    geojson = {}
    geojson["type"] = "FeatureCollection"

    features = []

    for index, value in DFS["shapes"].iterrows():
        if(mapfeatures.get(value["shape_id"]) == None):
            mapfeatures[value["shape_id"]] = {"coordinates": [], "properties": {}}
        mapfeatures[value["shape_id"]]["coordinates"].append([value["shape_pt_lon"], value["shape_pt_lat"]])
        print(index, "OK2")

    for key in mapfeatures.keys():
        feature = {}
        feature["type"] = "Feature"
        feature["geometry"] = {
            "type": "LineString",
            "coordinates": mapfeatures[key]["coordinates"]
        }
        feature["id"] = key

        feature["properties"] = mapfeatures[key]["properties"]

        features.append(feature)

        print(key, "OK3")

    geojson["features"] = features

    if(load_to_file != None):
        open(load_to_file, "w").write(json.dumps(geojson, indent="\t", sort_keys=False))

    return geojson


print(shapes_to_geojson(load_to_file = "works/testshapes.json"))

