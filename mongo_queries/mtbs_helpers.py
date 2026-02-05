import datetime
import atexit
import os
from pymongo.mongo_client import MongoClient
from scipy.spatial import ConvexHull
import pandas as pd
import numpy as np
import sys


disc_doy = "discovery_doy"
cont_doy = "cont_doy"
fire_year = "fire_year"

MONGO_URI = "mongodb://lattice-156:27018"
DATABASE = "sustaindb"
USERNAME = os.getenv("READ_MONGO_USER")
PASSWORD = os.getenv("READ_MONGO_PASS")

NASA_MIN_DOY = 305
NASA_MIN_YEAR = 2001

client = MongoClient(MONGO_URI, username=USERNAME, password=PASSWORD)
db = client[DATABASE]


def match_nfs_mtbs(fire):
    id = fire["wildfire_id"]
    pipeline = [
        {
            "$match": { 
                "mtbs_id": id
            }
        }
    ]

    return list(NFS_COLLECTION.aggregate(pipeline))


# ----- FROM DATE_FORMATTERS -----
def calc_day(day, year):
    return datetime.datetime.strftime(
        (datetime.datetime(year, 1, 1) + datetime.timedelta(day - 1)), "%Y-%m-%d"
    )

def add_day_offset(fire, day_offset):
    start = int(fire[disc_doy])
    end = int(fire[cont_doy])
    year = int(fire[fire_year])
    curr_year = 0

    curr_day = start + day_offset

    if curr_day < 0:
        curr_day = 365 + curr_day
        curr_year = year - 1
    elif curr_day > 365:
        curr_day = curr_day - 365
        curr_year = year + 1
    else:
        curr_year = year

    date = calc_day(curr_day, curr_year)
    return date

def burning_days(fire):
    start, end, days = calc_end_start_fires(fire)
    for day in range(end - start + 1):
        days.append(add_day_offset(fire, day))
    return days


def calc_end_start_fires(fire):
    start = int(fire[disc_doy])
    end = int(fire[cont_doy])
    if end < start:
        end = 365 + end
    return start, end, []

    # ----- FROM WILDFIRE_SHAPES -----

def query_nfs(fire):
    pipeline = [
        {
            "$match": { 
                "mtbs_id": fire,
                "fire_year": { "$gte": 2001, "$lte": 2020 }
            }
        }
    ]

    return list(NFS_COLLECTION.aggregate(pipeline))


def format_geometries(geometries):
    geo_string = ""
    for geometry in geometries:
        geo_string += '{"geometry": { "type": "MultiPolygon", "coordinates": [[' + f"{geometry}" + ']]}},'
    return geo_string[:-1]

def calculate_shapes(nasa_matches):
    lat_longs = []
    for document in nasa_matches:
        lat_longs.append(document["geometry"]["coordinates"])
    if len(lat_longs) == 0:
        return []
    else:
        lat_longs_np = np.array(lat_longs)
        simplified_shape = ConvexHull(lat_longs_np)
        return [[lat_longs_np[index, 0], lat_longs_np[index, 1]] for index in simplified_shape.vertices]

def format_as_geojsonobject(val):
    id, fire = val
    fire_formatted = '{"type": "Feature", "properties\": {' \
        f"\"mtbs_id\": \"{id}\", \"start_date\": \"{fire['discovery_doy']}\", " \
        f"\"end_date\": \"{fire['cont_doy']}\", \"fire_size\": \"{fire['fire_size']}\", " \
        f"\"fire_size_class\": \"{fire['fire_size_class']}\"" \
        '}, "geometries": [' \
        f"{fire['geometries']}]" \
        "},"
    return ("\n", fire_formatted)


def query_shapes(points, date): #returns points that appear in given shape on given day
    # logic from aqi geowithin queries
    pipeline = [
        {
            "$match": {
                "geometry": {
                    "$geoWithin": {
                        "$geometry": {
                            "type": "MultiPolygon",
                            "coordinates": points
                        }
                    }
                },
                "curr_date": date
            }
        }
    ]

    return list(NASA_COLLECTION.aggregate(pipeline))




atexit.register(client.close)