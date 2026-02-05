from rasterio.mask import mask
import rasterio
import cv2
import ijson
import numpy as np
from shapely.geometry import Polygon
import shapely
import datetime
from pyproj import Transformer
from rasterio.enums import Resampling
from shapely.ops import transform as sh_transform
import pyproj
from rasterio.transform import rowcol, from_origin
from functools import partial
from rasterio.windows import Window
from datetime import datetime, timedelta
import ray
import time
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)


wgs84_globe = pyproj.Proj(proj='latlong', ellps='WGS84')

width = 11496 / 30
height = 14064 / 30


def fetch_adjusted_polygon(src, coords):
    transformer = Transformer.from_crs('EPSG:4326', src.crs, always_xy=True)

    for index, coord in enumerate(coords):
        coords[index] = transformer.transform(coord[0], coord[1])

    return Polygon(coords)


def fetch_file_name(properties):
    mtbs_id = properties['mtbs_id']
    date = properties['date']
    file_name = f"{mtbs_id}_{date}"
    return file_name, date[:4]

@ray.remote
def fetch_fire_shape(o):
    properties = o['properties']
    coords = o['geometry']['coordinates']

    if coords == [[[]]]: 
        pass
    else:
        try: 
            file_name, year = fetch_file_name(properties)
            print(file_name)

            with rasterio.open(f"output/fuel_grid_{year}.tif", "r") as src:

                polygon = fetch_adjusted_polygon(src, coords[0])
                centroid = shapely.centroid(pol)

                np_mask, out_transform = mask(src, [polygon], nodata=0, filled=True, crop=True)
                meta = src.meta.copy()
                meta["height"] = height
                meta["width"] = width
                meta["transform"] = out_transform

                with rasterio.open(f"test_images/{file_name}.tif", "w", **meta) as result:
                    result.write(np_mask)      

        except ValueError:
            with open("failed.txt", "a") as file_object:
                file_object.write(file_name + "\n")
            return 0
            
    return 0



if __name__ == "__main__":
    ray.init()
    f = open('filtered_160_km_fires.geojson')
    objects = ijson.items(f, 'features.item')
    
    count = 0
    o_2_go = []
    start_time = time.perf_counter()

    for o in objects:
        o_2_go.append(o)
        count += 1
        if count >= 20: #batch 20 ray tasks at a time
            count = 0
            remotes = [fetch_fire_shape.remote(fire) for fire in o_2_go]
            results = ray.get(remotes)
            o_2_go = []
    remotes = [fetch_fire_shape.remote(fire) for fire in o_2_go]
    results = ray.get(remotes)

    ray.shutdown()

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    print(f"Elapsed time: {elapsed_time:.4f} seconds")
