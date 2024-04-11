from utils import get_lat_lon_values
from scipy.spatial import cKDTree
from urllib import request
import pandas as pd
import numpy as np
import zipfile
import math
import os

def download_hgt_file(polygon, root_directory_path):
    file_name = f"{root_directory_path}/file_{polygon}.zip"
    url = f"https://opendap.cr.usgs.gov/opendap/hyrax/DP109/SRTM/SRTMGL1.003/2000.02.11/{polygon}.SRTMGL1.hgt.zip"
    request.urlretrieve(url, file_name)

    with zipfile.ZipFile(file_name, 'r') as zip_ref:
        zip_ref.extractall(root_directory_path)

    os.remove(file_name)
    
    return f"{root_directory_path}/{polygon}.hgt"

def read_hgt_file(file_name):
    with open(file_name, 'rb') as src:
        content = src.read()
        elevations = np.frombuffer(content, np.int16).reshape((3601, 3601))
        
    os.remove(file_name)    
    return elevations

def get_lat_lon_bounds(latitudes, longitudes):
    lat_min, lon_min = latitudes.min(), longitudes.min()
    lat_max, lon_max = latitudes.max(), longitudes.max()

    lat_min, lon_min = math.floor(lat_min), math.floor(lon_min)
    lat_max, lon_max = math.ceil(lat_max), math.floor(lon_max)

    return (lat_min, lon_min), (lat_max, lon_max)

def get_geo_values(lat_min, lon_min, total, arc_length=3600):
    lat_values = lat_min + np.linspace(0, total) / arc_length
    lon_values = lon_min + np.linspace(0, total) / arc_length

    return lat_values, lon_values

def get_changes(polygon, root_directory_path, lat_min, lon_min, latitudes, longitudes, min_distances, values):
    try:
        hgt_file_name = download_hgt_file(polygon, root_directory_path)
        elevations = read_hgt_file(hgt_file_name)
        lat, lon = get_geo_values(lat_min, lon_min, len(elevations))
        distances, indexes = get_indexes(lat, lon, latitudes, longitudes)
        result_indexes = np.where(distances < min_distances)[0]
        values[result_indexes] = elevations[indexes][result_indexes]
        min_distances[result_indexes] = distances[result_indexes]
    finally:
        return min_distances, values

def download_dem_data(root_directory_path, latitudes, longitudes, min_bounds, max_bounds):
    lat_min, lon_min = min_bounds
    lat_max, lon_max = max_bounds

    values = []
    min_distances = np.full(len(latitudes), np.inf)
    for i in range(lat_max - lat_min + 1):
        for j in range(lon_max - lon_min + 1):
            r = "S0" if lat_min + i < 0 else "N0" if lat_min + i < 10 else "N"
            polygon = f"{r}{abs(lat_min + i)}W0{abs(lon_min + j)}"
            min_distances, values = get_changes(polygon, root_directory_path, lat_min + i, lon_min + j, latitudes, longitudes, min_distances, values)

    return values

def get_indexes(lat, lon, lat_values, lon_values):
    points = np.vstack((lat, lon)).T
    tree = cKDTree(points)
    query_points = np.vstack((lat_values, lon_values)).T
    distances, indexes = tree.query(query_points)

    return distances, indexes

def get_minimum_differences(root_directory_path, df_forest_fire):
    lat_lon_values = np.array(get_lat_lon_values(df_forest_fire))
    latitudes, longitudes = lat_lon_values[:, 0], lat_lon_values[:, 1]

    min_bounds, max_bounds = get_lat_lon_bounds(latitudes, longitudes)
    values = download_dem_data(root_directory_path, latitudes, longitudes, min_bounds, max_bounds)

    return latitudes, longitudes, values

def save_df_dem(root_directory_path, df_forest_fire):
    latitudes, longitudes, values = get_minimum_differences(root_directory_path, df_forest_fire)
    df_dem = pd.DataFrame({'latitude': latitudes, 'longitude': longitudes, 'dem': values})
    df_dem.to_pickle(f"{root_directory_path}/dem.pkl")
