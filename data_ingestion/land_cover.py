from utils import get_lat_lon_values, get_inter_extrapolated_values
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from pyproj import Transformer
from urllib import request
import pandas as pd
import numpy as np
import threading
import rasterio
import os

def download_land_cover_dataset(root_directory_path):
    range_values = [(20, 80), (10, 80), (10, 70), ('00', 80), ('00', 70)]
    for year in range(2000, 2021, 5):
        for N, W in range_values:
            remote_url = f"https://storage.googleapis.com/earthenginepartners-hansen/GLCLU2000-2020/v2/{year}/{N}N_0{W}W.tif"
            land_cover_path = f"{root_directory_path}/land_cover_Colombia_{year}_{N}N_0{W}W.tif"
            request.urlretrieve(remote_url, land_cover_path)

    return range_values

def get_land_cover(lat_lon_array, land_cover_path):
    with rasterio.open(land_cover_path) as src:
        transform = src.transform
        tif_crs = src.crs
        transformer = Transformer.from_crs("epsg:4326", tif_crs, always_xy=True)
        lon_values, lat_values = lat_lon_array[:,1], lat_lon_array[:,0]
        x_coords, y_coords = transformer.transform(lon_values, lat_values)
        row, col = rasterio.transform.rowcol(transform, x_coords, y_coords)
        values = src.read(1)[row, col]

    return lat_values, lon_values, values

def save_land_cover_values(values, name, root_directory_path):
    land_cover_values = {'lat': [], 'lon': [], 'year': [], 'land_cover': []}
    lat_lon_array = np.array(values)
    for year in range(2000, 2021, 5):
        land_cover_path = f"{root_directory_path}/land_cover_Colombia_{year}_{name}.tif"
        lat_values, lon_values, result = get_land_cover(lat_lon_array, land_cover_path)

        land_cover_values['lat'] += list(lat_values)
        land_cover_values['lon'] += list(lon_values)
        land_cover_values['year'] += list(np.full(len(lat_values), year))
        land_cover_values['land_cover'] += list(result)

        os.remove(f"{root_directory_path}/land_cover_Colombia_{year}_{name}.tif")

    df = pd.DataFrame(land_cover_values)
    df.to_csv(f"{root_directory_path}/land_cover_Colombia_{name}.csv", index=False)

def split_lat_lon_values(lat_lon_values):
    lat_lon_values_split = [
        ((10, 20), (-80, -70), "00N_070W"),
        ((0, 10), (-80, -70), "00N_080W"),
        ((0, 10), (-70, -60), "10N_070W"),
        ((-10, 0), (-80, -70), "20N_080W"),
        ((-10, 0), (-70, -60), "10N_080W")
    ]
    threads = []
    for value in lat_lon_values_split:
        (lat_min, lat_max), (lon_min, lon_max), name = value
        lat_lon_values_filtered = list(filter(lambda lat_lon: lat_min < lat_lon[0] <= lat_max and lon_min <= lat_lon[1] < lon_max, lat_lon_values))
        thread = threading.Thread(target=save_land_cover_values, args=(lat_lon_values_filtered, name))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def union_land_cover_data(range_values, root_directory_path):
    df_land_cover = pd.DataFrame()
    for N, W in range_values:
        land_cover_csv_path = f"{root_directory_path}/land_cover_Colombia_{N}N_0{W}W.csv"
        df_tlc = pd.read_csv(land_cover_csv_path)
        df_land_cover = pd.concat([df_land_cover, df_tlc])
        os.remove(f"{root_directory_path}/land_cover_Colombia_{N}N_0{W}W.csv")

    return df_land_cover.sort_values(by="year").dropna()

def get_model(df_land_cover):
    X = df_land_cover[['lat', 'lon', 'year']].values
    y = df_land_cover['land_cover'].values

    X_train, _, y_train, _ = train_test_split(X, y, test_size=0.2, random_state=42)

    model = DecisionTreeRegressor(max_depth=30, random_state=42)
    model.fit(X_train, y_train)

    return model

def save_df_land_cover_predicted(root_directory_path, model, interpolated_values, extrapolated_values):
    land_covers_interpolated = model.predict(interpolated_values)
    land_covers_extrapolated = model.predict(extrapolated_values)
    
    df_land_cover_predicted = pd.DataFrame({
        'latitude': np.append(interpolated_values[:, 0], extrapolated_values[:, 0]),
        'longitude': np.append(interpolated_values[:, 1], extrapolated_values[:, 1]),
        'year': np.append(interpolated_values[:, 2], extrapolated_values[:, 2]).astype(int),
        'land_cover': np.append(land_covers_interpolated, land_covers_extrapolated).astype(int)
    }).sort_values(by="year")
    df_land_cover_predicted.to_pickle(f"{root_directory_path}/land_cover.pkl")

def download_land_cover_legend(root_directory_path):
    remote_url = f"https://storage.googleapis.com/earthenginepartners-hansen/GLCLU2000-2020/legend.xlsx"
    land_cover_legend_path = f"{root_directory_path}/land_cover_legend_Colombia.xlsx"
    request.urlretrieve(remote_url, land_cover_legend_path)

    return land_cover_legend_path

def set_values(df_land_cover_legend, column1, column2, indexes, nan_indexes=[]):
    for start_index, end_index in indexes:
        total = end_index - start_index + 1
        df_land_cover_legend.loc[np.linspace(start_index, end_index, total), column1] = df_land_cover_legend.at[start_index, column2]

    for nan_index in nan_indexes:
        df_land_cover_legend.at[nan_index, column1] = np.NAN

def save_df_land_cover_legend(root_directory_path, land_cover_legend_path):
    df_land_cover_legend = pd.read_excel(land_cover_legend_path)
    df_land_cover_legend = df_land_cover_legend.drop(columns={"Color code"}).rename(columns={'Unnamed: 2': 'class'})

    # Same column
    set_values(df_land_cover_legend, 'General class', 'General class', [(0, 96), (100, 196), (200, 207)], [97, 197, 208, 242, 245, 251, 255])
    set_values(df_land_cover_legend, 'class', 'class', [(0, 1), (2, 18), (19, 24), (25, 48), (100, 101), (102, 118), (119, 124), (125, 148)], [49, 149])

    # Other column
    set_values(df_land_cover_legend, 'class', 'General class', [(200, 207), (241, 241), (244, 244), (250, 250), (254, 254)])
    set_values(df_land_cover_legend, 'Sub-class', 'General class', [(241, 241), (244, 244), (250, 250), (254, 254)])

    # Replacing nan values
    df_land_cover_legend = df_land_cover_legend.fillna("Not registered")
    df_land_cover_legend.to_pickle(f"{root_directory_path}/land_cover_legend.pkl")
    os.remove(land_cover_legend_path)

def save_df_land_cover(root_directory_path, df_forest_fire):
    interpolated_values, extrapolated_values = get_inter_extrapolated_values(df_forest_fire)
    lat_lon_values = get_lat_lon_values(df_forest_fire)

    range_values = download_land_cover_dataset(range_values, root_directory_path)
    split_lat_lon_values(lat_lon_values)
    
    df_land_cover = union_land_cover_data(range_values, root_directory_path)
    land_cover_legend_path = download_land_cover_legend(root_directory_path)
    model = get_model(df_land_cover)
    
    save_df_land_cover_predicted(root_directory_path, model, interpolated_values, extrapolated_values)
    save_df_land_cover_legend(root_directory_path, land_cover_legend_path)