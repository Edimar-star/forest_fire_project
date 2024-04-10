from geopy.geocoders import Nominatim
from scipy.spatial import cKDTree
from urllib import request
import pandas as pd
import numpy as np
import threading
import math
import os

def download_ndvi_dataset(root_directory_path):
    remote_url = "https://data.humdata.org/dataset/7f2ba5ba-8df1-41cf-ab18-fc1da928a1e5/resource/c06298d9-0d4d-4e40-aecc-abc1da75dc4d/download/col-ndvi-adm2-full.csv"
    local_file_ndvi_dataset = f"{root_directory_path}/ndvi_Colombia.csv"
    request.urlretrieve(remote_url, local_file_ndvi_dataset)

    df_ndvi = pd.read_csv(local_file_ndvi_dataset, low_memory=False)
    df_ndvi = df_ndvi.drop(df_ndvi.index[0])
    df_ndvi['date'] = pd.to_datetime(df_ndvi['date'])
    df_ndvi = df_ndvi[df_ndvi['date'] <= pd.to_datetime("2023-12-31")]
    os.remove(local_file_ndvi_dataset)

    return df_ndvi

def get_ndvi_postal_codes(df_ndvi, root_directory_path):
    postal_codes = list(set(df_ndvi["ADM2_PCODE"].values.astype('str')))
    postal_codes = np.array([postal_code.replace("CO", "") for postal_code in postal_codes]).astype(int)
    
    # postal codes
    remote_url = "https://www.datos.gov.co/api/views/ixig-z8b5/rows.csv?accessType=DOWNLOAD"
    postal_codes_path = f"{root_directory_path}/postal_codes.csv"
    request.urlretrieve(remote_url, postal_codes_path)

    column_name = "codigo_municipio"
    df_postal_codes = pd.read_csv(postal_codes_path)

    df_postal_codes[column_name] = df_postal_codes[column_name].replace(',', '').astype(int)
    df_postal_codes = df_postal_codes.drop_duplicates(subset=column_name, keep='first')

    result = df_postal_codes[df_postal_codes[column_name].isin(postal_codes)][['nombre_departamento', 'nombre_municipio', 'codigo_municipio', 'codigo_postal']]
    result.reset_index(drop=True, inplace=True)
    result.sort_values(by="codigo_postal")
    os.remove(postal_codes_path)

    return result

def get_ndvi_lat_lon(geolocator, location):
    try:
        lat_lon_result = geolocator.geocode(location)
        return (lat_lon_result.latitude, lat_lon_result.longitude) if lat_lon_result else (None, None)
    except:
        return (None, None)

def set_ndvi_by_values(geolocator, locations, lat_lon_ndvi, municipality_fixed, lock, index):
    for municipality, department in locations:
        municipality = municipality_fixed[municipality] if municipality in municipality_fixed else municipality
        location = get_ndvi_lat_lon(geolocator, f"{municipality}, {department}, COLOMBIA")
        with lock:
            lat_lon_ndvi[index] = np.array(location)
            index += 1

def get_ndvi_lat_lon_values(result):
    geolocator = Nominatim(user_agent="ndvi_data")
    tam = int(result.size / result.columns.size)
    lat_lon_ndvi = np.zeros((tam, 2))
    lock = threading.Lock()
    num_threads = 10
    threads = []

    municipality_fixed = {
        "VILLA DE SAN DIEGO DE UBATE": "UBATE",
        "CERRO SAN ANTONIO": "SAN ANTONIO",
        "SAN JUAN DE RIO SECO": "SAN JUAN DE RIOSECO",
        "TOLU VIEJO": "TOLUVIEJO",
        "SAN ANDRES DE TUMACO": "TUMACO",
        "EL CANTON DEL SAN PABLO": "EL CANTON DE SAN PABLO",
        "SAN LUIS DE SINCE": "SINCE",
        "SAN JOSE DE ALBAN": "ALBAN"
    }

    values = result[['nombre_municipio', 'nombre_departamento']].values
    total = math.ceil(len(values) / num_threads)
    for index in range(num_threads):
        start_index = index * total
        end_index = min(len(values), start_index + total)
        thread = threading.Thread(target=set_ndvi_by_values, args=(geolocator, values[start_index:end_index], lat_lon_ndvi, municipality_fixed, lock, start_index))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return lat_lon_ndvi

def union_ndvi_data(df_ndvi, result, lat_lon_ndvi):
    result['latitude'] = lat_lon_ndvi[:, 0]
    result['longitude'] = lat_lon_ndvi[:, 1]

    result.rename(columns={'codigo_municipio': 'ADM2_PCODE'}, inplace=True)
    result['ADM2_PCODE'] = 'CO' + result['ADM2_PCODE'].astype(str)
    merged_df_ndvi = pd.merge(df_ndvi, result[['latitude', 'longitude', 'ADM2_PCODE']], on='ADM2_PCODE', how='left')
    merged_df_ndvi = merged_df_ndvi.drop(columns={'adm2_id', 'ADM2_PCODE'})

    return merged_df_ndvi

def collect_ndvi_data(root_directory_path, merged_df_ndvi, df_forest_fire):
    values = {key: [] for key in merged_df_ndvi.columns}
    merged_df_ndvi = merged_df_ndvi.dropna()

    for year in range(2002, 2024):
        # Filtramos los datos
        date_min, date_max = pd.to_datetime(f'{year}-01-01'), pd.to_datetime(f'{year}-12-31')
        df_ndvi_temp = merged_df_ndvi[(date_min <= merged_df_ndvi['date']) & (merged_df_ndvi['date'] <= date_max)]
        df_forest_fire_temp = df_forest_fire[(date_min <= df_forest_fire['date']) & (df_forest_fire['date'] <= date_max)]

        df_ndvi_temp.reset_index(drop=True, inplace=True)
        df_forest_fire_temp.reset_index(drop=True, inplace=True)
        init_date = pd.to_datetime(f'{year}-01-01')

        # forest fire values
        lat_values = df_forest_fire_temp['latitude'].values
        lon_values = df_forest_fire_temp['longitude'].values
        time_values = (df_forest_fire_temp['date'] - init_date).dt.days.values

        # ndvi values
        lat = df_ndvi_temp['latitude'].values
        lon = df_ndvi_temp['longitude'].values
        time = (df_ndvi_temp['date'] - init_date).dt.days.values

        points = np.vstack((lat, lon, time)).T
        tree = cKDTree(points)
        query_points = np.vstack((lat_values, lon_values, time_values)).T
        _, indexes = tree.query(query_points)

        for key in ['latitude', 'longitude', 'date']:
            values[key] += list(df_forest_fire_temp[key].values)

        for key in ['n_pixels', 'vim', 'vim_avg', 'viq']:
            values[key] += list(df_ndvi_temp.iloc[indexes][key].values)

    df_ndvi = pd.DataFrame(values).sort_values(by="date").dropna()
    df_ndvi.to_pickle(f"{root_directory_path}/ndvi.pkl")

def save_df_ndvi(root_directory_path, df_forest_fire):
    df_ndvi = download_ndvi_dataset(root_directory_path)
    result = get_ndvi_postal_codes(df_ndvi, root_directory_path)
    
    lat_lon_ndvi = get_ndvi_lat_lon_values(result)
    merged_df_ndvi = union_ndvi_data(df_ndvi, result, lat_lon_ndvi)
    
    collect_ndvi_data(root_directory_path, merged_df_ndvi, df_forest_fire)