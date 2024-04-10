from scipy.spatial import cKDTree
from netCDF4 import Dataset
from datetime import date
import pandas as pd
import numpy as np
import threading

def check_latlon_bounds(lat,lon,lat_index,lon_index,lat_target,lon_target):
    #check final indices are in right bounds
    if(lat[lat_index]>lat_target):
        if(lat_index!=0):
            lat_index = lat_index - 1
    if(lat[lat_index]<lat_target):
        if(lat_index!=len(lat)):
            lat_index = lat_index +1
    if(lon[lon_index]>lon_target):
        if(lon_index!=0):
            lon_index = lon_index - 1
    if(lon[lon_index]<lon_target):
        if(lon_index!=len(lon)):
            lon_index = lon_index + 1

    return [lat_index, lon_index]

def get_indexes(data, points):
    data_reshaped = data.filled().reshape(-1, 1)
    tree = cKDTree(data_reshaped)
    query_points = points.to_numpy().reshape(-1, 1)
    _, indexes = tree.query(query_points)

    return indexes

def get_data_by_date(varname, values, filehandle, time_values, lat_values, lon_values, year, lat_min, lon_min, lat_max, lon_max, lock):
    # subset in space (lat/lon)
    lathandle = filehandle.variables['lat']
    lonhandle = filehandle.variables['lon']
    lat=lathandle[:]
    lon=lonhandle[:]

    # find indices of target lat/lon/day
    lat_index_min = (np.abs(lat-lat_min)).argmin()
    lat_index_max = (np.abs(lat-lat_max)).argmin()
    lon_index_min = (np.abs(lon-lon_min)).argmin()
    lon_index_max = (np.abs(lon-lon_max)).argmin()

    [lat_index_min,lon_index_min] = check_latlon_bounds(lat, lon, lat_index_min, lon_index_min, lat_min, lon_min)
    [lat_index_max,lon_index_max] = check_latlon_bounds(lat, lon, lat_index_max, lon_index_max, lon_max, lon_max)

    if(lat_index_min>lat_index_max):
        lat_index_range = range(lat_index_max, lat_index_min+1)
    else:
        lat_index_range = range(lat_index_min, lat_index_max+1)
    if(lon_index_min>lon_index_max):
        lon_index_range = range(lon_index_max, lon_index_min+1)
    else:
        lon_index_range = range(lon_index_min, lon_index_max+1)

    lat=lat[lat_index_range]
    lon=lon[lon_index_range]

    # subset in time
    timehandle=filehandle.variables['time']
    time=timehandle[:]
    time_min = (date(year,1,1)-date(1900,1,1)).days
    time_max = (date(year,12,31)-date(1900,1,1)).days
    time_index_min = (np.abs(time-time_min)).argmin()
    time_index_max = (np.abs(time-time_max)).argmin()
    time_index_range = range(time_index_min, time_index_max+1)
    time = timehandle[time_index_range]

    # subset data
    datahandle = filehandle.variables[varname]
    data = datahandle[time_index_range, lat_index_range, lon_index_range]

    # Indexes
    time_indexes = get_indexes(time, time_values)
    lat_indexes = get_indexes(lat, lat_values)
    lon_indexes = get_indexes(lon, lon_values)

    with lock:
        values[varname] += list(data[time_indexes, lat_indexes, lon_indexes].filled(np.nan))

def get_data_country(df_modis, varnames, datasets):
  values = {varname: [] for varname in ["date", "latitude", "longitude"] + varnames}
  df_modis["date"] = pd.to_datetime(df_modis["date"])
  df_modis = df_modis.sort_values(by="date")
  lock = threading.Lock()

  for year in range(2002, 2024):
    df = df_modis[df_modis["date"] <= pd.to_datetime(f"{year}-12-31")]
    df = df[pd.to_datetime(f"{year}-01-01") <= df["date"]]

    date_values, lat_values, lon_values = df['date'], df['latitude'], df['longitude']
    lat_min, lon_min = lat_values.min(), lon_values.min()
    lat_max, lon_max = lat_values.max(), lon_values.max()

    values['date'] += [str(date_.date()) for date_ in date_values]
    values['latitude'] += list(lat_values.values)
    values['longitude'] += list(lon_values.values)
    time_values = (date_values - pd.to_datetime("1900-01-01")).dt.days

    threads = []
    for varname in varnames:
        filehandle = datasets[f"{year}-{varname}"]
        thread = threading.Thread(target=get_data_by_date, args=(varname, values, filehandle, time_values, lat_values, lon_values, year, lat_min, lon_min, lat_max, lon_max, lock))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    
  return values

def download_global_climate_dataset(varnames):
    datasets = {}
    for year in range(2002, 2024):
        for varname in varnames:
            pathname = f"http://thredds.northwestknowledge.net:8080/thredds/dodsC/TERRACLIMATE_ALL/data/TerraClimate_{varname}_{year}.nc"
            filehandle = Dataset(pathname, 'r', format="NETCDF4")
            datasets[f"{year}-{varname}"] = filehandle

    return datasets


def save_df_global_climate(root_directory_path, df_forest_fire):
    varnames = ["ws", "vpd", "vap", "tmin", "tmax", "swe", "srad", "soil", "q", "ppt", "pet", "def", "aet", "PDSI"]
    datasets = download_global_climate_dataset(varnames)
    values = get_data_country(df_forest_fire, varnames, datasets)
    df_global_climate = pd.DataFrame(values)
    for varname in varnames:
        df_global_climate[varname] = df_global_climate[varname].astype(float, copy=True)

    # Convertimos las temperaturas a kelvin
    kelvin = 273.15
    df_global_climate["tmin"] = df_global_climate["tmin"] + kelvin
    df_global_climate["tmax"] = df_global_climate["tmax"] + kelvin

    # Guardamos el dataset
    df_global_climate['date'] = pd.to_datetime(df_global_climate['date'])
    df_global_climate = df_global_climate.sort_values(by="date").dropna()
    df_global_climate.to_pickle(f"{root_directory_path}/global_climate.pkl")