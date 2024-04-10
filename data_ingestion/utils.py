import os

def get_root_directory():
    root_directory_path = "../datasets"

    if not os.path.exists(root_directory_path):
        os.makedirs(root_directory_path)

    return root_directory_path

def get_lat_lon_values(df_forest_fire):
    lat_values, lon_values = df_forest_fire['latitude'].to_numpy(), df_forest_fire['longitude'].to_numpy()
    lat_lon_values = list(set([(lat, lon) for lat, lon in zip(lat_values, lon_values)]))

    return lat_lon_values

def get_inter_extrapolated_values(df_forest_fire):
    df_forest_fire['year'] = df_forest_fire['date'].astype(str).str.slice(start=0, stop=4).astype(int)
    interpolated_values = df_forest_fire[df_forest_fire['year'] <= 2020][['latitude', 'longitude', 'year']].values
    extrapolated_values = df_forest_fire[2021 <= df_forest_fire['year']][['latitude', 'longitude', 'year']].values

    return interpolated_values, extrapolated_values