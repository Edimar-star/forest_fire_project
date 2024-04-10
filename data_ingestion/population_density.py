from utils import get_inter_extrapolated_values
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from scipy.spatial import cKDTree
from urllib import request
import pandas as pd
import numpy as np
import zipfile
import os

def download_population_density_dataset(root_directory_path):
    for year in range(2002, 2021):
        remote_url = f"https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km/{year}/COL/col_pd_{year}_1km_ASCII_XYZ.zip"
        local_file = f"{root_directory_path}/population_density_Colombia_{year}.zip"
        request.urlretrieve(remote_url, local_file)

        with zipfile.ZipFile(local_file, 'r') as zip_ref:
            zip_ref.extractall(root_directory_path)
        os.remove(local_file)

def save_data(root_directory_path, df_forest_fire):
    for year in range(2002, 2021):
        pd_path = f"{root_directory_path}/col_pd_{year}_1km_ASCII_XYZ.csv"
        if os.path.exists(pd_path):
            df_pd = pd.read_csv(pd_path)
            df_pd.rename(columns={'X': 'longitude', 'Y': 'latitude', 'Z': 'population_density'}, inplace=True)

            # Filtramos por fecha
            date_min, date_max = pd.to_datetime(f"{year}"), pd.to_datetime(f"{year + 1}")
            df_ff = df_forest_fire[(date_min <= df_forest_fire['date']) & (df_forest_fire['date'] < date_max)]
            lat_values, lon_values = df_ff['latitude'], df_ff['longitude']

            # Minimos
            lat_min, lat_max = lat_values.min(), lat_values.max()
            lon_min, lon_max = lon_values.min(), lon_values.max()

            # Filtramos las latitudes
            df_pd.sort_values(by="latitude")
            df_pd = df_pd[lat_min <= df_pd['latitude']]
            df_pd = df_pd[df_pd['latitude'] <= lat_max]

            # Filtramos las longitudes
            df_pd.sort_values(by="longitude")
            df_pd = df_pd[lon_min <= df_pd['longitude']]
            df_pd = df_pd[df_pd['longitude'] <= lon_max]

            # Establecemos valores
            df_pd.reset_index(drop=True, inplace=True)
            lat, lon = df_pd['latitude'].to_numpy(), df_pd['longitude'].to_numpy()

            # Hallamos los valores
            points = np.vstack((lat, lon)).T
            tree = cKDTree(points)
            query_points = np.vstack((lat_values, lon_values)).T
            _, indices = tree.query(query_points)
            population_density_values = df_pd.iloc[indices]['population_density'].to_numpy()

            # Guardamos los datos
            df_population_density = pd.DataFrame({'latitude': lat_values, 'longitude': lon_values,
                                        'year': np.full(len(lat_values), year), 'population_density': population_density_values})
            df_population_density.to_csv(f"{root_directory_path}/population_density_Colombia_{year}.csv", index=False)
            os.remove(pd_path)

def union_data(root_directory_path):
    df_population_density = pd.DataFrame()
    for year in range(2002, 2021):
        df_ppd = pd.read_csv(f"{root_directory_path}/population_density_Colombia_{year}.csv")
        df_population_density = pd.concat([df_population_density, df_ppd])
        os.remove(f"{root_directory_path}/population_density_Colombia_{year}.csv")

    return df_population_density.sort_values(by="year").dropna()

def get_model(df_population_density):
    X = df_population_density[['latitude', 'longitude', 'year']].values  # Características: año, latitud y longitud
    y = df_population_density['population_density'].values             # Densidad de población como variable dependiente

    X_train, _, y_train, _ = train_test_split(X, y, test_size=0.3, random_state=42)

    regressor = DecisionTreeRegressor(max_depth=20, random_state=42)
    regressor.fit(X_train, y_train)

    return regressor

def save_df_population_density_predicted(root_directory_path, df_population_density, regressor, extrapolated_values):
    densities_predicted = regressor.predict(extrapolated_values)
    df_pd_predicted = pd.DataFrame({
        'latitude': extrapolated_values[:, 0],
        'longitude': extrapolated_values[:, 1],
        'year': extrapolated_values[:, 2].astype(int),
        'population_density': densities_predicted
    })

    df_pd_predicted = pd.concat([df_population_density, df_pd_predicted]).sort_values(by="year")
    df_pd_predicted.to_pickle(f"{root_directory_path}/population_density.pkl")

def save_df_population_density(root_directory_path, df_forest_fire):
    download_population_density_dataset(root_directory_path)
    save_data(root_directory_path, df_forest_fire)

    _, extrapolated_values = get_inter_extrapolated_values(df_forest_fire)
    df_population_density = union_data(root_directory_path)
    regressor = get_model(df_population_density)
    
    save_df_population_density_predicted(root_directory_path, df_population_density, regressor, extrapolated_values)
