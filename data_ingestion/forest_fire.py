from urllib import request
import pandas as pd
import zipfile
import os

# Download the data
def download_forest_fire_dataset(root_directory_path):
    elements = [
        ["https://firms.modaps.eosdis.nasa.gov/data/download/DL_FIRE_M-C61_443477.zip", 
        "forest_fire_Colombia.zip", "fire_archive_M-C61_443477.csv", "forest_fire_Colombia_2002_2023.csv", 
        ["Readme.txt", "fire_nrt_M-C61_443477.csv"]], 
        ["https://firms.modaps.eosdis.nasa.gov/data/download/DL_FIRE_SV-C2_446719.zip", 
        "forest_fire_Colombia.zip", "fire_archive_SV-C2_446719.csv", "forest_fire_Colombia_2012_2020.csv",
        ["Readme.txt"]]
    ]

    for element in elements:
        remote_url, local_file, remote_name, local_name, delete_files = element
        request.urlretrieve(remote_url, local_file)

        with zipfile.ZipFile(local_file, 'r') as zip_ref:
            zip_ref.extractall(root_directory_path)

        for delete_file in delete_files:
            os.remove(f"{root_directory_path}/{delete_file}")

        os.rename(f"{root_directory_path}/{remote_name}", f"{root_directory_path}/{local_name}")
        os.remove(local_file)

    return f"{root_directory_path}/{elements[0][3]}", f"{root_directory_path}/{elements[1][3]}"

# Unimos los datos
def union_forest_fire_data(local_csv_1, local_csv_2):
    df_ff_2002_2023 = pd.read_csv(local_csv_1, low_memory=False)
    df_ff_2012_2020 = pd.read_csv(local_csv_2, low_memory=False)
    df_forest_fire_merged = pd.concat([df_ff_2002_2023, df_ff_2012_2020])
    os.remove(local_csv_1)
    os.remove(local_csv_2)

    # Fixing data
    return pd.merge(
        df_forest_fire_merged.sort_values(by="acq_date")
            .rename(columns={"type": "fire_type", "acq_date": "date"})
            .dropna(),
        pd.DataFrame({
            "fire_type": [0, 1, 2, 3], 
            "type": ["presumed vegetation fire", "active volcano", "other static land source", "offshore"]
        }), on="fire_type", how="left").drop(columns=["fire_type"])

def get_df_forest_fire(root_directory_path):
    local_csv_1, local_csv_2 = download_forest_fire_dataset(root_directory_path)
    df_forest_fire = union_forest_fire_data(local_csv_1, local_csv_2)
    df_forest_fire.to_pickle(f"{root_directory_path}/forest_fire.pkl")
    df_forest_fire['date'] = pd.to_datetime(df_forest_fire['date'])

    return df_forest_fire