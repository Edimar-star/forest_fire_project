from population_density import save_df_population_density
from global_climate import save_df_global_climate
from forest_fire import get_df_forest_fire
from land_cover import save_df_land_cover
from utils import get_root_directory
from ndvi import save_df_ndvi
import multiprocessing

if __name__ == "__main__":
    total_time = 0
    root_directory_path = get_root_directory()
    df_forest_fire = get_df_forest_fire(root_directory_path)
    datasets = [save_df_ndvi, save_df_global_climate, save_df_land_cover, save_df_population_density]
    
    processes = []
    for save_df in datasets:
        process = multiprocessing.Process(target=save_df, args=(root_directory_path, df_forest_fire))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()