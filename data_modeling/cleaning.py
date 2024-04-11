from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sqlalchemy import text
import pandas as pd
import psycopg2
import os

def create_connection():
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT')
    POSTGRES_DB = os.getenv('POSTGRES_DB')

    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')

    Session = sessionmaker(bind=engine)
    session = Session()

    return engine, session

def get_data(session):
    sql_query = text("""
    WITH global_climate_and_forest_fire AS (
        SELECT * FROM global_climate 
        JOIN forest_fire USING(latitude, longitude, \"date\")
    ), 
    global_climate_and_forest_fire_and_ndvi AS (
        SELECT * FROM global_climate_and_forest_fire 
        JOIN ndvi USING(latitude, longitude, \"date\")
    ), land_cover_data AS (
        SELECT lc.latitude, lc.longitude, lc."year", 
        lcl."General class", lcl."class", lcl."Sub-class"
        FROM land_cover lc JOIN land_cover_legend lcl
        ON lc.land_cover = lcl."Map value"
    ),
    population_density_and_land_cover AS (
        SELECT * FROM population_density 
        JOIN land_cover_data USING(latitude, longitude, \"year\")
    ),
    population_density_and_land_cover_and_dem AS (
        SELECT * FROM dem
        JOIN population_density_and_land_cover USING(latitude, longitude)
    ), 
    all_data AS (
        SELECT DISTINCT * FROM population_density_and_land_cover_and_dem pd_lc_dm
        JOIN global_climate_and_forest_fire_and_ndvi gc_ff_nd
        USING(latitude, longitude) 
        WHERE pd_lc_dm.\"year\" = EXTRACT(YEAR FROM gc_ff_nd.\"date\")
    )

    SELECT * FROM all_data;
    """)
    
    return session.execute(sql_query)

def query_to_dataframe(data):
    return pd.DataFrame(data.fetchall(), columns=data.keys())

def change_column_names(df):
    columns = {
        "latitude": "latitude", "longitude": "longitude", "population_density": "population_density",
        "General class": "land_cover_type", "class": "land_cover_subtype", "Sub-class": "vegetation_percent",
        "date": "date", "ws": "win_speed", "vpd": "vapor_pressure_deficit", "vap": "vapor_pressure",
        "tmin": "minimum_temperature", "tmax": "maximum_temperature", "swe": "snow_water_equivalent",
        "srad": "surface_shortwave_radiation", "soil": "soil_moisture", "q": "runoff", "ppt": "precipitation_accumulation",
        "pet": "Reference_evapotranspiration", "def": "climate_water_deficit", "aet": "actual_Evapotranspiration",
        "PDSI": "palmer_drought_severity_index", "brightness": "brightness_temperature", "bright_t31": "brightness_temperature_31",
        "scan": "scan_fire_size", "track": "track_fire_size", "confidence": "confidence", "frp": "fire_radiative_power",
        "daynight": "daynight", "type": "fire_type", "n_pixels": "n_pixels_ndvi", "vim": "ndvi",
        "vim_avg": "ndvi_long_term_average", "viq": "ndvi_anomaly_percent"
    }

    return df.rename(columns=columns)[columns.values()]

def close_connection(session, engine):
    session.close()
    engine.dispose()

if __name__ == "__main__":
    load_dotenv()
    session, engine = create_connection()
    file_path = "../datasets/final_dataset.pkl"

    data = get_data(session)
    df = query_to_dataframe(data)
    df = change_column_names(df)
    df.to_pickle(file_path)

    close_connection(session, engine)
