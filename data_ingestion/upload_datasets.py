from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from utils import get_root_directory
from dotenv import load_dotenv
from sqlalchemy import text
import pandas as pd
import psycopg2
import os

def create_instance():
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT')
    POSTGRES_DB = os.getenv('POSTGRES_DB')
    
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    
    Session = sessionmaker(bind=engine)
    session = Session()

    return engine, session

def execute_query(query, session):
    sql_query = text(query)
    session.execute(sql_query)
    session.commit()

def upload_datasets(root_directory_path, results):
    engine, session = create_instance()

    for name in results:
        df = pd.read_pickle(f"{root_directory_path}/{name}.pkl")
        df.to_sql(name, engine, if_exists='replace', index=False, method='multi', chunksize=1000)

    querys = [
        "ALTER TABLE forest_fire ALTER COLUMN date TYPE DATE USING date::date;",
        "ALTER TABLE ndvi ALTER COLUMN date TYPE DATE USING date::date;",
        "ALTER TABLE global_climate ALTER COLUMN date TYPE DATE USING date::date;"
    ]
    for query in querys:
        execute_query(query, session)

    session.close()
    engine.dispose()

if __name__ == "__main__":
    load_dotenv()
    root_directory_path = get_root_directory()
    results = ["forest_fire", "ndvi", "global_climate", "land_cover", "land_cover_legend", "population_density"]
    upload_datasets(root_directory_path, results)