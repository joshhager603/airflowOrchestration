import psycopg2
from psycopg2 import sql
import pandas as pd
from scripts.constants import *
from scripts.common_functions import create_sqlalchemy_engine

def load_raw_data():
    try:

        # create a SQLAlchemy engine to execute our queries
        engine = create_sqlalchemy_engine()
        connection = engine.raw_connection()
        connection.autocommit = True
        cursor = connection.cursor()

        # create the table for our raw data
        # using VARCHAR for all data to preserve raw data with its imperfections
        create_table_statement = f'''
        CREATE TABLE IF NOT EXISTS {RAW_DATA_TABLE_NAME} (
            employee_id VARCHAR(500),
            name VARCHAR(500),
            age VARCHAR(500),
            department VARCHAR(500),
            date_of_joining VARCHAR(500),
            years_of_experience VARCHAR(500),
            country VARCHAR(500),
            salary VARCHAR(500),
            performance_rating VARCHAR(500)
        );
        '''

        cursor.execute(create_table_statement)
        connection.commit()
        print(f"Table {RAW_DATA_TABLE_NAME} is created.")

        # load the raw data into the raw data table        
        df = pd.read_csv(RAW_DATA_FILEPATH)

        df.to_sql(RAW_DATA_TABLE_NAME, engine, if_exists='replace', index=False)
        print("Raw data has been inserted into the raw data table.")


    except psycopg2.Error as e:
        print("An error occurred:", e)

load_raw_data()




