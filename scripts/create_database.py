import psycopg2
from psycopg2 import sql
from scripts.constants import *
from scripts.common_functions import create_sqlalchemy_engine

def create_database():
    try:

        # create a connection to the default database in Postgres (postgres)
        engine = create_sqlalchemy_engine(db_name='postgres')
        connection = engine.raw_connection()
        connection.autocommit = True
        cursor = connection.cursor()

        # check if the source database exists already
        cursor.execute(f"SELECT * FROM pg_database WHERE datname = '{DB_NAME}'")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(sql.SQL(f"CREATE DATABASE {DB_NAME}"))
            print(f"Created database {DB_NAME}")
        else:
            print(f'{DB_NAME} already exists!')

        # close the connection to 'postgres' db and open a connection to the one we just made
        cursor.close()
        connection.close()
    except psycopg2.Error as e:
        print("An error occurred:", e)