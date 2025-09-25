import logging 
import os 
from sqlalchemy import create_engine , text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def create_connection(host, db_name, user, password):
    try:
        connection_string = f'postgresql://{user}:{password}@{host}/{db_name}'
        engine = create_engine(connection_string)
        connection = engine.connect()
        logging.info("Connected to PostgreSQL database successfully")
        return connection, engine
    except SQLAlchemyError as error:
        logging.error("Error while connecting to PostgreSQL: %s", error)
        return None, None
    

def close_connection(connection, engine):
    if connection:
        connection.close()
        logging.info("PostgreSQL connection is closed")
    if engine:
        engine.dispose()
        logging.info("SQLAlchemy engine disposed")


def Get_data(conn,engine,table_name):
    logging.info(f"Extracting data from {table_name} table")
    try:
        query = f"select * from {table_name}"
        df = pd.read_sql_query(query, conn)
        logging.info(f"Extracting data from {table_name} table done")
        return df
    except Exception as E:
        logging.error(f'Error while Extracting data from {table_name} table: {str(E)}')
        return None

