import sys
import os

# Add the Utils directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
utils_dir = os.path.join(current_dir, 'Utils')
if utils_dir not in sys.path:
    sys.path.append(utils_dir)

from db_utils import Get_data
from s3_utils import upload_parquet
import pandas as pd
import logging
from datetime import datetime


def landing_layer_extract(conn,engine,bucket,table_name):
    try:
        if conn:
            df = Get_data(conn,engine,table_name)
            if df is None:
                logging.info(f"No data extracted from {table_name} table")
                return False
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"bronze_layer/batch_job/{table_name}/{timestamp}.parquet"
            success = upload_parquet(df, bucket, s3_key)
            if success:
                logging.info(f"Data from {table_name} table uploaded to S3 successfully")
                return True
            else:
                logging.info(f"Failed to upload data from {table_name} table to S3")
                return False
        else:
            logging.info("No valid database connection")
            return False
    except Exception as e:
        logging.error(f"Error in landing_layer_extract for {table_name}: {str(e)}")
        return False
        
