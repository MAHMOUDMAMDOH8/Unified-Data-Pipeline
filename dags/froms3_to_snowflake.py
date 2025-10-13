from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import logging
from datetime import datetime, timedelta

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
scripts_dir = os.path.join(current_dir, 'scripts')
if scripts_dir not in sys.path:
    sys.path.append(scripts_dir)

from Utils.db_utils import *
from Utils.s3_utils import * 
from Utils.snowflake_utilz import *


from extract_to_snowflake import extract_silver_to_snowflake

# Configuration
bucket = 'incircl'
tables = ['products','categories','customers','employees','orders','order_details','events_items','events']
folder_key = 'silver_layer/valid_data'

def snowflake_credentials():
    return {
        'user': os.getenv("SNOWFLAKE_USER","UnifiedPipeline"),
        'password': os.getenv("SNOWFLAKE_PASSWORD","UnifiedPipeline1"),
        'account': os.getenv("SNOWFLAKE_ACCOUNT","QDZURPO-YQ18586"),
        'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE","COMPUTE_WH"),
        'database': os.getenv("SNOWFLAKE_DATABASE","UNIFIED_ECOMM"),
        'schema': os.getenv("SNOWFLAKE_SCHEMA","PUBLIC"),
    }

def database_credentials():
    return {
        "host": "172.19.0.4",
        "db_name": 'store_db',
        "user": 'airflow',
        "password": 'airflow'
    }

def test_snowflake_connection():
    """Test Snowflake connection"""
    try:
        db_credentials = snowflake_credentials()
        conn, engine = snowFlaek_connection(**db_credentials)
        if conn:
            logging.info("Snowflake connection is valid")
            close_connection(conn, engine)
            return True
        else:
            logging.error("Failed to connect to the Snowflake")
            return False
    except Exception as e:
        logging.error(f"Error in test_snowflake_connection: {str(e)}")
        return False

def test_database_connection():
    """Test PostgreSQL database connection"""
    try:
        db_credentials = database_credentials()
        conn, engine = create_connection(**db_credentials)
        if conn:
            logging.info("Database connection is valid")
            close_connection(conn, engine)
            return True
        else:
            logging.error("Failed to connect to the database")
            return False
    except Exception as e:
        logging.error(f"Error in test_database_connection: {str(e)}")
        return False

def extract_s3_to_snowflake_task():
    try:
        logging.info("Starting S3 to Snowflake extraction process")
        success = extract_silver_to_snowflake(bucket, tables)
        
        if success:
            logging.info("S3 to Snowflake extraction completed successfully")
            return True
        else:
            logging.error("S3 to Snowflake extraction failed")
            raise Exception("Extraction process failed")
            
    except Exception as e:
        logging.error(f"Error in extract_s3_to_snowflake_task: {str(e)}")
        raise

# DAG Configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}


dag = DAG(
    's3_to_snowflake_extraction',
    default_args=default_args,
    description='Extract validated data from S3 silver layer and load into Snowflake',
    schedule_interval='@daily',  
    max_active_runs=1,
    tags=['s3', 'snowflake', 'extraction', 'silver-layer']
)


test_snowflake_task = PythonOperator(
    task_id='test_snowflake_connection',
    python_callable=test_snowflake_connection,
    dag=dag
)

test_database_task = PythonOperator(
    task_id='test_database_connection',
    python_callable=test_database_connection,
    dag=dag
)

extract_to_snowflake_task = PythonOperator(
    task_id='extract_s3_to_snowflake',
    python_callable=extract_s3_to_snowflake_task,
    dag=dag
)

[test_snowflake_task, test_database_task] >> extract_to_snowflake_task