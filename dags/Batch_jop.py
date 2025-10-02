from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from scripts.extract_to_s3 import landing_layer_extract
from scripts.Utils.db_utils import create_connection, close_connection
from scripts.GP_validate import run_validation


import logging

default_args = {
    'owner': 'Mahmoud',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'batch_jop',
    default_args=default_args,
    description='form db to s3',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

def load_credentials():
    return {
        "host": "172.19.0.3",
        "db_name": 'store_db',
        "user": 'airflow',
        "password": 'airflow'
    }
bucket = 'incircl'
tables = ['products','categories','customers','employees','orders','order_details']

def test_connection():
    db_credentials = load_credentials()
    conn, engine = create_connection(**db_credentials)
    if conn:
        logging.info("Database connection is valid")
        close_connection(conn, engine)
        return True
    else:
        logging.error("Failed to connect to the database")
        return False

def from_db_to_s3():
    try:
        db_credentials = load_credentials()
        conn, engine = create_connection(**db_credentials)
        
        if not conn:
            logging.error("Failed to establish database connection")
            return
        
        success_count = 0
        for table in tables:
            logging.info(f"Starting data extraction for table: {table}")
            success = landing_layer_extract(conn, engine, bucket, table)
            if success:
                success_count += 1
                logging.info(f"Data extraction completed for table: {table}")
            else:
                logging.error(f"Data extraction failed for table: {table}")
        
        close_connection(conn, engine)
        logging.info(f"Batch job completed. {success_count}/{len(tables)} tables processed successfully")
        
    except Exception as e:
        logging.error(f"Error in from_db_to_s3: {str(e)}")
        if 'conn' in locals() and 'engine' in locals():
            close_connection(conn, engine)


def gp_validation():
    try:
        logging.info("Starting GP validation process")
        run_validation()
        logging.info("GP validation completed successfully")
        return True
    except Exception as e:
        logging.error(f"Error in GP validation: {str(e)}")
        return False


database_preparation_task = PythonOperator(
    task_id='database_preparation',
    python_callable=test_connection,
    dag=dag,
)

data_delivery_task = PythonOperator(
    task_id='data_delivery',
    python_callable=from_db_to_s3,
    dag=dag,
)

gp_validation_task = PythonOperator(
    task_id='gp_validation',
    python_callable=gp_validation,
    dag=dag,
)


database_preparation_task >> data_delivery_task >> gp_validation_task  
