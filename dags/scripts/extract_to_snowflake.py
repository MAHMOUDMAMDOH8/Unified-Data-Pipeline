import sys
import os
import logging
import pandas as pd
from datetime import datetime

# Add Utils directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
utils_dir = os.path.join(current_dir, 'Utils')
if utils_dir not in sys.path:
    sys.path.append(utils_dir)

from s3_utils import (
    get_s3_client,
    list_objects,
    read_parquet_from_s3
)
from snowflake_utilz import (
    snowFlaek_connection,
    close_connection,
    insert_raw_data
)
from db_utils import (
    create_connection,
    close_connection as close_postgres_connection,
    is_file_processed,
    mark_file_as_processed
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_snowflake_credentials():
    return {
        'user': os.getenv("SNOWFLAKE_USER", "UnifiedPipeline"),
        'password': os.getenv("SNOWFLAKE_PASSWORD", "UnifiedPipeline1"),
        'account': os.getenv("SNOWFLAKE_ACCOUNT", "QDZURPO-YQ18586"),
        'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        'database': os.getenv("SNOWFLAKE_DATABASE", "UNIFIED_ECOMM"),
        'schema': os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    }

def get_postgres_credentials():
    return {
        "host": os.getenv("POSTGRES_HOST", "172.19.0.4"),
        "db_name": os.getenv("POSTGRES_DB", 'store_db'),
        "user": os.getenv("POSTGRES_USER", 'airflow'),
        "password": os.getenv("POSTGRES_PASSWORD", 'airflow')
    }

def get_latest_file_for_table(bucket: str, table_name: str, prefix: str = 'silver_layer/valid_data') -> tuple[str, datetime] | tuple[None, None]:
    try:
        s3_client = get_s3_client()
        if not s3_client:
            logging.error("Failed to get S3 client")
            return None, None

        table_prefix = f"{prefix}/{table_name}/"
        latest_file = None
        latest_modified = None
        
        for obj in list_objects(s3_client, bucket, table_prefix):
            key = obj['Key']
            if key.endswith('.parquet'):
                last_modified = obj.get('LastModified')
                if latest_modified is None or (last_modified and last_modified > latest_modified):
                    latest_modified = last_modified
                    latest_file = key
        
        if latest_file:
            logging.info(f"Latest file for {table_name}: {latest_file}")
            return latest_file, latest_modified
        else:
            logging.info(f"No parquet files found for table {table_name} in {table_prefix}")
            return None, None
            
    except Exception as e:
        logging.error(f"Error getting latest file for {table_name}: {e}")
        return None, None

def extract_and_load_table(bucket: str, table_name: str, snowflake_conn, snowflake_engine, postgres_conn) -> bool:
    try:
        latest_file_key, last_modified = get_latest_file_for_table(bucket, table_name)
        
        if not latest_file_key:
            logging.info(f"No files found for table {table_name}")
            return True 
        
        
        if is_file_processed(postgres_conn, latest_file_key):
            logging.info(f"File {latest_file_key} has already been processed for table {table_name}")
            return True
        
        logging.info(f"Reading data from S3: s3://{bucket}/{latest_file_key}")
        df = read_parquet_from_s3(bucket, latest_file_key)
        
        if df is None:
            logging.error(f"Failed to read parquet file: s3://{bucket}/{latest_file_key}")
            return False
        
        if df.empty:
            logging.info(f"Empty dataframe for table {table_name} from file {latest_file_key}")
            mark_file_as_processed(postgres_conn, latest_file_key)
            return True
        
        logging.info(f"Loaded {len(df)} rows from {latest_file_key}")
        
        snowflake_table_name = f"RAW_{table_name.upper()}"
        logging.info(f"Inserting data into Snowflake table: {snowflake_table_name}")
        
        insert_raw_data(snowflake_table_name, df, snowflake_conn, snowflake_engine)
        mark_file_as_processed(postgres_conn, latest_file_key)
        logging.info(f"Successfully processed file {latest_file_key} for table {table_name}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error processing table {table_name}: {e}")
        return False

def extract_silver_to_snowflake(bucket: str = 'incircl', tables: list = None):
    if tables is None:
        tables = ['products', 'categories', 'customers', 'employees', 'orders', 'order_details', 'events_items', 'events']
    
    logging.info(f"Starting extraction from S3 bucket '{bucket}' to Snowflake")
    logging.info(f"Processing tables: {tables}")
    
    snowflake_creds = get_snowflake_credentials()
    postgres_creds = get_postgres_credentials()
    
    logging.info("Connecting to Snowflake...")
    snowflake_conn, snowflake_engine = snowFlaek_connection(**snowflake_creds)
    
    logging.info("Connecting to PostgreSQL...")
    postgres_conn, postgres_engine = create_connection(**postgres_creds)
    
    if not snowflake_conn:
        logging.error("Failed to connect to Snowflake")
        return False
    
    if not postgres_conn:
        logging.error("Failed to connect to PostgreSQL")
        close_connection(snowflake_conn, snowflake_engine)
        return False
    
    try:
        success_count = 0
        total_tables = len(tables)
        
        for table_name in tables:
            logging.info(f"Processing table {table_name} ({success_count + 1}/{total_tables})")
            
            if extract_and_load_table(bucket, table_name, snowflake_conn, snowflake_engine, postgres_conn):
                success_count += 1
                logging.info(f"Successfully processed table: {table_name}")
            else:
                logging.error(f"Failed to process table: {table_name}")
        
        logging.info(f"Extraction completed. Successfully processed {success_count}/{total_tables} tables")
        return success_count == total_tables
        
    except Exception as e:
        logging.error(f"Error during extraction process: {e}")
        return False
        
    finally:
        close_connection(snowflake_conn, snowflake_engine)
        close_postgres_connection(postgres_conn, postgres_engine)
        logging.info("All database connections closed")

def main():
    try:
        bucket = 'incircl'
        tables = ['products', 'categories', 'customers', 'employees', 'orders', 'order_details', 'events_items', 'events']
        
        success = extract_silver_to_snowflake(bucket, tables)
        
        if success:
            logging.info("All tables processed successfully")
            return True
        else:
            logging.error("Some tables failed to process")
            return False
            
    except Exception as e:
        logging.error(f"Error in main function: {e}")
        return False

if __name__ == "__main__":
    main()
