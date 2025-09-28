import os
import logging
from datetime import datetime
import pandas as pd

from scripts.Utils.s3_utils import (
    get_latest_table_dataframe,
    upload_parquet,
)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def validate_dataframe(df: pd.DataFrame) -> tuple[bool, pd.DataFrame, pd.DataFrame]:

    if df is None or df.empty:
        return False, pd.DataFrame(), df if df is not None else pd.DataFrame()

    
    reject_mask = df.isnull().all(axis=1) 
    reject_df = df[reject_mask]
    valid_df = df[~reject_mask]
    
    if not valid_df.empty:
        duplicate_mask = valid_df.duplicated()
        if duplicate_mask.any():
            reject_df = pd.concat([reject_df, valid_df[duplicate_mask]])
            valid_df = valid_df[~duplicate_mask]
    
    is_valid = len(reject_df) == 0 and len(valid_df) > 0
    return is_valid, valid_df, reject_df


def process_table(bucket: str, table_name: str):
    logging.info(f"Processing table: {table_name}")
    df, src_key = get_latest_table_dataframe(bucket=bucket, table_name=table_name)
    if df is None or src_key is None:
        logging.info(f"No data found for table {table_name}")
        return False

    is_valid, valid_df, reject_df = validate_dataframe(df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if is_valid and not valid_df.empty:
        key = f"silver_layer/valid_data/{table_name}/{timestamp}.parquet"
        ok = upload_parquet(valid_df, bucket, key)
        logging.info(f"Valid data uploaded to s3://{bucket}/{key}: {ok}")
    if not reject_df.empty:
        key = f"silver_layer/reject_data/{table_name}/{timestamp}.parquet"
        ok = upload_parquet(reject_df, bucket, key)
        logging.info(f"Reject data uploaded to s3://{bucket}/{key}: {ok}")

    return True


def run_validation():
    bucket = os.getenv('DATA_LAKE_BUCKET', 'incircl')
    tables = ['products','categories','customers','employees','orders','order_details']
    for table in tables:
        try:
            process_table(bucket, table)
        except Exception as e:
            logging.error(f"Validation failed for {table}: {e}")


if __name__ == "__main__":
    run_validation()
