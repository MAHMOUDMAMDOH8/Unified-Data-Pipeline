import os
import logging
from datetime import datetime
import pandas as pd

from Utils.s3_utils import (
    get_latest_table_dataframe,
    upload_parquet,
)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def validate_dataframe(df: pd.DataFrame) -> tuple[bool, pd.DataFrame, pd.DataFrame]:
    """
    Validate dataframe and return valid data, rejected data with reasons
    """
    if df is None or df.empty:
        return False, pd.DataFrame(), df if df is not None else pd.DataFrame()

    # Create a copy to add rejection reasons
    df_with_reasons = df.copy()
    df_with_reasons['rejection_reason'] = ''
    
    # Check for completely null rows
    null_mask = df.isnull().all(axis=1)
    df_with_reasons.loc[null_mask, 'rejection_reason'] = 'All fields are null'
    
    # Check for duplicates (only on non-null rows)
    valid_mask = ~null_mask
    if valid_mask.any():
        valid_df_temp = df[valid_mask]
        duplicate_mask = valid_df_temp.duplicated()
        # Map back to original dataframe
        duplicate_indices = valid_df_temp[duplicate_mask].index
        df_with_reasons.loc[duplicate_indices, 'rejection_reason'] = 'Duplicate row'
    
    # Check for invalid dates in order_date column (if it exists)
    if 'order_date' in df.columns:
        date_mask = valid_mask & (df['order_date'].notna())
        if date_mask.any():
            try:
                pd.to_datetime(df.loc[date_mask, 'order_date'], errors='coerce')
                invalid_date_mask = pd.to_datetime(df.loc[date_mask, 'order_date'], errors='coerce').isna()
                invalid_date_indices = df.loc[date_mask][invalid_date_mask].index
                df_with_reasons.loc[invalid_date_indices, 'rejection_reason'] = 'Invalid date format in order_date'
            except:
                pass
    
    # Separate valid and rejected data
    reject_mask = (df_with_reasons['rejection_reason'] != '')
    reject_df = df_with_reasons[reject_mask]
    valid_df = df_with_reasons[~reject_mask].drop('rejection_reason', axis=1)
    
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
