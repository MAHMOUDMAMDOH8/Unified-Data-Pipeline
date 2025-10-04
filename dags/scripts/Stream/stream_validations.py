import pandas as pd
import logging
import os
import sys
import json
from datetime import datetime


current_dir = os.path.dirname(os.path.abspath(__file__))
utils_dir = os.path.join(current_dir, '..', 'Utils')
if utils_dir not in sys.path:
    sys.path.append(utils_dir)

from s3_utils import (
    get_latest_table_dataframe,
    upload_parquet,
)

def validate_stream_orders(df: pd.DataFrame) -> tuple[bool, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    if df.empty:
        return True, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if 'payload' in df.columns:
        df['parsed_payload'] = df['payload'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
        event_data = []
        for idx, row in df.iterrows():
            if pd.notna(row['parsed_payload']):
                event_data.append(row['parsed_payload'])
            else:
                event_data.append({})
        
        valid_df = pd.DataFrame(event_data)
    else:
        valid_df = df.copy()
    
    reject_df = pd.DataFrame(columns=list(valid_df.columns) + ["rejection_reason"])
    
    logging.info(f"Starting validation with {len(valid_df)} rows")
    logging.info(f"Initial columns: {list(valid_df.columns)}")

    if 'geo_location' in valid_df.columns:
        valid_df['geo_location_parsed'] = valid_df['geo_location'].apply(
            lambda x: json.loads(x) if isinstance(x, str) and x.startswith('{') else {}
        )
    
    if 'details' in valid_df.columns:
        def safe_json_parse(x):
            try:
                if isinstance(x, str) and x.startswith('{'):
                    return json.loads(x)
                elif isinstance(x, dict):
                    return x
                else:
                    return {}
            except (json.JSONDecodeError, TypeError) as e:
                logging.warning(f"Failed to parse JSON: {e}, value: {x}")
                return {}
        
        valid_df['details_parsed'] = valid_df['details'].apply(safe_json_parse)
        
        if not valid_df.empty:
            sample_details = valid_df['details_parsed'].iloc[0]
            logging.info(f"Sample parsed details structure: {list(sample_details.keys()) if isinstance(sample_details, dict) else 'Not a dict'}")
            logging.info(f"Sample details content: {sample_details}")

    required_top_level = ["timestamp", "user_id", "session_id", "event_type"]
    for col_name in required_top_level:
        invalid = valid_df[valid_df[col_name].isnull()]
        if not invalid.empty:
            invalid = invalid.copy()
            invalid["rejection_reason"] = f"Missing required {col_name}"
            reject_df = pd.concat([reject_df, invalid])
            valid_df = valid_df.drop(invalid.index)
            logging.info(f"Rejected {len(invalid)} rows due to missing {col_name}")
    
    logging.info(f"After top-level validation: {len(valid_df)} valid, {len(reject_df)} rejected")

    try:
        if 'details_parsed' in valid_df.columns:
            details_df = pd.json_normalize(valid_df["details_parsed"])
            details_df.index = valid_df.index
        else:
            details_df = pd.json_normalize(valid_df["details"])
            details_df.index = valid_df.index
        
        logging.info(f"Details DataFrame columns: {list(details_df.columns)}")
        
    except Exception as e:
        logging.error(f"Failed to normalize details: {e}")
        details_df = pd.DataFrame(index=valid_df.index)

    required_details = ["order_id", "status", "order_amount", "currency"]
    for col_name in required_details:
        if col_name in details_df.columns:
            invalid_idx = details_df[details_df[col_name].isnull()].index
            if not invalid_idx.empty:
                invalid = valid_df.loc[invalid_idx].copy()
                invalid["rejection_reason"] = f"Missing details.{col_name}"
                reject_df = pd.concat([reject_df, invalid])
                valid_df = valid_df.drop(invalid.index)
                logging.info(f"Rejected {len(invalid)} rows due to missing details.{col_name}")
        else:
            logging.warning(f"Column {col_name} not found in details. Available columns: {list(details_df.columns)}")
            invalid = valid_df.copy()
            invalid["rejection_reason"] = f"Missing required details column: {col_name}"
            reject_df = pd.concat([reject_df, invalid])
            valid_df = valid_df.drop(valid_df.index)  
            logging.info(f"Rejected all {len(invalid)} rows due to missing details column: {col_name}")

    logging.info(f"After details validation: {len(valid_df)} valid, {len(reject_df)} rejected")

    if "order_amount" in details_df.columns:
        invalid_idx = details_df[details_df["order_amount"] <= 0].index
        if not invalid_idx.empty:
            invalid = valid_df.loc[invalid_idx].copy()
            invalid["rejection_reason"] = "Invalid order_amount <= 0"
            reject_df = pd.concat([reject_df, invalid])
            valid_df = valid_df.drop(invalid.index)
            logging.info(f"Rejected {len(invalid)} rows due to invalid order_amount <= 0")
    else:
        logging.warning("order_amount column not found in details")
    
    logging.info(f"After order_amount validation: {len(valid_df)} valid, {len(reject_df)} rejected")

    if 'details_parsed' in valid_df.columns:
        details_df = pd.json_normalize(valid_df["details_parsed"])
        details_df.index = valid_df.index
    else:
        details_df = pd.json_normalize(valid_df["details"])
        details_df.index = valid_df.index

    orders_cols = [
        "timestamp", "user_id", "session_id", "event_type",
        "level", "service", "session_duration", "device_type",
        "country", "city",  
        "order_id", "status", "order_amount", "currency",
        "shipping_method", "shipping_address", "warehouse",
        "carrier", "tracking_number", "completed_at", "delivery_estimate"
    ]
    
    if 'geo_location_parsed' in valid_df.columns:
        geo_df = pd.json_normalize(valid_df["geo_location_parsed"])
    else:
        geo_df = pd.json_normalize(valid_df["geo_location"])
    geo_df.index = valid_df.index
    
    drop_cols = ["details", "geo_location"]
    if 'details_parsed' in valid_df.columns:
        drop_cols.append("details_parsed")
    if 'geo_location_parsed' in valid_df.columns:
        drop_cols.append("geo_location_parsed")
    
    orders_df = pd.concat([
        valid_df.drop(columns=drop_cols), 
        geo_df.rename(columns={"country": "country", "city": "city"}),
        details_df.drop(columns=["items"] if "items" in details_df.columns else [])
    ], axis=1)
    
    available_cols = [col for col in orders_cols if col in orders_df.columns]
    orders_df = orders_df[available_cols]

    
    items_rows = []
    if "items" in details_df.columns:
        for idx, items in details_df["items"].dropna().items():
            for item in items:
                q = item.get("quantity", 0)
                p = item.get("unit_price", 0)
                if q <= 0 or p <= 0:
                    invalid = valid_df.loc[[idx]].copy()
                    invalid["rejection_reason"] = "Invalid item quantity/unit_price"
                    reject_df = pd.concat([reject_df, invalid])
                    continue
                items_rows.append({
                    "order_id": details_df.at[idx, "order_id"],
                    "product_id": item.get("product_id"),
                    "quantity": q,
                    "unit_price": p,
                    "line_amount": item.get("line_amount"),
                    "category": item.get("category")
                })
    else:
        logging.warning("items column not found in details")

    items_df = pd.DataFrame(items_rows)

    
    orders_df = orders_df.reset_index(drop=True)
    items_df = items_df.reset_index(drop=True)
    reject_df = reject_df.reset_index(drop=True)

    logging.info(f"Validated stream batch: {len(orders_df)} orders, {len(items_df)} items, {len(reject_df)} rejected")
    
    if not reject_df.empty:
        rejection_reasons = reject_df['rejection_reason'].value_counts()
        logging.info(f"Rejection reasons: {rejection_reasons.to_dict()}")
        logging.info(f"Sample rejected rows: {reject_df[['user_id', 'rejection_reason']].head()}")
    
    is_valid = reject_df.empty
    return is_valid, orders_df, reject_df, items_df


def process_table(bucket: str, table_name: str):
    logging.info(f"Processing table: {table_name}")
    df, src_key = get_latest_table_dataframe(file_type='json',bucket=bucket, table_name=table_name)
    if df is None or src_key is None:
        logging.info(f"No data found for table {table_name}")
        return False

    is_valid, valid_df, reject_df, items_df = validate_stream_orders(df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not valid_df.empty:
        key = f"silver_layer/valid_data/{table_name}/{timestamp}.parquet"
        ok = upload_parquet(valid_df, bucket, key)
        logging.info(f"Valid orders data uploaded to s3://{bucket}/{key}: {ok}")
    
    if not items_df.empty:
        key = f"silver_layer/valid_data/{table_name}_items/{timestamp}.parquet"
        ok = upload_parquet(items_df, bucket, key)
        logging.info(f"Valid items data uploaded to s3://{bucket}/{key}: {ok}")
    
    if not reject_df.empty:
        key = f"silver_layer/reject_data/{table_name}/{timestamp}.parquet"
        logging.info(f"Attempting to upload {len(reject_df)} rejected rows to s3://{bucket}/{key}")
        ok = upload_parquet(reject_df, bucket, key)
        logging.info(f"Reject data uploaded to s3://{bucket}/{key}: {ok}")
        if not ok:
            logging.error(f"Failed to upload rejected data to S3!")
    else:
        logging.info("No rejected data to upload")

    return True


def run_validation():
    bucket = os.getenv('DATA_LAKE_BUCKET', 'incircl')
    tables = ['events']
    for table in tables:
        try:
            process_table(bucket, table)
        except Exception as e:
            logging.error(f"Validation failed for {table}: {e}")




if __name__ == "__main__":
    run_validation()
