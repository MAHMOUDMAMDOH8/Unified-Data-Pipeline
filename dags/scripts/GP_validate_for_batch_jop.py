import os
import logging
from datetime import datetime
import pandas as pd
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
utils_dir = os.path.join(current_dir, 'Utils')
if utils_dir not in sys.path:
    sys.path.append(utils_dir)

from s3_utils import (
    get_latest_table_dataframe,
    upload_parquet,
)


def process_table(bucket: str, table_name: str):
    logging.info(f"Processing table: {table_name}")
    df, src_key = get_latest_table_dataframe(file_type='parquet',bucket=bucket, table_name=table_name)
    if df is None or src_key is None:
        logging.info(f"No data found for table {table_name}")
        return False

   
    if table_name == "customers":
        validate_fn = customer_validate
    elif table_name == "products":
        validate_fn = product_validate
    elif table_name == "categories":
        validate_fn = category_validate
    elif table_name == "orders":
        validate_fn = order_validate
    elif table_name == "order_details":
        validate_fn = order_details_validate
    elif table_name == "employees":
        validate_fn = employee_validate
    else:
        logging.warning(f"No validator defined for {table_name}")
        return False

    is_valid, valid_df, reject_df = validate_fn(df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
   
    if not valid_df.empty:
        key = f"silver_layer/valid_data/{table_name}/{timestamp}.parquet"
        ok = upload_parquet(valid_df, bucket, key)
        logging.info(f"Valid data uploaded to s3://{bucket}/{key}: {ok}")
    
    if not reject_df.empty:
        key = f"silver_layer/reject_data/{table_name}/{timestamp}.parquet"
        ok = upload_parquet(reject_df, bucket, key)
        logging.info(f"Reject data uploaded to s3://{bucket}/{key}: {ok}")

    return True




def basic_not_null(df: pd.DataFrame, required_cols: list[str], table_name: str) -> tuple[bool, pd.DataFrame, pd.DataFrame]:
    
    reject_df = df[df[required_cols].isnull().any(axis=1)].copy()
    if not reject_df.empty:
        reject_df["rejection_reason"] = f"{table_name}: missing required values {required_cols}"
    valid_df = df.drop(reject_df.index)
    return (reject_df.empty, valid_df, reject_df)



def customer_validate(df: pd.DataFrame) -> tuple[bool, pd.DataFrame, pd.DataFrame]:
    required_cols = ["customer_id", "company_name", "address", "city", "country"]
    return basic_not_null(df, required_cols, "customers")


def product_validate(df: pd.DataFrame) -> tuple[bool, pd.DataFrame, pd.DataFrame]:
    required_cols = ["product_id", "product_name", "supplier_id", "category_id", "unit_price"]
    return basic_not_null(df, required_cols, "products")


def category_validate(df: pd.DataFrame) -> tuple[bool, pd.DataFrame, pd.DataFrame]:
    required_cols = ["category_id", "category_name"]
    return basic_not_null(df, required_cols, "categories")


def order_validate(df: pd.DataFrame) -> tuple[bool, pd.DataFrame, pd.DataFrame]:
    """
    Focus: orders table validation
    """
    required_cols = [
        "order_id", "customer_id", "employee_id",
        "order_date", "required_date", "ship_name",
        "ship_address", "ship_city", "ship_country"
    ]
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logging.error(f"Missing required columns: {missing_cols}")
        reject_df = df.copy()
        reject_df["rejection_reason"] = f"Missing required columns: {missing_cols}"
        return False, pd.DataFrame(), reject_df
    
    is_valid, valid_df, reject_df = basic_not_null(df, required_cols, "orders")

    if not valid_df.empty:
       
        valid_df = valid_df.copy()
        valid_df.loc[:, "order_date"] = pd.to_datetime(valid_df["order_date"], errors="coerce")
        valid_df.loc[:, "required_date"] = pd.to_datetime(valid_df["required_date"], errors="coerce")

        
        invalid_date_rows = valid_df[valid_df["order_date"].isnull() | valid_df["required_date"].isnull()].copy()
        if not invalid_date_rows.empty:
            invalid_date_rows["rejection_reason"] = "Invalid order_date or required_date format"
            reject_df = pd.concat([reject_df, invalid_date_rows])
            valid_df = valid_df.drop(invalid_date_rows.index)

        
        invalid_required = valid_df[valid_df["required_date"] < valid_df["order_date"]].copy()
        if not invalid_required.empty:
            invalid_required["rejection_reason"] = "required_date earlier than order_date"
            reject_df = pd.concat([reject_df, invalid_required])
            valid_df = valid_df.drop(invalid_required.index)

    is_valid = reject_df.empty
    return (is_valid, valid_df, reject_df)


def order_details_validate(df: pd.DataFrame) -> tuple[bool, pd.DataFrame, pd.DataFrame]:
    """
    Focus: order_details table validation
    """
    required_cols = ["order_id", "product_id", "unit_price", "quantity"]
    is_valid, valid_df, reject_df = basic_not_null(df, required_cols, "order_details")

    if not valid_df.empty:
        valid_df = valid_df.copy()

        
        invalid_rows = valid_df[(valid_df["unit_price"] <= 0) | (valid_df["quantity"] <= 0)].copy()
        if not invalid_rows.empty:
            invalid_rows["rejection_reason"] = "unit_price or quantity <= 0"
            reject_df = pd.concat([reject_df, invalid_rows])
            valid_df = valid_df.drop(invalid_rows.index)


        if "discount" in valid_df.columns:
            invalid_disc = valid_df[(valid_df["discount"] < 0) | (valid_df["discount"] > 1)].copy()
            if not invalid_disc.empty:
                invalid_disc["rejection_reason"] = "discount not between 0 and 1"
                reject_df = pd.concat([reject_df, invalid_disc])
                valid_df = valid_df.drop(invalid_disc.index)

    is_valid = reject_df.empty
    return (is_valid, valid_df, reject_df)


def employee_validate(df: pd.DataFrame) -> tuple[bool, pd.DataFrame, pd.DataFrame]:
    required_cols = ["employee_id", "last_name", "first_name", "hire_date", "address", "city"]
    return basic_not_null(df, required_cols, "employees")


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