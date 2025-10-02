#!/usr/bin/env python3
"""
Create orders test data with validation issues and upload to S3
"""
import os
import sys
import pandas as pd
from datetime import datetime

# Add the dags directory to the path
sys.path.append('/opt/airflow/dags')

from scripts.Utils.s3_utils import upload_parquet

def create_orders_test_data():
    """Create orders test data with validation issues"""
    
    # Valid data
    valid_data = {
        'order_id': [9901, 9902, 9903],
        'customer_id': ['VAL01', 'VAL02', 'VAL03'],
        'employee_id': [1, 2, 1],
        'order_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'required_date': ['2024-01-02', '2024-01-03', '2024-01-04'],
        'shipped_date': ['2024-01-03', '2024-01-04', '2024-01-05'],
        'ship_via': [1, 2, 1],
        'freight': [10.50, 15.75, 8.25],
        'ship_name': ['Valid Ship Name 1', 'Valid Ship Name 2', 'Valid Ship Name 3'],
        'ship_address': ['123 Valid St', '456 Valid Ave', '789 Valid Blvd'],
        'ship_city': ['Valid City', 'Valid City', 'Valid City'],
        'ship_region': ['Valid Region', 'Valid Region', 'Valid Region'],
        'ship_postal_code': ['12345', '54321', '67890'],
        'ship_country': ['Valid Country', 'Valid Country', 'Valid Country']
    }
    
    # Data with all nulls (should be rejected)
    null_data = {
        'order_id': [None, None, None],
        'customer_id': [None, None, None],
        'employee_id': [None, None, None],
        'order_date': [None, None, None],
        'required_date': [None, None, None],
        'shipped_date': [None, None, None],
        'ship_via': [None, None, None],
        'freight': [None, None, None],
        'ship_name': [None, None, None],
        'ship_address': [None, None, None],
        'ship_city': [None, None, None],
        'ship_region': [None, None, None],
        'ship_postal_code': [None, None, None],
        'ship_country': [None, None, None]
    }
    
    # Duplicate data (should be rejected)
    duplicate_data = {
        'order_id': [9901, 9901, 9902],
        'customer_id': ['VAL01', 'VAL01', 'VAL02'],
        'employee_id': [1, 1, 2],
        'order_date': ['2024-01-01', '2024-01-01', '2024-01-02'],
        'required_date': ['2024-01-02', '2024-01-02', '2024-01-03'],
        'shipped_date': ['2024-01-03', '2024-01-03', '2024-01-04'],
        'ship_via': [1, 1, 2],
        'freight': [10.50, 10.50, 15.75],
        'ship_name': ['Valid Ship Name 1', 'Valid Ship Name 1', 'Valid Ship Name 2'],
        'ship_address': ['123 Valid St', '123 Valid St', '456 Valid Ave'],
        'ship_city': ['Valid City', 'Valid City', 'Valid City'],
        'ship_region': ['Valid Region', 'Valid Region', 'Valid Region'],
        'ship_postal_code': ['12345', '12345', '54321'],
        'ship_country': ['Valid Country', 'Valid Country', 'Valid Country']
    }
    
    # Data with invalid dates (should be rejected)
    invalid_data = {
        'order_id': [9904, 9905, 9906],
        'customer_id': ['INV01', 'INV02', 'INV03'],
        'employee_id': [1, 2, 1],
        'order_date': ['invalid-date', '2024-13-45', 'not-a-date'],
        'required_date': ['2024-01-02', '2024-01-03', '2024-01-04'],
        'shipped_date': ['2024-01-03', '2024-01-04', '2024-01-05'],
        'ship_via': [1, 2, 1],
        'freight': [10.50, 15.75, 8.25],
        'ship_name': ['Invalid Ship Name 1', 'Invalid Ship Name 2', 'Invalid Ship Name 3'],
        'ship_address': ['123 Invalid St', '456 Invalid Ave', '789 Invalid Blvd'],
        'ship_city': ['Invalid City', 'Invalid City', 'Invalid City'],
        'ship_region': ['Invalid Region', 'Invalid Region', 'Invalid Region'],
        'ship_postal_code': ['12345', '54321', '67890'],
        'ship_country': ['Invalid Country', 'Invalid Country', 'Invalid Country']
    }
    
    # Combine all data
    all_data = {key: valid_data[key] + null_data[key] + duplicate_data[key] + invalid_data[key] for key in valid_data.keys()}
    
    return pd.DataFrame(all_data)

def upload_test_data():
    """Upload test data to S3"""
    bucket = os.getenv('DATA_LAKE_BUCKET', 'incircl')
    
    # Create test data
    df = create_orders_test_data()
    
    # Upload to S3
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"bronze_layer/batch_job/orders/{timestamp}.parquet"
    
    print(f"Uploading test data to s3://{bucket}/{key}")
    print(f"Data shape: {df.shape}")
    print(f"Null rows: {df.isnull().all(axis=1).sum()}")
    print(f"Duplicate rows: {df.duplicated().sum()}")
    
    success = upload_parquet(df, bucket, key)
    
    if success:
        print("Test data uploaded successfully!")
        return True
    else:
        print("Failed to upload test data!")
        return False

if __name__ == "__main__":
    upload_test_data()
