#!/usr/bin/env python3
"""
Test script to verify rejection analysis functionality
"""
import os
import sys
import pandas as pd

# Add the dags directory to the path
sys.path.append('/opt/airflow/dags')

from scripts.Utils.s3_utils import get_s3_client, list_objects, read_parquet_from_s3

def test_rejection_analysis():
    """Test the rejection analysis functionality"""
    
    # Set environment variables
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    os.environ['S3_ENDPOINT_URL'] = 'https://legendary-waddle-wpxjw6pjxp7f9pvp-4566.app.github.dev'
    os.environ['S3_VERIFY_SSL'] = 'false'
    os.environ['S3_ADDRESSING_STYLE'] = 'path'
    
    try:
        s3_client = get_s3_client()
        if not s3_client:
            print("Failed to get S3 client")
            return
        
        bucket = 'incircl'
        
        # Test orders table rejection data
        print("=== TESTING ORDERS TABLE REJECTION DATA ===")
        reject_prefix = f"silver_layer/reject_data/orders/"
        reject_objects = list(list_objects(s3_client, bucket, reject_prefix))
        
        if reject_objects:
            latest_reject = max(reject_objects, key=lambda x: x['LastModified'])
            print(f"Latest reject file: {latest_reject['Key']}")
            
            # Read the rejected data
            df = read_parquet_from_s3(bucket, latest_reject['Key'])
            if df is not None and not df.empty:
                print(f"Rejected data shape: {df.shape}")
                print(f"Columns: {list(df.columns)}")
                
                if 'rejection_reason' in df.columns:
                    print(f"Rejection reasons:")
                    print(df['rejection_reason'].value_counts())
                    
                    print(f"\nSample rejected data:")
                    print(df[['order_id', 'customer_id', 'order_date', 'rejection_reason']].head())
                else:
                    print("No rejection_reason column found")
            else:
                print("No data found in reject file")
        else:
            print("No reject files found for orders table")
        
        # Test customers table rejection data
        print("\n=== TESTING CUSTOMERS TABLE REJECTION DATA ===")
        reject_prefix = f"silver_layer/reject_data/customers/"
        reject_objects = list(list_objects(s3_client, bucket, reject_prefix))
        
        if reject_objects:
            latest_reject = max(reject_objects, key=lambda x: x['LastModified'])
            print(f"Latest reject file: {latest_reject['Key']}")
            
            # Read the rejected data
            df = read_parquet_from_s3(bucket, latest_reject['Key'])
            if df is not None and not df.empty:
                print(f"Rejected data shape: {df.shape}")
                print(f"Columns: {list(df.columns)}")
                
                if 'rejection_reason' in df.columns:
                    print(f"Rejection reasons:")
                    print(df['rejection_reason'].value_counts())
                    
                    print(f"\nSample rejected data:")
                    print(df[['customer_id', 'company_name', 'rejection_reason']].head())
                else:
                    print("No rejection_reason column found")
            else:
                print("No data found in reject file")
        else:
            print("No reject files found for customers table")
            
    except Exception as e:
        print(f"Error during testing: {e}")

if __name__ == "__main__":
    test_rejection_analysis()
