import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import pandas as pd
from io import BytesIO

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def get_s3_client():
    try:
        return boto3.client('s3')
    except NoCredentialsError:
        logging.error("AWS credentials not found")
        return None


def path_exists(bucket, key):
    logging.info(f"Checking if path exists: s3://{bucket}/{key}")
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False
        s3_client.head_object(Bucket=bucket, Key=key)
        logging.info(f"Path exists: s3://{bucket}/{key}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logging.info(f"Path does not exist: s3://{bucket}/{key}")
            return False
        else:
            logging.error(f"Error checking path: {e}")
            return False


def upload_parquet(data, bucket, key):
    logging.info(f"Uploading data to s3://{bucket}/{key}")
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False
        
        if isinstance(data, pd.DataFrame):
            buffer = BytesIO()
            data.to_parquet(buffer, index=False)
            body = buffer.getvalue()
        elif isinstance(data, str):
            df = pd.read_csv(pd.StringIO(data))
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            body = buffer.getvalue()
        elif isinstance(data, bytes):
            body = data
        else:
            logging.error(f"Unsupported data type: {type(data)}")
            return False
        
        s3_client.put_object(Bucket=bucket, Key=key, Body=body)
        logging.info(f"Successfully uploaded parquet to s3://{bucket}/{key}")
        return True
        
    except Exception as e:
        logging.error(f"Error uploading parquet data: {e}")
        return False

