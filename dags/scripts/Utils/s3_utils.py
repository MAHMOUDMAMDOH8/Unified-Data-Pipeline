import logging
import os
import boto3
import json
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError
from urllib3.exceptions import InsecureRequestWarning
import urllib3
import pandas as pd
from io import BytesIO, StringIO

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def get_s3_client():
    try:

        endpoint_url = (
            os.getenv('S3_ENDPOINT_URL')
            or os.getenv('AWS_S3_ENDPOINT')
            or os.getenv('MINIO_ENDPOINT_URL')
            or os.getenv('ENDPOINT')
        )

        verify_env_value = os.getenv('S3_VERIFY_SSL', 'true').strip().lower()
        verify = verify_env_value in ('1', 'true', 't', 'yes', 'y')


        suppress_warn_env = os.getenv('S3_SUPPRESS_INSECURE_WARNING', 'true').strip().lower()
        suppress_insecure_warning = suppress_warn_env in ('1', 'true', 't', 'yes', 'y')
        if not verify and suppress_insecure_warning:
            urllib3.disable_warnings(InsecureRequestWarning)


        addressing_style = os.getenv('S3_ADDRESSING_STYLE', 'path').strip().lower()
        if addressing_style not in ('auto', 'path', 'virtual'):
            addressing_style = 'path'

        s3_config = Config(s3={'addressing_style': addressing_style})


        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_session_token = os.getenv('AWS_SESSION_TOKEN')
        region_name = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

        return boto3.client(
            's3',
            endpoint_url=endpoint_url,
            verify=verify,
            config=s3_config,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
        )
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
            df = pd.read_csv(StringIO(data))
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


def list_objects(s3_client, bucket: str, prefix: str):

    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get('Contents', [])
        for obj in contents:
            yield obj


def get_latest_parquet_key(bucket: str, table_name: str, base_prefix: str = 'bronze_layer/batch_job') -> str | None:

    try:
        s3_client = get_s3_client()
        if not s3_client:
            return None

        prefix = f"{base_prefix}/{table_name}/"
        latest_obj = None
        latest_ts = None
        for obj in list_objects(s3_client, bucket, prefix):
            key = obj['Key']
            if not key.endswith('.parquet'):
                continue
            last_modified = obj.get('LastModified')
            if latest_ts is None or (last_modified and last_modified > latest_ts):
                latest_ts = last_modified
                latest_obj = key

        return latest_obj
    except Exception as e:
        logging.error(f"Error getting latest parquet key for {table_name}: {e}")
        return None


def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame | None:

    try:
        s3_client = get_s3_client()
        if not s3_client:
            return None
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        buffer = BytesIO(body)
        df = pd.read_parquet(buffer)
        return df
    except Exception as e:
        logging.error(f"Error reading parquet from s3://{bucket}/{key}: {e}")
        return None


def read_json_from_s3(bucket: str, key: str) -> pd.DataFrame | None:
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return None
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        
        # Handle JSONL format (one JSON object per line) from Spark streaming
        try:
            # First try to read as JSONL
            text = body.decode('utf-8')
            lines = text.strip().split('\n')
            if lines and lines[0]:  # Check if file is not empty
                # Parse each line as a separate JSON object
                json_objects = []
                for line in lines:
                    line = line.strip()
                    if line:  # Skip empty lines
                        try:
                            json_obj = json.loads(line)
                            json_objects.append(json_obj)
                        except json.JSONDecodeError as e:
                            logging.warning(f"Skipping invalid JSON line: {e}")
                            continue
                
                if json_objects:
                    df = pd.DataFrame(json_objects)
                    return df
                else:
                    logging.warning(f"No valid JSON objects found in {key}")
                    return pd.DataFrame()
            else:
                logging.warning(f"Empty file: {key}")
                return pd.DataFrame()
                
        except UnicodeDecodeError:
            logging.error(f"Could not decode file as UTF-8: {key}")
            return None
            
    except Exception as e:
        logging.error(f"Error reading json from s3://{bucket}/{key}: {e}")
        return None


def get_latest_table_dataframe(file_type ,bucket: str, table_name: str) -> tuple[pd.DataFrame | None, str | None]:

    if file_type == 'parquet':
        latest_key = get_latest_parquet_key(bucket=bucket, table_name=table_name)
    else:
        latest_key = get_latest_json_key(bucket=bucket, table_name=table_name)
    if not latest_key:
        logging.info(f"No parquet found for table {table_name}")
        return None, None
    if file_type == 'parquet':
        df = read_parquet_from_s3(bucket=bucket, key=latest_key)
    else:
        df = read_json_from_s3(bucket=bucket, key=latest_key)
    return df, latest_key





def get_latest_json_key(bucket: str, table_name: str, base_prefix: str = 'bronze_layer/stream_job') -> str | None:

    try:
        s3_client = get_s3_client()
        if not s3_client:
            return None

        prefix = f"{base_prefix}/{table_name}/"
        latest_obj = None
        latest_ts = None
        for obj in list_objects(s3_client, bucket, prefix):
            key = obj['Key']
            if not key.endswith('.json'):
                continue
            last_modified = obj.get('LastModified')
            if latest_ts is None or (last_modified and last_modified > latest_ts):
                latest_ts = last_modified
                latest_obj = key

        return latest_obj
    except Exception as e:
        logging.error(f"Error getting latest json key for {table_name}: {e}")
        return None
