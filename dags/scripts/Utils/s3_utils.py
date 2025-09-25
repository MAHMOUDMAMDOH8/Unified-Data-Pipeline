import logging
import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError
from urllib3.exceptions import InsecureRequestWarning
import urllib3
import pandas as pd
from io import BytesIO

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def get_s3_client():
    try:
        # Read configuration from environment variables to support S3-compatible services
        # Endpoint precedence: S3_ENDPOINT_URL > AWS_S3_ENDPOINT > MINIO_ENDPOINT_URL > ENDPOINT
        endpoint_url = (
            os.getenv('S3_ENDPOINT_URL')
            or os.getenv('AWS_S3_ENDPOINT')
            or os.getenv('MINIO_ENDPOINT_URL')
            or os.getenv('ENDPOINT')
        )

        # SSL verification control (default true). Accept common truthy/falsey strings
        verify_env_value = os.getenv('S3_VERIFY_SSL', 'true').strip().lower()
        verify = verify_env_value in ('1', 'true', 't', 'yes', 'y')

        # Optionally suppress urllib3 insecure request warnings when verify is False
        suppress_warn_env = os.getenv('S3_SUPPRESS_INSECURE_WARNING', 'true').strip().lower()
        suppress_insecure_warning = suppress_warn_env in ('1', 'true', 't', 'yes', 'y')
        if not verify and suppress_insecure_warning:
            urllib3.disable_warnings(InsecureRequestWarning)

        # Addressing style: 'path' is preferred for many S3-compatible services
        addressing_style = os.getenv('S3_ADDRESSING_STYLE', 'path').strip().lower()
        if addressing_style not in ('auto', 'path', 'virtual'):
            addressing_style = 'path'

        s3_config = Config(s3={'addressing_style': addressing_style})

        # Credentials/region (fallback to env used by AWS SDK)
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

