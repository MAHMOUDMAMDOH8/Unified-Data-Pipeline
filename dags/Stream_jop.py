from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
import os


default_args = {
    'owner': 'Mahmoud',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


dag = DAG(
    dag_id='stream_jop',
    default_args=default_args,
    description='Produce Kafka logs and consume to S3 (stream job)',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)


PRODUCER_PATH = "/opt/airflow/dags/scripts/Stream/Producer.py"
CONSUMER_PATH = "/opt/airflow/dags/scripts/Stream/Consumer.py"


task_env = {
    'KAFKA_TOPIC': os.getenv('KAFKA_TOPIC', 'Stock'),
    'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
    'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64',
    'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID', 'test'),
    'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
    'AWS_DEFAULT_REGION': os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
    'S3_ENDPOINT_URL': os.getenv('S3_ENDPOINT_URL'),
    'S3_VERIFY_SSL': os.getenv('S3_VERIFY_SSL', 'false'),
    'S3_ADDRESSING_STYLE': os.getenv('S3_ADDRESSING_STYLE', 'path'),
    'DATA_LAKE_BUCKET': os.getenv('DATA_LAKE_BUCKET', 'incircl'),
    'STREAM_BRONZE_PREFIX': os.getenv('STREAM_BRONZE_PREFIX', 'bronze_layer/stream_job/'),
}


produce_logs = BashOperator(
    task_id='produce_logs',
    bash_command=f"python {PRODUCER_PATH}",
    env=task_env,
    dag=dag,
)


consume_to_s3 = BashOperator(
    task_id='consume_to_s3',
    bash_command=f"python {CONSUMER_PATH}",
    env=task_env,
    dag=dag,
)


produce_logs >> consume_to_s3
