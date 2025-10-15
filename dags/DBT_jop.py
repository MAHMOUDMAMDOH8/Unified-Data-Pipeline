from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.utils.dates import days_ago
import os

default_args = {
    'owner':'ma7moud',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'dbt_orchestrator', 
    default_args=default_args,
    description='dbt_orchestrator Pipeline',
    schedule_interval='*/15 * * * *', 
    start_date=days_ago(1),
    catchup=False,
    tags=['dbt', 'snowflake', 'Modeling', 'Gold-layer']
)
profiles_path = '/opt/airflow/scripts/profiles.yml'
project_path = '/opt/airflow/scripts/unifide_ecomm/dbt_project.yml'

with dag:
    with TaskGroup("test_connection",dag=dag) as test_cconnection:
        dbt_test_connection =BashOperator(
            task_id = 'dbt_test_connection',
            bash_command = f"dbt debug --profiles-dir {os.path.dirname(profiles_path)} --project-dir {os.path.dirname(project_path)}"
        )

    with TaskGroup("run_snapshot",dag=dag) as dbt_run_snapshot:
        dbt_snapshot_task = BashOperator(
            task_id='dbt_snapshot',
            bash_command=f"dbt snapshot --profiles-dir {os.path.dirname(profiles_path)} --project-dir {os.path.dirname(project_path)}",
        )
    with TaskGroup("dbt_run_staging",dag=dag) as dbt_run_staging_group:
        staging_models = ['STG_category','STG_Customers','STG_Employees','STG_orders_ditals','STG_orders','STG_Products','STG_stream_event_item','STG_Users']
        dbt_run_staging_tasks = []
        for model in staging_models:
            dbt_run_task = BashOperator(
                task_id=f'dbt_run_{model}',
                bash_command=f"dbt run --profiles-dir {os.path.dirname(profiles_path)} --project-dir {os.path.dirname(project_path)} --models {model}",
            )
            dbt_run_staging_tasks.append(dbt_run_task)
    with TaskGroup('dbt_run_dimensions', dag=dag) as dbt_run_dimensions_group:
        dimension_models = ['DIM_Customers', 'DIM_Date','DIM_Employee','DIM_product','DIM_time']
        dbt_run_dimension_tasks = []
        for model in dimension_models:
            dbt_run_task = BashOperator(
                task_id=f'dbt_run_{model}',
                bash_command=f"dbt run --profiles-dir {os.path.dirname(profiles_path)} --project-dir {os.path.dirname(project_path)} --models {model}",
            )
            dbt_run_dimension_tasks.append(dbt_run_task)
    with TaskGroup('dbt_run_fact', dag=dag) as dbt_run_fact_group:
        fact_models = ['Fact_orders','Fact_user_event']
        dbt_run_fact_tasks = []
        for model in fact_models:
            dbt_run_task = BashOperator(
                task_id=f'dbt_run_fact_model_{model}',
                bash_command=f"dbt run --profiles-dir {os.path.dirname(profiles_path)} --project-dir {os.path.dirname(project_path)} --models {model}",
            )
            dbt_run_fact_tasks.append(dbt_run_task)



test_cconnection>>dbt_run_snapshot>>dbt_run_staging_group>>dbt_run_dimensions_group>>dbt_run_fact_group