from datetime import datetime
from airflow import DAG
from docker.types import Mount

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

from airflow.operators.python_operator import PythonOperator
import subprocess


# CONN_ID = '823d3a7f-27b0-471f-bafa-150937edf536'
CONN_ID = '02af16f3-0566-4fa0-9d39-d0940fe3acbe'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def run_elt_script():
    script_path = "/opt/airflow/elt_script/elt_script.py"
    result = subprocess.run(["python", script_path],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Script failed with error: {result.stderr}")
    else:
        print(result.stdout)
        
dag = DAG(
    'elt_and_dbt',
    default_args=default_args,
    description='An ELT workflow with dbt',
    start_date=datetime(2025, 2, 26),
    catchup=False,
)

t1 = AirbyteTriggerSyncOperator(
    task_id='airbyte_postgres_postgres',
    airbyte_conn_id='airbyte_conn',
    connection_id=CONN_ID,
    asynchronous=False,
    timeout=3600,
    wait_seconds=10,
    dag=dag
)

# t1 = PythonOperator(
#     task_id='run_elt_script',
#     python_callable=run_elt_script,
#     dag=dag,
# )

t2 = DockerOperator(
    task_id='dbt_run',
    image='ghcr.io/dbt-labs/dbt-postgres:latest',
    command=[
        "run",
        "--profiles-dir", "/root",
        "--project-dir", "/opt/dbt",
        "--full-refresh"
    ],
    auto_remove='success',
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mounts=[
        Mount(source='E:/Codehub/etl-pipeline/src/custom_postgres',
              target='/opt/dbt', type='bind'),
        Mount(source='C:/Users/hyder/.dbt', target='/root', type='bind'),
    ],
    dag=dag
)

t1 >> t2



# from airflow import DAG
# from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
# from airflow.providers.docker.operators.docker import DockerOperator
# from docker.types import Mount
# from airflow.utils.dates import days_ago

# # Replace with your actual Airbyte connection ID
# AIRBYTE_CONNECTION_ID = "823d3a7f-27b0-471f-bafa-150937edf536"

# with DAG(
#     dag_id="trigger_airbyte_sync",
#     default_args={"owner": "airflow"},
#     schedule_interval=None,  # Run manually or via external trigger
#     start_date=days_ago(1),
#     catchup=False,
#     tags=["airbyte"],
# ) as dag:
    
#     airbyte_sync = AirbyteTriggerSyncOperator(
#         task_id="trigger_airbyte_sync",
#         airbyte_conn_id="airbyte_conn",  # Airflow connection ID for Airbyte
#         connection_id=AIRBYTE_CONNECTION_ID,
#         asynchronous=False,
#         timeout=3600,  # Timeout in seconds
#         wait_seconds=10  # Time between status checks
#     )
    
    # dbt_run = DockerOperator(
    #     task_id='dbt_run',
    #     image='ghcr.io/dbt-labs/dbt-postgres:latest',
    #     command=[
    #         "run",
    #         "--profiles-dir", "/root",
    #         "--project-dir", "/opt/dbt",
    #         "--full-refresh"
    #     ],
    #     auto_remove='success',
    #     docker_url="unix://var/run/docker.sock",
    #     network_mode="bridge",
    #     mounts=[
    #         Mount(source='E:/Codehub/etl-pipeline/src/custom_postgres',
    #               target='/opt/dbt', type='bind'),
    #         Mount(source='C:/Users/hyder/.dbt', target='/root', type='bind'),
    #     ]
    # )
    
#     airbyte_sync >> dbt_run