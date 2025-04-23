from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from docker.types import Mount

default_args = {
    'owner': 'hyderreza',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    # Mandatory params
    dag_id="openfda_ingestion",
    start_date=datetime(2025,4,23),

    # Optional
    default_args=default_args,
    schedule="@weekly",
    description="Extract data from openFDA drug events",
    catchup=False
) as dag:
    
    openfda_container = DockerOperator(
        task_id="run_main_script",
        container_name="openfda-ingest",
        image="ade-pipeline/openfda:latest",
        docker_url="unix:///var/run/docker.sock",
        auto_remove="success",
        mounts=[
            Mount(source='/home/hyderreza/codehub/ade-pipeline/keys/gcs-credentials.json',target='/app/gcs-credentials.json',type='bind',read_only=True)
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcs-credentials.json"
        },
    )
