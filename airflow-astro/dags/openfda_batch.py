from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.apache.livy.operators.livy import LivyOperator
# from airflow.providers.ssh.operators.ssh import SSHOperator
from docker.types import Mount


params = {
    "year" : Param("", type="string", description="Select year to extract and transform")
}

with DAG(
    # Mandatory params
    dag_id="openfda_batch_manual",
    start_date=datetime(2025,4,23),

    # Optional
    schedule=None,
    description="Extract data from openFDA drug events and clean data",
    catchup=False,

    # params
    params=params

) as dag:
    
    ingest = DockerOperator(
        task_id="fetch_batch",
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
        command="--year={{ params.year }}"
    )

    transform = LivyOperator(
        task_id="transform_batch",
        livy_conn_id="livy_default",
        file="/opt/workspace/jobs/process_raw_layer/main.py",
        args=["--only_years={{ params.year }}"],
        py_files=["/opt/workspace/jobs/process_raw_layer.zip"],
        polling_interval=5,   
    )

    ingest >> transform

    

    