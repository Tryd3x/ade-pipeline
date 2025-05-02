from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from docker.types import Mount

# default_args = {
#     'owner': 'hyderreza',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
# }

params = {
    "year" : Param("", type="string", description="Param to extract and transform selected year")
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
    
    ingest_data = DockerOperator(
        task_id="fetch_batch_data",
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

    clean_data = SSHOperator(
        task_id='clean_batch_data',
        ssh_conn_id='spark_ssh',
        command="""
        source /opt/workspace/env.sh && \
        spark-submit \
        --py-files /opt/workspace/jobs/process_raw_layer.zip \
        --deploy-mode client \
        /opt/workspace/jobs/process_raw_layer/main.py --only_years={{ params.year }}
        """,
    )

    ingest_data >> clean_data

    

    