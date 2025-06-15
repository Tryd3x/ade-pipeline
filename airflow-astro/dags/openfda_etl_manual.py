from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.utils.log.logging_mixin import LoggingMixin

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.operators.python import PythonOperator

from docker.types import Mount

from google.cloud import storage, bigquery

logger = LoggingMixin().log
params = {
    "year" : Param("", type="string", description="Enter years seperated by ',' to perform ETL on"),
    "max_batch_size_mb" : Param(13000, type="integer", description="Enter maximum size of each batch in MB")
}

# Helper functions
def get_year(blob):
    return blob.name.split("/")[3]

def scan_years(blobs): 
    return list({get_year(blob) for blob in blobs})

def update_uris(bucket, schema, years):
    return list(f"gs://{bucket}/cleaned/pq/{schema}/{year}/*.parquet" for year in years)


BUCKET_NAME = "ade-pipeline-bucket"
DATASET_ID = "ade_external"
SCHEMA = ['patient', 'reaction', 'drug']

def update_external_table_uris():
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    # Update uris of external table per schema
    for s in SCHEMA:
        blobs = storage_client.list_blobs(BUCKET_NAME, prefix=f"cleaned/pq/{s}")
        years = scan_years(blobs)

        # Setting Configuration
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = update_uris(BUCKET_NAME, s, years)
        external_config.autodetect = True

        table_ref = f"{bigquery_client.project}.{DATASET_ID}.ext_{s}"
        try:
            # Check if table exists
            table = bigquery_client.get_table(table_ref)
            table.external_data_configuration = external_config
            bigquery_client.update_table(table, ['external_data_configuration'])
            logger.info(f"Table updated: {table_ref}")
        except Exception as e:
            logger.warning(f"Table not found: {table_ref}")
            logger.info(f"Creating new table: {table_ref}")

            new_table = bigquery.Table(table_ref)
            new_table.external_data_configuration = external_config
            table = bigquery_client.create_table(new_table)

            logger.info(f"Created table: {table.project}.{table.dataset_id}.{table.table_id}")

with DAG(
    # Mandatory params
    dag_id="openfda_etl",
    start_date=datetime(2025,4,23),

    # Optional
    schedule=None,
    description="ETL for openFDA drug events",
    catchup=False,

    # params
    params=params

) as dag:
    
    ingest = DockerOperator(
        task_id="ingest_batch",
        container_name="openfda-ingest",
        image="ade-pipeline/openfda:latest",
        docker_url="unix:///var/run/docker.sock",
        auto_remove="success",
        network_mode="shared_network",
        mounts=[
            Mount(source='/home/hyderreza/codehub/ade-pipeline/keys/gcs-credentials.json',target='/app/gcs-credentials.json',type='bind',read_only=True)
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcs-credentials.json"
        },
        command="--year={{ params.year }} --metrics_gateway=pushgateway:9091 --max_batch_size_mb={{ params.max_batch_size_mb }}"
    )

    transform = LivyOperator(
        task_id="transform_batch",
        livy_conn_id="livy_default",
        file="/opt/workspace/jobs/process_raw_layer/main.py",
        args=["--year={{ params.year }}"],
        py_files=["/opt/workspace/jobs/process_raw_layer.zip"],
        polling_interval=5,   
    )

    sync_bq = PythonOperator(
        task_id="sync_bq_external_tables",
        python_callable=update_external_table_uris,
    )

    build_dbt = DockerOperator(
        task_id="build_dbt_models",
        container_name="ade-dbt",
        image="dbt-base:latest",
        docker_url="unix:///var/run/docker.sock",
        auto_remove="success",
        network_mode="shared_network",
        mounts=[
            Mount(source="/home/hyderreza/codehub/ade-pipeline/dbt/volumes/ade_pipeline_dbt",target='/opt/dbt',type='bind'), # dbt project
            Mount(source='/home/hyderreza/codehub/ade-pipeline/dbt/volumes/dbt_profiles',target='/root/.dbt',type='bind'), # dbt profile
            Mount(source='/home/hyderreza/codehub/ade-pipeline/keys/gcs-credentials.json',target='/app/gcs-credentials.json',type='bind',read_only=True) # gcs service account credentials
        ],
        command=[
            "run",
            "--profiles-dir", "/root/.dbt",
            "--project-dir", "/opt/dbt",
            "--full-refresh"
        ]
    )

    ingest >> transform >> sync_bq >> build_dbt