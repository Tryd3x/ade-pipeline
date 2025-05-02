from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(
    dag_id="openfda_spark_ssh_clean",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    ssh_submit_spark_job = SSHOperator(
        task_id='submit_spark_job',
        ssh_conn_id='spark_ssh',
        command=f"""
        source /opt/workspace/env.sh && \
        spark-submit \
        --py-files /opt/workspace/jobs/process_raw_layer.zip \
        --deploy-mode client \
        /opt/workspace/jobs/process_raw_layer/main.py --only_years=2004
        """,
    )