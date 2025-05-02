from datetime import datetime

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models.param import Param

default_args = {
    'owner': 'hyderreza',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

params = {
        "names": Param(
            ["Linda", "Martha", "Thomas"],
            type="array",
            description="Define the list of names for which greetings should be generated in the logs."
            " Please have one name per line.",
            title="Names to greet",
        )
     }

with DAG(
    dag_id="test_params",
    start_date=datetime(2025, 4, 23),
    default_args=default_args,
    schedule=None,
    description="Testing params",
    catchup=False,
    params=params,
) as dag:

    clean_data = SSHOperator(
            task_id='clean_batch_data',
            ssh_conn_id='spark_ssh',
            command="""
            {% for name in params.names %}
            echo "Hello, {{ name }}!"
            {% endfor %}
            """,
        )