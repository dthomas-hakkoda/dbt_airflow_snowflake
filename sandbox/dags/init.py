from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,7,1),
    'retries': 0
}


with DAG('1_test_configuration', default_args=default_args, schedule_interval='@once') as dag:
    task_1 = BashOperator(
        task_id='test_configuration',
        bash_command='cd /dbt && dbt -h',
        env={
            'dbt_user': '{{ var.value.dbt_user }}',
            'dbt_password': '{{ var.value.dbt_password }}',
            'account': '{{ var.value.account }}',
            **os.environ
        },
        dag=dag
    )

task_1 