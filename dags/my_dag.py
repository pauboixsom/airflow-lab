from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from functions import process

from datetime import datetime

with DAG('my_dag', start_date=datetime(2021, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    task_a = PythonOperator(
        task_id="task_a",
        python_callable=process,
        #op_kwargs=Variable.get("my_dag_settings", deserialize_json=True)
        op_kwargs={
            "filename": "{{ var.value.my_dag_filename }}",
            "path": "{{ var.value.my_dag_path }}"
        }
    )
