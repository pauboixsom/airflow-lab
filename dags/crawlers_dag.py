import datetime

from airflow import DAG
#from airflow.example_dags.plugins.workday import AfterWorkdayTimetable
from monday_friday import BeforeWorkdayTimetable
#from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

def _process(data_interval_start, data_interval_end,path, filename):
    print(f"Start: {data_interval_start} end: {data_interval_end}!")
    print(f"Process: {path} {filename} DONE!")

with DAG(
    dag_id="AAAexample_before_workday_timetable_dag",
    start_date=datetime.datetime(2022, 3, 20),
    timetable=BeforeWorkdayTimetable(),
    tags=["examplePau", "timetableCustom"],
) as dag:
    task_a = PythonOperator(
        task_id="task_a",
        python_callable=_process,
        #op_kwargs=Variable.get("my_dag_settings", deserialize_json=True)
        provide_context=True,
        op_kwargs={
            ""
            "filename": "{{ var.value.my_dag_filename }}",
            "path": "{{ var.value.my_dag_path }}"
        }
    )