import datetime

from airflow import DAG
#from airflow.example_dags.plugins.workday import AfterWorkdayTimetable
from weekday_time import AfterWorkdayTimetable
from airflow.operators.dummy import DummyOperator


with DAG(
    dag_id="example_after_workday_timetable_dag",
    start_date=datetime.datetime(2021, 3, 10),
    timetable=AfterWorkdayTimetable(),
    tags=["examplePau", "timetableCustom"],
) as dag:
    t1 = DummyOperator(task_id='test')