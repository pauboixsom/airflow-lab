from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import datetime

#pendent decidir el start date i el catchup
@dag(start_date=datetime(2021, 1, 1), schedule_interval='@daily', catchup=False)
def hs_conversations_dag():

    get_conversations_task = DockerOperator(
        task_id='hs_conversations',
        container_name='hs_conversations_container',
        image='somenergia-indicadors-KPIs:v1.0.0',
        command='python3 datasources/helpscout/hs_get_conversations.py "{{ data_interval_start }}" "{{ data_interval_end }}"',
        docker_url='unix://var/run/docker.sock',
        network_mode='host', #import per connectar a bd?
        auto_remove=True,
    )

    get_conversations_task

dag = hs_conversations_dag()