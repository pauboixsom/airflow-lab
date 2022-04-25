from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import datetime

@dag(start_date=datetime(2021, 1, 1), schedule_interval='@daily', catchup=False)
def BBBBBtest_docker_dag():

    @task()
    def t1():
        pass

    t2_test2 = DockerOperator(
        task_id='t2_test2',
        container_name='task2_container_name', #perque he de canviar això tota l'estona?
        image='stock_image:v3.0.0',
        command='python3 stock_data.py "{{ data_interval_start }}" "{{ data_interval_end }}"',
        # enviroment = {
        #     'SDATE' : "{{ data_interval_start }}",
        #     'EDATE' : "{{ data_interval_end }}",
        # },
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove=True,
    )

    t1() >> t2_test2

dag = BBBBBtest_docker_dag()