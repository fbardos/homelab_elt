import datetime as dt

from airflow import DAG

from python_docker_operator.operator import PythonDockerOperator

with DAG(
    dag_id='bfs_statatlas',
    schedule_interval='0 4 * * 6',
    start_date=dt.datetime(2024, 1, 1),
    max_active_runs=1,
    catchup=False,
    default_args=dict(
        email='fabian@bardos.dev',
        email_on_failure=True,
    ),
    tags=['ogd', 'bfs', 'statatlas'],
) as dag:

    elt = PythonDockerOperator(
        task_id='elt',
        image='fbardos/bfs_statatlas:latest',
        custom_file_path='run_bfs_statatlas.py',
        custom_connection_ids=['lab_postgis']
    )

    elt
