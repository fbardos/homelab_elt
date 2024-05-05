from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
import datetime as dt
from airflow.providers.docker.operators.docker import DockerOperator

from python_docker_operator.operator import PythonDockerOperator

with DAG(
    dag_id='ktzh_download_statistics',
    schedule_interval='0 5 * * *',
    start_date=dt.datetime(2024, 5, 1),
    max_active_runs=1,
    catchup=False,
    default_args=dict(
        email='fabian@bardos.dev',
        email_on_failure=True,
    ),
    tags=['ogd', 'ktzh', 'metadata', 'statistics'],
) as dag:

    el = AirbyteTriggerSyncOperator(
        task_id='extract_load_ktzh_downloads',
        airbyte_conn_id='lab_airbyte',
        connection_id='4becdaa8-8021-4f1f-b2f4-5da2434a5ac1',
    )

    dbt_run = DockerOperator(
        task_id='run_dbt_models',
        image='fbardos/lab-dbt:latest',
        network_mode='host',
        command='dbt run --profiles-dir profiles -s stgn_ktzh_downloads_dataset+'
    )

    dbt_test = DockerOperator(
        task_id='test_dbt_models',
        image='fbardos/lab-dbt:latest',
        network_mode='host',
        command='dbt test --profiles-dir profiles --store-failures -s stgn_ktzh_downloads_dataset+'
    )

    el.set_downstream(dbt_run)
    dbt_run.set_downstream(dbt_test)

