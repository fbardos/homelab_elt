import datetime as dt

from airflow import DAG
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from airflow.providers.docker.operators.docker import DockerOperator

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
        connection_id=Variable.get('AIRBYTE__KTZH_DOWNLOADS__POSTGRES'),
    )

    dbt_run = DockerOperator(
        task_id='run_dbt_models',
        docker_url=Variable.get('DOCKER__URL'),
        image='fbardos/lab-dbt:latest',
        network_mode='host',
        command='dbt run --profiles-dir profiles -s stgn_ktzh_downloads_dataset+'
    )

    dbt_test = DockerOperator(
        task_id='test_dbt_models',
        docker_url=Variable.get('DOCKER__URL'),
        image='fbardos/lab-dbt:latest',
        network_mode='host',
        command='dbt test --profiles-dir profiles --store-failures -s stgn_ktzh_downloads_dataset+'
    )

    el.set_downstream(dbt_run)
    dbt_run.set_downstream(dbt_test)

