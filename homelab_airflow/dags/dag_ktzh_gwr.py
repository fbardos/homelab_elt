from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
import datetime as dt
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator

from python_docker_operator.operator import PythonDockerOperator

with DAG(
    dag_id='ktzh_gwr',
    schedule_interval='5 4 10 */3 *',
    start_date=dt.datetime(2024, 4, 1),
    max_active_runs=1,
    catchup=False,
    default_args=dict(
        email='fabian@bardos.dev',
        email_on_failure=True,
    ),
    tags=['ogd', 'ktzh', 'metadata', 'statistics'],
) as dag:

    start_el = EmptyOperator(task_id='start_el')
    end_el = EmptyOperator(task_id='end_el')

    ### EL WHG
    el_whg = AirbyteTriggerSyncOperator(
        task_id='extract_load_ktzh_gwr_apartments',
        airbyte_conn_id='lab_airbyte',
        connection_id='0c377a06-d676-4411-87d1-a687c4d153ef',
    )
    el_whg.set_upstream(start_el)

    el_whg_duplicates = PostgresOperator(
        task_id='remove_duplicates_ktzh_gwr_apartments',
        postgres_conn_id='lab_postgis',
        sql="""
            DELETE FROM src.ktzh_gwr_apartments a USING (
                SELECT
                    MAX(_airbyte_extracted_at) as ctid
                    , "Stichtag" || '-' || "Eidgenoessischer_Gebaeudeidentifikator" || '-' || "Eidgenoessischer_Wohnungsidentifikator" as key
                FROM src.ktzh_gwr_apartments
                GROUP BY 2 HAVING COUNT(*) > 1
            ) b
            WHERE "Stichtag" || '-' || "Eidgenoessischer_Gebaeudeidentifikator" || '-' || "Eidgenoessischer_Wohnungsidentifikator" = b.key
            AND a._airbyte_extracted_at <> b.ctid
        """
    )
    el_whg_duplicates.set_upstream(el_whg)
    el_whg_duplicates.set_downstream(end_el)

    ### EL GEB
    el_geb = AirbyteTriggerSyncOperator(
        task_id='extract_load_ktzh_gwr_houses',
        airbyte_conn_id='lab_airbyte',
        connection_id='d0c694cf-5e58-41ff-af17-c79fcc1ed824',
    )
    el_geb.set_upstream(start_el)

    el_geb_duplicates = PostgresOperator(
        task_id='remove_duplicates_ktzh_gwr_houses',
        postgres_conn_id='lab_postgis',
        sql="""
            DELETE FROM src.ktzh_gwr_houses a USING (
                SELECT
                    MAX(_airbyte_extracted_at) as ctid
                    , "Stichtag" || '-' || "Eidgenoessischer_Gebaeudeidentifikator" as key
                FROM src.ktzh_gwr_houses
                GROUP BY 2 HAVING COUNT(*) > 1
            ) b
            WHERE "Stichtag" || '-' || "Eidgenoessischer_Gebaeudeidentifikator" = b.key
            AND a._airbyte_extracted_at <> b.ctid
        """
    )
    el_geb_duplicates.set_upstream(el_geb)
    el_geb_duplicates.set_downstream(end_el)

    ### EL EIN
    el_ein = AirbyteTriggerSyncOperator(
        task_id='extract_load_ktzh_gwr_entrances',
        airbyte_conn_id='lab_airbyte',
        connection_id='0ac48080-5617-4a1d-ba2e-0f68ddf94066',
    )
    el_ein.set_upstream(start_el)

    el_ein_duplicates = PostgresOperator(
        task_id='remove_duplicates_ktzh_gwr_entrances',
        postgres_conn_id='lab_postgis',
        sql="""
            DELETE FROM src.ktzh_gwr_entrance a USING (
                SELECT
                    MAX(_airbyte_extracted_at) as ctid
                    , "Stichtag" || '-' || "Eidgenoessischer_Gebaeudeidentifikator" || '-' || "Eidgenoessischer_Eingangsidentifikator" as key
                FROM src.ktzh_gwr_entrance
                GROUP BY 2 HAVING COUNT(*) > 1
            ) b
            WHERE "Stichtag" || '-' || "Eidgenoessischer_Gebaeudeidentifikator" || '-' || "Eidgenoessischer_Eingangsidentifikator" = b.key
            AND a._airbyte_extracted_at <> b.ctid
        """
    )
    el_ein_duplicates.set_upstream(el_ein)
    el_ein_duplicates.set_downstream(end_el)

    dbt_run = DockerOperator(
        task_id='run_dbt_models',
        image='fbardos/lab-dbt:latest',
        network_mode='host',
        command='dbt run --profiles-dir profiles -s stgn_ktzh_gwr_geb+ stgn_ktzh_gwr_whg+ stgn_ktzh_gwr_ein+'
    )
    dbt_run.set_upstream(end_el)

    dbt_test = DockerOperator(
        task_id='test_dbt_models',
        image='fbardos/lab-dbt:latest',
        network_mode='host',
        command='dbt test --profiles-dir profiles --store-failures -s stgn_ktzh_gwr_geb+ stgn_ktzh_gwr_whg+ stgn_ktzh_gwr_ein+'
    )
    dbt_test.set_upstream(dbt_run)

