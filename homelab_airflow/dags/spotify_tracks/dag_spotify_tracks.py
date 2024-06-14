import datetime as dt

from airflow import DAG
from airflow.models import Variable
from docker.types.services import Mount

from python_docker_operator.operator import PythonDockerOperator

with DAG(
    dag_id='spotify_tracks',
    schedule_interval='0 17 * * 5',  # At 17:00 on Friday
    start_date=dt.datetime(2024, 1, 1),
    max_active_runs=1,
    catchup=False,
    default_args=dict(
        email='fabian@bardos.dev',
        email_on_failure=True,
    ),
    tags=['scripts', 'spotify'],
) as dag:

    script = PythonDockerOperator(
        task_id='script',
        docker_url=Variable.get('DOCKER__URL'),
        image='fbardos/spotify_tracks:latest',
        mounts=[
            Mount('/app/.cache', 'spotify-oauth-cache', type='volume'),
        ],
        custom_file_path='run_spotify_tracks.py',
        custom_variables=[
            'SPOTIFY__TRACKS_API_CLIENT',
            'SPOTIFY__TRACKS_API_SECRET',
        ]
    )

    script
