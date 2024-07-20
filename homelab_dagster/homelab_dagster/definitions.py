from dagster import load_assets_from_package_module
from dagster import Definitions
from dagster import EnvVar

from homelab_dagster import assets
from homelab_dagster.resources.gtfs.gtfs import GTFSRTResource
from homelab_dagster.resources.postgres.postgres import PostgresResource
from homelab_dagster import dbt


defs = Definitions(
    assets=load_assets_from_package_module(assets),
    resources={
        'dbt_cli': dbt,
        'api_gtfs_rt': GTFSRTResource(api_key=EnvVar('GTFS__API_KEY')),
        'postgres': PostgresResource(sqlalchemy_connection_string=EnvVar('POSTGRES__SQLALCHEMY_DATABASE_URI')),
    }
)
