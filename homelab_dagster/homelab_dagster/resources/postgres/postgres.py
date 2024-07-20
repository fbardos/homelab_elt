import requests
import pandas as pd

from dagster import asset, AssetObservation, OpExecutionContext, op, MaterializeResult, MetadataValue, ConfigurableResource, Definitions, EnvVar, Output, load_assets_from_package_module
import sqlalchemy
import psycopg2



class PostgresResource(ConfigurableResource):
    sqlalchemy_connection_string: str

    def get_sqlalchemy_engine(self):
        return sqlalchemy.create_engine(self.sqlalchemy_connection_string)

