import requests
import pandas as pd
import json
from typing import Tuple

import time
from google.transit import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict
import requests
import pandas as pd

from dagster import asset, AssetObservation, OpExecutionContext, op, MaterializeResult, MetadataValue, ConfigurableResource, Definitions, EnvVar, Output, load_assets_from_package_module
from dagster import MaterializeResult
from dagster_first_steps.resources.gtfs import GTFSRTResource
from dagster_first_steps.resources.postgres import PostgresResource


@asset(
    resource_defs={
        'api': GTFSRTResource(api_key=EnvVar('GTFS__API_KEY')),
        'db': PostgresResource(sqlalchemy_connection_string=EnvVar('POSTGRES__SQLALCHEMY_DATABASE_URI')),
    },
    group_name='gtfs',
)
def data_from_gtfs_rt(api: GTFSRTResource, db: PostgresResource) -> MaterializeResult:
    data = api.request()
    df = pd.json_normalize(
        data,
        record_path=['entity', 'trip_update', 'stop_time_update'],
        meta=[
            ['entity', 'id'],
            ['entity', 'trip_update', 'trip', 'route_id'],
            ['entity', 'trip_update', 'trip', 'schedule_relationship'],
            ['entity', 'trip_update', 'trip', 'start_date'],
            ['entity', 'trip_update', 'trip', 'start_time'],
            ['entity', 'trip_update', 'trip', 'trip_id'],
        ],
        errors='ignore',
    )
    engine = db.get_sqlalchemy_engine()
    # with engine.connect() as conn:
    df.to_sql('gtfsrt', engine, schema='src', if_exists='replace', index=False)
 
    return MaterializeResult(
        metadata={
            'num_records': len(df),
            'preview': MetadataValue.md(df.head().to_markdown()),
        }
    )

