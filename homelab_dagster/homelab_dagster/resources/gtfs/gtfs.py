import requests
import pandas as pd
import json
from typing import Tuple

import time
from google.transit import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict
import requests
import pandas as pd
from sqlalchemy import Column, INTEGER

from dagster import asset, AssetObservation, OpExecutionContext, op, MaterializeResult, MetadataValue, ConfigurableResource, Definitions, EnvVar, Output, load_assets_from_package_module

class GTFSResource(ConfigurableResource):
    api_key: str
    _BASE_URL: str = ''
    _COMPLEX_TEST: Tuple[Column, ...]

# First, define a resource to access the API
class GTFSRTResource(GTFSResource):
    api_key: str
    _BASE_URL = 'https://api.opentransportdata.swiss/gtfsrt2020'
    _COMPLEX_TEST = (
        Column('id', INTEGER),
        Column('name', INTEGER),
    )


    def request(self) -> dict:
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(
            self._BASE_URL,
            headers={
                'Authorization': self.api_key,
            }
        )
        feed.ParseFromString(response.content)
        data = protobuf_to_dict(feed)
        data['header']['_import_timestamp'] = time.time()

        # Sometimes the API returns empty stop_time_update (rarely, but it happens)
        data['entity'] = [
            entity for entity in data['entity']
            if entity.get('trip_update', {}).get('stop_time_update') is not None
        ]
        return data

