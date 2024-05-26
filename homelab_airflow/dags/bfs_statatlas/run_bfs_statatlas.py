""" Downloads data from STATATLAS and saves it as one big table.

dam-api des BFS:
- STATATLAS has bfs.articleModel.code == KM10 and bfs.articleModel.id == 900052

"""
import logging
import re
import urllib
from dataclasses import dataclass

import pandas as pd
import requests
import tqdm

from python_docker_operator.interface import ConnectionInterface


@dataclass(frozen=True)
class Asset:
    dam_id: str
    statatlas_id: str
    year: int

    def __hash__(self):
        return hash(self.statatlas_id)

    def __eq__(self, other):
        return self.statatlas_id == other.statatlas_id

    # Maybe also add collection and prodigma levels as dataframe columns for later filtering
    def load_df(self) -> pd.DataFrame:
        """ Load data from STATATLAS and return as dataframe.

        URL suffix _131 returns data as german.
        """
        return pd.read_csv(f'https://www.atlas.bfs.admin.ch/core/projects/13/xshared/csv/{self.statatlas_id}_131.csv', sep=';')


def main():

    # Get assets from BFS DAM API
    response = requests.get(
        'https://dam-api.bfs.admin.ch/hub/api/dam/assets',
        params={
            'articleModel': '900052',  # KM10 STATATLAS
            'spatialdivision': '900004',  # Gemeinden
        }
    )
    data = response.json()

    # Iterate over assets (in 20er steps) and create dataclasses
    data_objs = []

    # Get data synchronously (async would lead to timeouts)
    responses = []
    logging.info(f'Loading {data["total"]} assets from BFS DAM API.')
    for skip in tqdm.tqdm(range(0, data['total'], 20), desc='Get metadata'):
        url = f'https://dam-api.bfs.admin.ch/hub/api/dam/assets?articleModel=900052&spatialdivision=900004&skip={skip}'
        response = requests.get(url)
        assert response.status_code == 200
        responses.extend(response.json().get('data', []))

    # Parse responses
    for asset in responses:
        data_objs.append(Asset(
            dam_id=asset.get('ids', {}).get('damId'),
            statatlas_id=re.search(r'KM10\-(\d+)-.*', asset.get('shop', {}).get('orderNr')).group(1),
            year=asset.get('description', {}).get('bibliography', {}).get('year')
        ))

    # Remove duplicates
    logging.info(f'API calls returned {len(data_objs)} assets.')
    data_objs = list(set(data_objs))
    logging.info(f'After removing duplicates, {len(data_objs)} assets remain.')

    # Fetch dataframes (async would lead to timeouts)
    dataframes = []
    skips = 0
    for obj in tqdm.tqdm(data_objs, desc='Loading asset dataframes'):
        try:
            dataframes.append(obj.load_df())
        except urllib.error.HTTPError:
            skips += 1
            continue
    logging.info(f'Loaded dataframes, skipped {skips} assets.')
    df = pd.concat(dataframes)

    # Remove duplicates
    df = df.drop_duplicates()

    # Write to database
    engine = ConnectionInterface('lab_postgis').sqlalchemy_engine
    with engine.begin() as conn:
        df.to_sql('bfs_statatlas', conn, schema='src', if_exists='replace', index=False)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()




