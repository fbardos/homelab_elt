""" Downloads data from STATATLAS and saves it as one big table.

dam-api des BFS:
- STATATLAS has bfs.articleModel.code == KM10 and bfs.articleModel.id == 900052

"""
import logging
import urllib
from dataclasses import dataclass

import pandas as pd
import tqdm

from python_docker_operator.interface import ConnectionInterface


def main():
    
    dataframes = []
    skips = 0
    max_found = 0

    # Fetch dataframes by brute force
    # From last exports, max(MAP_ID) was 27642
    for i in tqdm.tqdm(range(30_000), mininterval=10, desc='Loading asset dataframes'):
        try:
            dataframes.append(pd.read_csv(f'https://www.atlas.bfs.admin.ch/core/projects/13/xshared/csv/{i}_131.csv', sep=';'))
            max_found = i
        except urllib.error.HTTPError:
            skips += 1
            continue

    logging.info(f'Loaded dataframes, skipped {skips} assets, highest ID found {max_found}.')
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
