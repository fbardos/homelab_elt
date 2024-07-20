from homelab_dagster.definitions import defs

import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets
from dagster_dbt import DbtCliResource
from dagster_dbt import DbtProject

dbt_path = os.getenv('DBT__ABSOLUTE_PATH')
assert dbt_path is not None
homelab_dbt = DbtProject(project_dir=dbt_path)
dbt = DbtCliResource(project_dir=homelab_dbt)

if os.getenv('DAGSTER_DBT_PARSE_PROJECT_ON_LOAD'):
    dbt_manifest_path = (
        dbt.cli(
            ["--quiet", "parse"],
            target_path=Path("target")

        )
        .wait()
        .target_path.joinpath('manifest.json')
    )
else:
    assert dbt_path is not None
    dbt_manifest_path = Path(dbt_path).joinpath("target", "manifest.json")


@dbt_assets(
    manifest=dbt_manifest_path,
    # exclude='config.materialized:snapshot'
)
def assets_homelab_dbt(context: AssetExecutionContext, dbt_cli: DbtCliResource):
    yield from dbt_cli.cli(["build"], context=context).stream()

# from dagster import Definitions, load_assets_from_modules

# from . import assets

# all_assets = load_assets_from_modules([assets])

# defs = Definitions(
    # assets=all_assets,
# )
