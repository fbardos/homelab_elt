-- is an incremental model, will only append new data
{{
    config(
        materialized='incremental',
        unique_key='_surrogate_key',
    )
}}

with src as (
    select
        datum::DATE as datum
        , publisher
        , datensatz_id::INT as datensatz_id
        , datensatz_titel
        , anzahl_klicks::INT as anzahl_klicks
        , anzahl_besuchende::INT as anzahl_besuchende
        , {{ dbt_utils.generate_surrogate_key(['datum', 'datensatz_id']) }} as _surrogate_key
        , _airbyte_raw_id
        , _airbyte_extracted_at
        , _airbyte_meta
    from {{ source('ktzh_metadata', 'ktzh_downloads_dataset') }}
)
select *
from src
