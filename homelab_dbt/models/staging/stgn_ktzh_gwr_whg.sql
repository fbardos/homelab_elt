with src as (
    select *
    from {{ source('ktzh_gwr', 'ktzh_gwr_apartments') }} geb
)
select *
from src
