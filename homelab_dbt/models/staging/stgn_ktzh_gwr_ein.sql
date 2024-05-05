{{
    config(
      materialized='incremental',
      unique_key='_surrogate_key'
    )
}}

with src as (
    select
        "Stichtag"::DATE as stichtag
        , "Postleitzahl" as plz
        , "Postleitzahl_Name" as plz_name
        , "Strassenbezeichnung" as strassenbezeichnung
        , "E_Eingangskoordinate" as ein_koord_e
        , "N_Eingangskoordinate" as ein_koord_n
        , "Eingangsnummer_Gebaeude" as eingangsnummer_gebaeude
        , "Offizielle_Adresse_Code" as offizielle_adresse_code
        , "Strassenbezeichnung_kurz" as strassenbezeichnung_kurz
        , "Postleitzahl_Zusatzziffer" as plz_zusatz
        , "Strassenbezeichnung_Index" as strassenbezeichnung_index
        , "Offizielle_Adresse_Bezeichnung" as offizielle_adresse_bezeichnung
        , "Strassenbezeichnung_Sprache_Code" as strassenbezeichnung_sprache_code
        , "Strassenbezeichnung_offiziell_Code" as strassenbezeichnung_offiziell_code
        , "Eidgenoessischer_Eingangsidentifikator" as eged
        , "Eidgenoessischer_Gebaeudeidentifikator" as egid
        , "Eidgenoessischer_Strassenidentifikator" as esid
        , "Strassenbezeichnung_Sprache_Bezeichnung" as strassenbezeichnung_sprache
        , "Strassenbezeichnung_offiziell_Bezeichnung" as strassenbezeichnung_offiziell
        , "Eidgenoessischer_Gebaeudeadressidentifikator" as egaid
        , _airbyte_raw_id
        , _airbyte_extracted_at
        , _airbyte_meta
    from {{ source('ktzh_gwr', 'ktzh_gwr_entrance') }} whg
)
, intm as (
    select
        stichtag
        , plz
        , plz_name
        , strassenbezeichnung
        , ein_koord_e
        , ein_koord_n
        , eingangsnummer_gebaeude
        , offizielle_adresse_code
        , strassenbezeichnung_kurz
        , plz_zusatz
        , strassenbezeichnung_index
        , offizielle_adresse_bezeichnung
        , strassenbezeichnung_sprache_code
        , strassenbezeichnung_offiziell_code
        , eged
        , egid
        , esid
        , strassenbezeichnung_sprache
        , strassenbezeichnung_offiziell
        , egaid
        , {{ dbt_utils.generate_surrogate_key(['egid', 'eged', 'stichtag']) }} as _surrogate_key
        , _airbyte_raw_id
        , _airbyte_extracted_at
        , _airbyte_meta
    from src
)
select *
from intm
