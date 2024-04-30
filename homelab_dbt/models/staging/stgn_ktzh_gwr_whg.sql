with src as (
    select
        "Stichtag"::DATE as stichtag
        , "Anzahl_Zimmer"::INT as anzahl_zimmer
        , "Stockwerk_Code"::INT as stockwerk_code
        , "Wohnungsflaeche" as wohnungsflaeche
        , "Baujahr_der_Wohnung"::INT as whg_baujahr
        , "Wohnungsstatus_Code"::INT as whg_status_id
        , "Wohnungsstatus_Bezeichnung" as whg_status
        , "Kocheinrichtung_Code"::INT as whg_kocheinrichtung_id
        , "Kocheinrichtung_Bezeichnung" as whg_kocheinrichtung
        , "Stockwerk_Bezeichnung" as whg_stockwerk_bezeichnung
        , "Lage_auf_dem_Stockwerk" as lage_auf_dem_stockwerk
        , "Abbruchjahr_der_Wohnung"::INT as whg_abbruchjahr
        , "Physische_Wohnungsnummer"::INT as physische_wohnungsnummer
        , "Mehrgeschossige_Wohnung_Code"::INT as mehrgeschossige_wohnung_id
        , "Mehrgeschossige_Wohnung_Bezeichnung" as mehrgeschossige_wohnung
        , "Administrative_Wohnungsnummer" as administrative_wohnungsnummer
        , "Eidgenoessischer_Eingangsidentifikator"::INT as eged
        , "Eidgenoessischer_Gebaeudeidentifikator"::INT as egid
        , "Eidgenoessischer_Wohnungsidentifikator"::INT as ewid
        , _airbyte_raw_id
        , _airbyte_extracted_at
        , _airbyte_meta
    from {{ source('ktzh_gwr', 'ktzh_gwr_apartments') }} whg
)
, intm as (
    select
        stichtag
        , anzahl_zimmer
        , stockwerk_code
        , wohnungsflaeche
        , whg_baujahr
        , whg_status_id
        , whg_status
        , whg_kocheinrichtung_id
        , {{ remap_ja_nein_boolean('whg_kocheinrichtung') }}
        , whg_stockwerk_bezeichnung
        , lage_auf_dem_stockwerk
        , whg_abbruchjahr
        , physische_wohnungsnummer
        , mehrgeschossige_wohnung_id
        , {{ remap_ja_nein_boolean('mehrgeschossige_wohnung') }}
        , administrative_wohnungsnummer
        , eged
        , egid
        , ewid
        , _airbyte_raw_id
        , _airbyte_extracted_at
        , _airbyte_meta
    from src

)
select *
from intm
