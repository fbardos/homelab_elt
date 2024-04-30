with src as (
    select
        "BFS_NR"::INT as gemeinde_bfs_id
        , "Gemeindename" as gemeinde
        , "Kanton" as kanton
        , "Stichtag"::DATE as stichtag
        , "Eidgenoessischer_Gebaeudeidentifikator"::INT as egid
        , "Eidgenoessischer_Grundstuecksidentifikator" as eggi
        , "Bauperiode_Code"::INT as bauperiode_id
        , "Bauperiode_Bezeichnung" as bauperiode
        , "Gebaeudeflaeche" as geb_flaeche
        , "Gebaeudevolumen" as geb_volumen
        , "Anzahl_Geschosse"::INT as anzahl_geschosse
        , "Anzahl_Wohnungen"::INT as anzahl_wohnungen
        , "Grundstuecksnummer" as grundstuecksnummer
        , "Name_des_Gebaeudes" as gebaeude_name
        , "Gebaeudeklasse_Code"::INT as gebaeudeklasse_id
        , "Gebaeudeklasse_Bezeichnung" as gebaeudeklasse
        , "Gebaeudestatus_Code"::INT as gebaeudestatus_id
        , "Gebaeudestatus_Bezeichnung" as gebaeudestatus
        , "E_Gebaeudekoordinate" as geb_koord_e
        , "N_Gebaeudekoordinate" as geb_koord_n
        , "Energiebezugsflaeche" as energiebezugsflaeche
        , "Grundbuchkreisnummer"::INT as grundbuchkreisnummer
        , "Zivilschutzraum_Code" as zivilschutzraum_id
        , "Zivilschutzraum_Bezeichnung" as zivilschutzraum
        , "Baujahr_des_Gebaeudes"::INT as geb_baujahr
        , "Baumonat_des_Gebaeudes"::INT as geb_baumonat
        , "Gebaeudekategorie_Code"::INT as gebaeudekategorie_id
        , "Gebaeudekategorie_Bezeichnung" as gebaeudekategorie
        , "Amtliche_Gebaeudenummer" as amtliche_gebaeudenummer
        , "Koordinatenherkunft_Code"::INT as koordinatenherkunft_id
        , "Koordinatenherkunft_Bezeichnung" as koordinatenherkunft
        , "Abbruchjahr_des_Gebaeudes"::INT as geb_abbruchjahr
        , "Gebaeudevolumen_Norm_Code" as gebaeudevolumen_norm_id
        , "Gebaeudevolumen_Norm_Bezeichnung" as gebaeudevolumen_norm
        , "Anzahl_separate_Wohnraeume" as anzahl_separate_wohnraeume
        , "Waermeerzeuger_Heizung_primaer_Code" as waermeerzeuger_heizung_primaer_id
        , "Waermeerzeuger_Heizung_primaer_Bezeichnung" as waermeerzeuger_heizung_primaer
        , "Aktualisierungsdatum_Heizung_primaer" as aktualisierungsdatum_heizung_primaer
        , "Waermeerzeuger_Heizung_sekundaer_Code" as waermeerzeuger_heizung_sekundaer_id
        , "Waermeerzeuger_Heizung_sekundaer_Bezeichnung" as waermeerzeuger_heizung_sekundaer
        , "Aktualisierungsdatum_Heizung_sekundaer" as aktualisierungsdatum_heizung_sekundaer
        , "Waermeerzeuger_Warmwasser_primaer_Code" as waermeerzeuger_warmwasser_primaer_id
        , "Waermeerzeuger_Warmwasser_primaer_Bezeichnung" as waermeerzeuger_warmwasser_primaer
        , "Aktualisierungsdatum_Warmwasser_primaer" as aktualisierungsdatum_warmwasser_primaer
        , "Informationsquelle_Heizung_primaer_Code" as informationsquelle_heizung_primaer_id
        , "Informationsquelle_Heizung_primaer_Bezeichnung" as informationsquelle_heizung_primaer
        , "Waermeerzeuger_Warmwasser_sekundaer_Code" as waermeerzeuger_warmwasser_sekundaer_id
        , "Waermeerzeuger_Warmwasser_sekundaer_Bezeichnung" as waermeerzeuger_warmwasser_sekundaer
        , "Aktualisierungsdatum_Warmwasser_sekundaer" as aktualisierungsdatum_warmwasser_sekundaer
        , "Informationsquelle_Heizung_sekundaer_Code" as informationsquelle_heizung_sekundaer_id
        , "Informationsquelle_Heizung_sekundaer_Bezeichnung" as informationsquelle_heizung_sekundaer
        , "Energie__Waermequelle_Heizung_primaer_Code" as energie_waermequelle_heizung_primaer_id
        , "Energie__Waermequelle_Heizung_primaer_Bezeichnung" as energie_waermequelle_heizung_primaer
        , "Informationsquelle_Warmwasser_primaer_Code" as informationsquelle_warmwasser_primaer_id
        , "Informationsquelle_Warmwasser_primaer_Bezeichnung" as informationsquelle_warmwasser_primaer
        , "Informationsquelle_zum_Gebaeudevolumen_Code" as informationsquelle_zum_gebaeudevolumen_id
        , "Informationsquelle_zum_Gebaeudevolumen_Bezeichnung" as informationsquelle_zum_gebaeudevolumen
        , "Energie__Waermequelle_Heizung_sekundaer_Code" as energie_waermequelle_heizung_sekundaer_id
        , "Energie__Waermequelle_Heizung_sekundaer_Bezeichnung" as energie_waermequelle_heizung_sekundaer
        , "Informationsquelle_Warmwasser_sekundaer_Code" as informationsquelle_warmwasser_sekundaer_id
        , "Informationsquelle_Warmwasser_sekundaer_Bezeichnung" as informationsquelle_warmwasser_sekundaer
        , "Energie__Waermequelle_Warmwasser_primaer_Code" as energie_waermequelle_warmwasser_primaer_id
        , "Energie__Waermequelle_Warmwasser_primaer_Bezeichnung" as energie_waermequelle_warmwasser_primaer
        , "Energie__Waermequelle_Warmwasser_sekundaer_Code" as energie_waermequelle_warmwasser_sekundaer_id
        , "Energie__Waermequelle_Warmwasser_sekundaer_Bezeichnung" as energie_waermequelle_warmwasser_sekundaer
        , _airbyte_raw_id
        , _airbyte_extracted_at
        , _airbyte_meta
    from {{ source('ktzh_gwr', 'ktzh_gwr_houses') }} whg
)
, intm as (
    select
        *
    from src

)
select *
from intm
