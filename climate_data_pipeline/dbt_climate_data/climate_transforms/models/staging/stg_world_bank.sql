{{ config(materialized='view') }}

SELECT
    country,
    year,
    CAST(sp_pop_totl AS NUMERIC) AS population,
    CAST(ny_gdp_pcap_cd AS NUMERIC) AS gdp_per_capita,
    CAST(en_atm_co2e_pc AS NUMERIC) AS co2_emissions_per_capita,
    CAST(sp_dyn_le00_in AS NUMERIC) AS life_expectancy,
    CAST(se_sec_enrr AS NUMERIC) AS school_enrollment,
    CAST(si_pov_gini AS NUMERIC) AS gini_index,
    CAST(sl_uem_totl_zs AS NUMERIC) AS unemployment_rate
FROM {{ source('raw_data', 'world_bank_indicators_2016') }}