{{ config(materialized='view') }}

SELECT
    country,
    year,
    -- World Bank indicators
    CAST(sp_pop_totl AS NUMERIC) AS population,
    CAST(ny_gdp_pcap_cd AS NUMERIC) AS gdp_per_capita,
    --CAST(en_atm_co2e_pc AS NUMERIC) AS co2_emissions_per_capita,
    CAST(sp_dyn_le00_in AS NUMERIC) AS life_expectancy,
    CAST(se_sec_enrr AS NUMERIC) AS school_enrollment,
    CAST(si_pov_gini AS NUMERIC) AS gini_index,
    CAST(sl_uem_totl_zs AS NUMERIC) AS unemployment_rate,
    -- Climate Trace indicators
    CAST(co2 AS NUMERIC) AS co2_emissions,
    CAST(ch4 AS NUMERIC) AS ch4_emissions,
    CAST(n2o AS NUMERIC) AS n2o_emissions,
    CAST(total AS NUMERIC) AS total_emissions,
    CAST(energy_percent AS NUMERIC) AS energy_emissions_percent
FROM {{ source('raw_data', 'combined_climate_economic') }}
WHERE country IS NOT NULL