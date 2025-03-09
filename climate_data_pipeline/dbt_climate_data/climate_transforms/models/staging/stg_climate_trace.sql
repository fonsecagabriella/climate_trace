{{ config(materialized='view') }}

SELECT
    country,
    year,
    CAST(co2 AS NUMERIC) AS co2_emissions,
    CAST(ch4 AS NUMERIC) AS ch4_emissions,
    CAST(n2o AS NUMERIC) AS n2o_emissions,
    CAST(total AS NUMERIC) AS total_emissions,
    CAST(energy_percent AS NUMERIC) AS energy_emissions_percent
FROM {{ source('raw_data', 'climate_trace_emissions_2016') }}