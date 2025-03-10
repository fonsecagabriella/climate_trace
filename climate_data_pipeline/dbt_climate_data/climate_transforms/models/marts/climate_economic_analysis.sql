{{ config(materialized='table') }}

WITH combined_data AS (
    SELECT * FROM {{ ref('stg_combined_climate_economic') }}
)

SELECT
    country,
    year,
    population,
    gdp_per_capita,
    --co2_emissions_per_capita,
    total_emissions,
    co2_emissions,
    energy_emissions_percent,

    -- Calculate additional metrics
    ROUND(total_emissions / NULLIF(population, 0), 4) AS total_emissions_per_capita,
    --ROUND(gdp_per_capita / NULLIF(co2_emissions_per_capita, 0), 2) AS gdp_per_emission_unit,

    -- Economic and emissions categorization
    CASE
        WHEN gdp_per_capita > 20000 THEN 'High income'
        WHEN gdp_per_capita > 5000 THEN 'Middle income'
        ELSE 'Low income'
    END AS income_category,

    CASE
        WHEN total_emissions > 1000000000 THEN 'Very high emissions'
        WHEN total_emissions > 100000000 THEN 'High emissions'
        WHEN total_emissions > 10000000 THEN 'Medium emissions'
        ELSE 'Low emissions'
    END AS emissions_category

FROM combined_data
WHERE
    population > 0
    AND total_emissions IS NOT NULL
    AND gdp_per_capita IS NOT NULL