{{ config(materialized='view') }}

SELECT
    country,
    year,
    total_emissions,
    co2_emissions,
    population,
    gdp_per_capita,
    total_emissions_per_capita,
    --gdp_per_emission_unit,
    income_category,
    emissions_category
FROM {{ ref('climate_economic_analysis') }}
WHERE country IN (
    -- Top emitting countries (this is just an example - adjust as needed)
    SELECT country 
    FROM {{ ref('climate_economic_analysis') }}
    ORDER BY total_emissions DESC
    LIMIT 20
)
ORDER BY country, year