{{ config(materialized='view') }}

SELECT
    income_category,
    year,
    COUNT(DISTINCT country) AS country_count,
    SUM(total_emissions) AS total_emissions,
    SUM(population) AS total_population,
    AVG(gdp_per_capita) AS avg_gdp_per_capita,
    AVG(total_emissions_per_capita) AS avg_emissions_per_capita
FROM {{ ref('climate_economic_analysis') }}
GROUP BY income_category, year
ORDER BY total_emissions DESC