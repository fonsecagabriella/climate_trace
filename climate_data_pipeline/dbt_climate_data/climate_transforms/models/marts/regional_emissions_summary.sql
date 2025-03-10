{{ config(materialized='table') }}

WITH country_data AS (
    SELECT * FROM {{ ref('climate_economic_analysis') }}
),

-- Define regions (simplified version - you might want to use a proper mapping table)
country_regions AS (
    SELECT
        country,
        CASE
            WHEN country IN ('USA', 'CAN', 'MEX') THEN 'North America'
            WHEN country IN ('FRA', 'DEU', 'GBR', 'ITA', 'ESP') THEN 'Europe'
            WHEN country IN ('CHN', 'JPN', 'IND', 'KOR') THEN 'Asia'
            ELSE 'Other'
        END AS region
    FROM (SELECT DISTINCT country FROM country_data)
)

SELECT
    r.region,
    c.year,
    COUNT(DISTINCT c.country) AS country_count,
    SUM(c.population) AS total_population,
    SUM(c.total_emissions) AS total_emissions,
    AVG(c.gdp_per_capita) AS avg_gdp_per_capita,
    SUM(c.total_emissions) / NULLIF(SUM(c.population), 0) AS emissions_per_capita,
    --AVG(c.gdp_per_emission_unit) AS avg_gdp_per_emission_unit
FROM country_data c
JOIN country_regions r ON c.country = r.country
GROUP BY r.region, c.year
ORDER BY total_emissions DESC