# Climate and Social Indicators Data Pipeline

This project implements an end-to-end data pipeline that:

- Extracts emissions data from [Climate Trace API](https://climatetrace.org/)
- Extracts social/economic indicators from [World Bank API](https://data.worldbank.org/)
- Integrates the datasets in a data warehouse
- Visualizes the data in an interactive dashboard

## Project Background
This data engineering project leverages two key global datasets: Climate Trace emissions data and World Bank socioeconomic indicators. By combining these complementary datasets, we can explore relationships between countries' carbon footprints and their social/economic development metrics.

### The Problem
Climate change analysis often lacks integration between emissions data and socioeconomic factors. Researchers and policymakers struggle to connect environmental impact with human development indicators, making it difficult to identify:

Countries achieving economic growth with lower emissions
Correlations between emissions and development metrics
Equitable approaches to emissions reduction based on development status

### The Goal
Build an end-to-end data pipeline that:

- Extracts emissions data from Climate Trace API for all countries
- Extracts socioeconomic indicators from World Bank API (population, GDP, etc.)
- Integrates these datasets into a unified data warehouse
- Creates a dashboard with visualizations showing key relationships between emissions and development indicators

**This will enable data-driven insights into the complex interplay between development and environmental impact across different countries and regions.**

## Project Components
1. Data Lake Extraction Scripts

**Climate Trace Emissions Extractor**
Extracts global emissions data by country:

`climate_claude.py` - Fetches emissions data from Climate Trace API

**World Bank Data Extractor**
Extracts population and social indicators by country:

`world_bank_extractor.py` - Fetches population, GDP, and other indicators