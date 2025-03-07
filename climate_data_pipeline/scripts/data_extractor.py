import requests
import pandas as pd
import os
import time

def fetch_world_bank_data(year):
    """Fetch data from World Bank API directly."""

    # treats year as string
    # Convert year to integer if it's a string
    year = int(year) if isinstance(year, str) else year
    
    # Define indicators
    indicators = {
        'SP.POP.TOTL': 'Population, total',
        'NY.GDP.PCAP.CD': 'GDP per capita (current US$)',
        'EN.ATM.CO2E.PC': 'CO2 emissions (metric tons per capita)',
        'SP.DYN.LE00.IN': 'Life expectancy at birth, total (years)',
        'SE.SEC.ENRR': 'School enrollment, secondary (% gross)',
        'SI.POV.GINI': 'Gini index (World Bank estimate)',
        'SL.UEM.TOTL.ZS': 'Unemployment, total (% of total labor force)'
    }
    
    # Get list of countries
    print("Fetching list of countries from World Bank API")
    response = requests.get("https://api.worldbank.org/v2/country?format=json&per_page=300")
    response.raise_for_status()
    countries_data = response.json()
    countries = []
    if len(countries_data) > 1 and isinstance(countries_data[1], list):
        countries = countries_data[1]
    
    # Prepare data container
    all_data = []
    
    # For each indicator, get data for all countries
    for indicator_code, indicator_name in indicators.items():
        print(f"Fetching {indicator_name} for {year}...")
        
        url = f"https://api.worldbank.org/v2/countries/all/indicators/{indicator_code}"
        params = {
            "date": year,
            "format": "json",
            "per_page": 300
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        result = response.json()
        if len(result) > 1 and isinstance(result[1], list):
            data = result[1]
            
            # Process data
            for item in data:
                if not isinstance(item, dict):
                    continue
                
                country_code = item.get('countryiso3code')
                value = item.get('value')
                
                if not country_code or value is None or country_code == '':
                    continue
                
                # Find or create entry for this country
                country_entry = next((c for c in all_data if c.get('country') == country_code), None)
                if country_entry is None:
                    country_entry = {'country': country_code, 'year': year}
                    all_data.append(country_entry)
                
                # Add this indicator
                country_entry[indicator_code] = value
        
        # Add a short delay to avoid API rate limits
        time.sleep(0.5)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    return df

def fetch_climate_trace_data(year):
    """Fetch data from Climate Trace API directly."""
    
    # treats year as string
    # Convert year to integer if it's a string
    year = int(year) if isinstance(year, str) else year


    # Get list of countries
    print("Fetching list of countries from Climate Trace API")
    countries_response = requests.get("https://api.climatetrace.org/v6/definitions/countries/")
    countries_response.raise_for_status()
    countries = countries_response.json()
    country_codes = [country['alpha3'] for country in countries]
    
    # Process countries in batches
    batch_size = 10
    all_data = []
    
    for i in range(0, len(country_codes), batch_size):
        batch_countries = country_codes[i:i+batch_size]
        print(f"Processing batch of {len(batch_countries)} countries...")
        
        countries_str = ",".join(batch_countries)
        url = f"https://api.climatetrace.org/v6/country/emissions?since={year}&to={year+1}&countries={countries_str}"
        
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            continue
        
        data = response.json()
        
        for item in data:
            if not isinstance(item, dict):
                continue
            
            country_code = item.get('country')
            emissions = item.get('emissions', {})
            
            if not country_code or not emissions:
                continue
            
            # Create a row for this country
            result = {'country': country_code, 'year': year}
            
            # Add emissions values
            for emission_type, value in emissions.items():
                result[emission_type] = value
            
            all_data.append(result)
        
        # Add a short delay to avoid API rate limits
        time.sleep(0.5)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    return df

def run_world_bank_pipeline(year, destination_path):
    """Run the World Bank pipeline and save to local files."""
    os.makedirs(destination_path, exist_ok=True)
    
    # Fetch data directly
    df = fetch_world_bank_data(year)
    
    # Save as CSV
    csv_path = f"{destination_path}/world_bank_indicators_{year}.csv"
    df.to_csv(csv_path, index=False)
    
    print(f"Saved World Bank indicators to: {csv_path}")
    return csv_path

def run_climate_trace_pipeline(year, destination_path):
    """Run the Climate Trace pipeline and save to local files."""
    os.makedirs(destination_path, exist_ok=True)
    
    # Fetch data directly
    df = fetch_climate_trace_data(year)
    
    # Save as CSV
    csv_path = f"{destination_path}/global_emissions_{year}.csv"
    df.to_csv(csv_path, index=False)
    
    print(f"Saved Climate Trace emissions to: {csv_path}")
    return csv_path

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python dlt_extractor.py [world_bank|climate_trace] YEAR OUTPUT_DIR")
        sys.exit(1)
    
    source_type = sys.argv[1]
    year = int(sys.argv[2])
    output_dir = sys.argv[3] if len(sys.argv) > 3 else "data"
    
    if source_type == "world_bank":
        run_world_bank_pipeline(year, output_dir)
    elif source_type == "climate_trace":
        run_climate_trace_pipeline(year, output_dir)
    else:
        print(f"Unknown source type: {source_type}")
        sys.exit(1)