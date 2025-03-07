import os
import sys
import requests
import pandas as pd
import argparse
from typing import List, Dict, Union, Any

class ClimateTraceExtractor:
    BASE_URL = "https://api.climatetrace.org/v6"
    OUTPUT_DIR = "climate_trace_emissions_data"

    def __init__(self, since_year: int, to_year: int = None):
        # Ensure output directory exists
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)

        # If to_year is not provided, use since_year
        self.since_year = since_year
        self.to_year = to_year or since_year

    def get_countries(self) -> List[Dict]:
        """Retrieve list of countries from Climate Trace API."""
        try:
            response = requests.get(f"{self.BASE_URL}/definitions/countries/")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching countries: {e}")
            return []

    def fetch_emissions_data(self, country_codes: List[str], year: int) -> List[Dict]:
        """Fetch emissions data for specific countries and year."""
        # Build the base URL
        base_url = f"{self.BASE_URL}/country/emissions"

        # Create URL with properly formatted country list (not using params to avoid URL encoding of commas)
        countries_str = ",".join(country_codes)
        url = f"{base_url}?since={year}&to={year+1}&countries={countries_str}"

        try:
            # Make the request with the manually formatted URL
            print(f"Full request URL: {url}")
            response = requests.get(url)
            print(f"Response status: {response.status_code}")
            print(f"Response content: {response.text[:500]}...")

            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            print(f"Error fetching emissions data: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Error response: {e.response.text}")
            return []

    def process_emissions_data(self, data: List[Dict]) -> pd.DataFrame:
        """
        Process emissions data into a DataFrame with countries as rows 
        and emission types as columns.
        """
        countries_data = []
        
        # Extract data for each country
        for item in data:
            if not isinstance(item, dict):
                continue
                
            country_code = item.get('country')
            emissions = item.get('emissions', {})
            
            if not country_code or not emissions:
                continue
                
            # Create a row for this country
            country_row = {'country': country_code}
            
            # Add emissions values as columns
            for emission_type, value in emissions.items():
                country_row[emission_type] = value
                
            countries_data.append(country_row)
        
        # Convert to DataFrame
        df = pd.DataFrame(countries_data)
        
        return df

    def save_year_data(self, year: int, data: pd.DataFrame):
        """
        Save emissions data for a specific year to CSV.
        
        :param year: Year of emissions data
        :param data: DataFrame with countries as rows and emission types as columns
        """
        # Create output directory
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)

        # Filename for the year
        filename = os.path.join(self.OUTPUT_DIR, f"global_emissions_{year}.csv")

        # Save to CSV
        if not data.empty:
            data.to_csv(filename, index=False)
            print(f"Saved emissions data for {year} to {filename}")
        else:
            print(f"No data available for {year}")

    def extract_emissions_by_year(self):
        """Extract emissions data for each year."""
        # Get all countries
        countries = self.get_countries()
        country_codes = [country['alpha3'] for country in countries]
        
        print(f"Found {len(country_codes)} countries")
        
        # Process each year
        for year in range(self.since_year, self.to_year + 1):
            print(f"Processing emissions data for year {year}...")
            
            # Due to API limitations, we may need to process countries in batches
            batch_size = 20  # Adjust based on API limitations
            all_data = []
            
            for i in range(0, len(country_codes), batch_size):
                batch_countries = country_codes[i:i+batch_size]
                print(f"Processing batch of {len(batch_countries)} countries...")
                
                data = self.fetch_emissions_data(batch_countries, year)
                if data:
                    all_data.extend(data)
            
            # Process and save data for this year
            df = self.process_emissions_data(all_data)
            self.save_year_data(year, df)

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Climate Trace Emissions Data Extractor")
    parser.add_argument("since_year", type=int, help="Starting year for emissions data")
    parser.add_argument("--to_year", type=int, help="Ending year for emissions data (optional)")
    
    # Parse arguments
    args = parser.parse_args()

    # Create extractor and run pipeline
    extractor = ClimateTraceExtractor(
        since_year=args.since_year, 
        to_year=args.to_year
    )
    extractor.extract_emissions_by_year()

if __name__ == "__main__":
    main()