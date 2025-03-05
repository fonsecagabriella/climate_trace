import os
import sys
import requests
import pandas as pd
import argparse
from typing import List, Dict, Union, Any
import csv

class ClimateTraceExtractor:
    BASE_URL = "https://api.climatetrace.org/v6"
    OUTPUT_DIR = "climate_trace_emissions_data"

    def __init__(self, since_year: int, to_year: int = None):
        # Ensure output directory exists
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)

        # If to_year is not provided, increment since_year by 1
        self.since_year = since_year
        self.to_year = to_year or (since_year + 1)

    def get_countries(self) -> List[Dict]:
        """Retrieve list of countries from Climate Trace API."""
        try:
            response = requests.get(f"{self.BASE_URL}/definitions/countries/")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching countries: {e}")
            return []

    def get_sectors(self) -> List[str]:
        """Retrieve list of sectors from Climate Trace API."""
        try:
            response = requests.get(f"{self.BASE_URL}/definitions/sectors")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching sectors: {e}")
            return []

    def fetch_emissions_data(self, country: Union[str, Dict], sector: str) -> List[Dict]:
        """Fetch emissions data for a specific country and sector."""
        # Extract country code handling both string and dictionary inputs
        country_code = country['alpha3'] if isinstance(country, dict) else country

        params = {
            "since": self.since_year,
            "to": self.to_year,
            "country": country_code,
            "sector": sector
        }
        
        try:
            response = requests.get(f"{self.BASE_URL}/country/emissions", params=params)
            print("RESPONSE")
            print(response.status_code, response.text)  # Debugging output
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching data for {country_code} in {sector}: {e}")
            return []

    def process_data_items(self, data: List[Any], sector: str) -> Dict:
        """
        Process data items to extract emissions data for a sector.
        
        :param data: List of data items from API
        :param sector: Sector name
        :return: Dictionary with sector as key and emissions data as values
        """
        # Handle both single dictionary and list inputs
        if isinstance(data, dict):
            data = [data]
        
        # Create a dictionary for this sector
        sector_data = {'sector': sector}
        
        for item in data:
            if isinstance(item, dict) and 'emissions' in item:
                # Add emissions values directly as columns
                for emission_type, value in item['emissions'].items():
                    sector_data[emission_type] = value
                
                # If we found valid data, return it
                if len(sector_data) > 1:  # More than just the 'sector' key
                    return sector_data
        
        # Return the sector with nulls if no emissions data was found
        return sector_data

    def save_country_data(self, country_code: str, country_sectors_data: List[Dict]):
        """
        Save data to CSV for a specific country with sectors as rows.
        
        :param country_code: Country's alpha3 code
        :param country_sectors_data: List of data items for the country, one per sector
        """
        # Create year-specific directory
        year_dir = os.path.join(
            self.OUTPUT_DIR, 
            str(self.since_year)
        )
        os.makedirs(year_dir, exist_ok=True)

        # Filename for the country
        filename = os.path.join(
            year_dir, 
            f"{country_code}_emissions_{self.since_year}_to_{self.to_year}.csv"
        )

        # Check if we have data
        if not country_sectors_data:
            print(f"No data found for {country_code}")
            return

        # Create DataFrame with sectors as rows
        df = pd.DataFrame(country_sectors_data)
        
        # Print column names for verification
        print(f"Columns for {country_code}: {list(df.columns)}")
        
        # Save to CSV
        df.to_csv(filename, index=False)
        
        print(f"Saved emissions data for {country_code} to {filename}")

    def extract_emissions(self):
        """Main extraction pipeline."""
        # Get countries and sectors
        countries = self.get_countries()
        sectors = self.get_sectors()

        print(f"Extracting emissions data from {self.since_year} to {self.to_year}")
        print(f"Countries to process: {len(countries)}")
        print(f"Sectors to process: {len(sectors)}")

        # Process each country
        for country in countries:
            country_code = country['alpha3']
            country_sectors_data = []

            for sector in sectors:
                print(f"Processing {country.get('name', country_code)} - {sector}...")
                data = self.fetch_emissions_data(country, sector)
                
                # Process and add sector data to country's consolidated data
                if data:
                    processed_data = self.process_data_items(data, sector)
                    country_sectors_data.append(processed_data)
                else:
                    # Add empty sector data to maintain consistent rows
                    country_sectors_data.append({'sector': sector})

            # Save country data immediately after processing all sectors
            if country_sectors_data:
                self.save_country_data(country_code, country_sectors_data)

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
    extractor.extract_emissions()

if __name__ == "__main__":
    main()