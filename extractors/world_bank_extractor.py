import os
import sys
import requests
import pandas as pd
import argparse
from typing import List, Dict, Union, Any
import time

class WorldBankExtractor:
    BASE_URL = "https://api.worldbank.org/v2"
    OUTPUT_DIR = "world_bank_data"
    
    # List of indicators to extract (you can modify this list)
    # https://data.worldbank.org/indicator/
    
    INDICATORS = {
        'SP.POP.TOTL': 'Population, total',
        'NY.GDP.PCAP.CD': 'GDP per capita (current US$)',
        'EN.ATM.CO2E.PC': 'CO2 emissions (metric tons per capita)',
        'SP.DYN.LE00.IN': 'Life expectancy at birth, total (years)',
        'SE.SEC.ENRR': 'School enrollment, secondary (% gross)',
        'SI.POV.GINI': 'Gini index (World Bank estimate)',
        'SL.UEM.TOTL.ZS': 'Unemployment, total (% of total labor force)'
    }

    def __init__(self, start_year: int, end_year: int = None):
        # Ensure output directory exists
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
        
        self.start_year = start_year
        self.end_year = end_year or start_year
        
    def get_countries(self) -> List[Dict]:
        """Retrieve list of countries from World Bank API."""
        try:
            response = requests.get(f"{self.BASE_URL}/country?format=json&per_page=300")
            response.raise_for_status()
            
            # World Bank returns a list where the first element is metadata and the second is the actual data
            result = response.json()
            if len(result) > 1 and isinstance(result[1], list):
                return result[1]
            return []
        except requests.RequestException as e:
            print(f"Error fetching countries: {e}")
            return []
    
    def fetch_indicator_data(self, indicator: str, year: int) -> List[Dict]:
        """Fetch data for a specific indicator and year for all countries."""
        url = f"{self.BASE_URL}/countries/all/indicators/{indicator}"
        params = {
            "date": year,
            "format": "json",
            "per_page": 300  # Maximum allowed to get all countries in one request
        }
        
        try:
            print(f"Fetching {self.INDICATORS.get(indicator, indicator)} data for {year}...")
            response = requests.get(url, params=params)
            print(f"Response status: {response.status_code}")
            
            response.raise_for_status()
            result = response.json()
            
            # Check if we have data in the response
            if len(result) > 1 and isinstance(result[1], list):
                return result[1]
            return []
        except requests.RequestException as e:
            print(f"Error fetching indicator data: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Error response: {e.response.text[:500]}")
            return []
    
    def process_indicator_data(self, data: List[Dict], indicator: str) -> Dict[str, float]:
        """Process indicator data into a dictionary with country codes as keys."""
        result = {}
        
        for item in data:
            if not isinstance(item, dict):
                continue
            
            country_code = item.get('countryiso3code')
            value = item.get('value')
            
            # Skip entries with missing country code or value
            if not country_code or value is None or country_code == '':
                continue
            
            result[country_code] = value
        
        return result
    
    def extract_indicators_for_year(self, year: int) -> pd.DataFrame:
        """Extract all indicators for a specific year and combine into one DataFrame."""
        # Dictionary to collect all indicator data
        country_data = {}
        
        # For each indicator
        for indicator_code, indicator_name in self.INDICATORS.items():
            # Fetch data
            data = self.fetch_indicator_data(indicator_code, year)
            
            # Process into dictionary: country_code -> value
            indicator_data = self.process_indicator_data(data, indicator_code)
            
            # Add to country_data dictionary
            for country_code, value in indicator_data.items():
                if country_code not in country_data:
                    country_data[country_code] = {'country': country_code}
                
                # Use the short indicator code as column name
                country_data[country_code][indicator_code] = value
            
            # Add a small delay to avoid hitting API rate limits
            time.sleep(0.5)
        
        # Convert to DataFrame
        df = pd.DataFrame(list(country_data.values()))
        
        # If we have data, ensure 'country' is the first column
        if not df.empty and 'country' in df.columns:
            cols = ['country'] + [col for col in df.columns if col != 'country']
            df = df[cols]
        
        return df
    
    def save_year_data(self, year: int, data: pd.DataFrame):
        """Save data for a specific year to CSV."""
        # Create output filename
        filename = os.path.join(self.OUTPUT_DIR, f"world_bank_indicators_{year}.csv")
        
        # Save to CSV
        if not data.empty:
            data.to_csv(filename, index=False)
            print(f"Saved World Bank data for {year} to {filename}")
            
            # Print some statistics
            print(f"Number of countries with data: {len(data)}")
            print(f"Columns included: {', '.join(data.columns)}")
        else:
            print(f"No data available for {year}")
    
    def extract_data(self):
        """Main extraction pipeline."""
        print(f"Extracting World Bank indicators from {self.start_year} to {self.end_year}")
        
        # For each year in the range
        for year in range(self.start_year, self.end_year + 1):
            print(f"\nProcessing year {year}...")
            
            # Extract all indicators for this year
            df = self.extract_indicators_for_year(year)
            
            # Save the data
            self.save_year_data(year, df)
            
            print(f"Completed processing for {year}")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="World Bank Data Extractor")
    parser.add_argument("start_year", type=int, help="Starting year for data extraction")
    parser.add_argument("--end_year", type=int, help="Ending year for data extraction (optional)")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Create extractor and run pipeline
    extractor = WorldBankExtractor(
        start_year=args.start_year,
        end_year=args.end_year
    )
    extractor.extract_data()

if __name__ == "__main__":
    main()