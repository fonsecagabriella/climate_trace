import dlt
import requests
import pandas as pd
import os

# API URL and parameters
API_URL = "https://api.climatetrace.org/v6/assets"
YEAR = 2020
OUTPUT_DIR = f"climate_trace_data/{YEAR}"


# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Function to fetch country list from Climate Trace API
def get_country_list():
    response = requests.get(f"{API_URL}/countries")
    print(response.status_code, response.text)  # Debugging output
    if response.status_code == 200:
    # Extract ISO3 country codes from the dictionary
        return list(response.json().keys())
    else:
        raise Exception(f"Error fetching country list: {response.status_code} - {response.text}")


# Function to fetch paginated emissions data
def fetch_emissions_data(country, year=YEAR):
    all_data = []
    page = 1
    while True:
        params = {
            "year": year,
            "country": country,
            "page": page,
            "limit": 1000
        }
        response = requests.get(API_URL, params=params)
        if response.status_code != 200:
            print(f"Error fetching data for {country}: {response.text}")
            break
        data = response.json()
        if not data:
            break
        all_data.extend(data)
        page += 1
    return all_data

# Function to save emissions data locally
def save_to_csv(data, country):
    if data:
        df = pd.DataFrame(data)
        file_path = os.path.join(OUTPUT_DIR, f"{country}_emissions.csv")
        df.to_csv(file_path, index=False)
        print(f"Saved data for {country} to {file_path}")
    else:
        print(f"No data found for {country}")

# Main pipeline to extract data
def climate_trace_pipeline():
    # Initialize dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="climate_trace_extractor",
        destination="filesystem"
    )

    # Get country list and fetch data for each country
    countries = get_country_list()
    for country in countries:
        print(f"Fetching data for {country}...")
        data = fetch_emissions_data(country)
        save_to_csv(data, country)

    print("✅ Data extraction complete. Files saved in:", OUTPUT_DIR)

if __name__ == "__main__":
    climate_trace_pipeline()
