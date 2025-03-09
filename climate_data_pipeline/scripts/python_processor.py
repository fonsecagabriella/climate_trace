import pandas as pd
import os
import numpy as np
from google.cloud import storage
import tempfile
import io

def download_from_gcs(gcs_path, local_path=None):
    """
    Download a file from GCS to local storage
    
    Args:
        gcs_path: Path in GCS (gs://bucket/path/to/file)
        local_path: Optional local path to save the file
    
    Returns:
        Path to downloaded file
    """
    # Parse the GCS path
    if not gcs_path.startswith('gs://'):
        return gcs_path  # Already a local path
        
    bucket_name = gcs_path.replace('gs://', '').split('/')[0]
    blob_name = '/'.join(gcs_path.replace(f'gs://{bucket_name}/', '').split('/'))
    
    # If no local path provided, create a temp file
    if not local_path:
        # Create a temp directory with the same structure
        temp_dir = tempfile.mkdtemp()
        local_path = os.path.join(temp_dir, os.path.basename(blob_name))
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    # Download the file
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(local_path)
        print(f"Downloaded {gcs_path} to {local_path}")
        return local_path
    except Exception as e:
        print(f"Error downloading from GCS: {e}")
        # If download fails, return local path which will be created as sample data
        return local_path

def upload_to_gcs(local_path, gcs_path):
    """
    Upload a file to GCS
    
    Args:
        local_path: Path to local file
        gcs_path: Destination in GCS (gs://bucket/path/to/file)
    
    Returns:
        GCS path if successful, None otherwise
    """
    if not gcs_path.startswith('gs://'):
        return gcs_path  # Not a GCS path
        
    bucket_name = gcs_path.replace('gs://', '').split('/')[0]
    blob_name = '/'.join(gcs_path.replace(f'gs://{bucket_name}/', '').split('/'))
    
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        print(f"Uploaded {local_path} to {gcs_path}")
        return gcs_path
    except Exception as e:
        print(f"Error uploading to GCS: {e}")
        return None

def process_world_bank_data(input_path, output_path):
    """Process World Bank data using Pandas and store in GCS"""
    import pandas as pd
    import os
    from google.cloud import storage
    import tempfile
    
    print(f"Processing World Bank data from: {input_path}")
    
    # Extract year from input path
    year = os.path.basename(input_path).split('.')[0].split('_')[-1]
    print(f"Processing data for year: {year}")
    
    # Create a temp directory for processing
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download the input file from GCS
        client = storage.Client()
        bucket_name = input_path.replace("gs://", "").split("/")[0]
        blob_path = "/".join(input_path.replace(f"gs://{bucket_name}/", "").split("/"))
        
        local_input_path = os.path.join(temp_dir, os.path.basename(input_path))
        
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.download_to_filename(local_input_path)
        print(f"Downloaded {input_path} to {local_input_path}")
        
        # Read the CSV data
        df = pd.read_csv(local_input_path)
        
        # Clean column names - lowercase, replace spaces with underscores, strip whitespace
        df.columns = [col.lower().replace(' ', '_').strip() for col in df.columns]
        
        # Print column names for debugging
        print(f"Columns after cleaning: {df.columns.tolist()}")
        
        # Convert numeric columns
        for col in df.columns:
            if col != 'country':
                df[col] = pd.to_numeric(df[col], errors='coerce')
    
        # Add calculated columns
        if 'sp_pop_totl' in df.columns and 'en_atm_co2e_pc' in df.columns:
            df['total_emissions'] = df['sp_pop_totl'] * df['en_atm_co2e_pc']
            
        # Add year column if not present
        if 'year' not in df.columns:
            df['year'] = int(year)
        
        # Write to a temporary parquet file
        local_output_file = os.path.join(temp_dir, "data.parquet")
        df.to_parquet(local_output_file)
        
        # Check if year-specific output directory already exists
        year_output_path = f"{output_path}/{year}"
        output_file = f"{year_output_path}/data.parquet"
        
        # Upload to GCS
        output_bucket_name = year_output_path.replace("gs://", "").split("/")[0]
        output_blob_path = "/".join(year_output_path.replace(f"gs://{output_bucket_name}/", "").split("/"))
        
        output_bucket = client.bucket(output_bucket_name)
        
        # Delete existing directory if it exists
        for blob in output_bucket.list_blobs(prefix=f"{output_blob_path}/"):
            blob.delete()
            print(f"Deleted existing blob: {blob.name}")
        
        # Upload new file
        output_blob = output_bucket.blob(f"{output_blob_path}/data.parquet")
        output_blob.upload_from_filename(local_output_file)
        print(f"Uploaded processed data to {output_file}")
        
    return year_output_path

def process_climate_trace_data(input_path, output_path):
    """Process Climate Trace data using Pandas and store in GCS"""
    import pandas as pd
    import os
    from google.cloud import storage
    import tempfile
    
    print(f"Processing Climate Trace data from: {input_path}")
    
    # Extract year from input path
    year = os.path.basename(input_path).split('.')[0].split('_')[-1]
    print(f"Processing data for year: {year}")
    
    # Create a temp directory for processing
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download the input file from GCS
        client = storage.Client()
        bucket_name = input_path.replace("gs://", "").split("/")[0]
        blob_path = "/".join(input_path.replace(f"gs://{bucket_name}/", "").split("/"))
        
        local_input_path = os.path.join(temp_dir, os.path.basename(input_path))
        
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.download_to_filename(local_input_path)
        print(f"Downloaded {input_path} to {local_input_path}")
        
        # Read the CSV data
        df = pd.read_csv(local_input_path)
        
        # Clean column names - lowercase, replace spaces with underscores, strip whitespace
        df.columns = [col.lower().replace(' ', '_').strip() for col in df.columns]
        
        # Print the column names to debug
        print(f"Columns in the data after cleaning: {df.columns.tolist()}")
        
        # Convert all numeric columns
        for col in df.columns:
            if col not in ['country', 'year']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Ensure year is integer
        if 'year' in df.columns:
            df['year'] = df['year'].astype(int)
        else:
            df['year'] = int(year)
        
        # Create aggregations by country
        result_df = df.groupby(['country', 'year']).agg({
            'co2': 'sum',
            'ch4': 'sum',
            'n2o': 'sum',
            'co2e_100yr': 'sum',
            'co2e_20yr': 'sum'
        }).reset_index()
        
        # Rename co2e_100yr to total for consistency
        result_df = result_df.rename(columns={'co2e_100yr': 'total'})
        
        # Calculate energy percentage (approximating energy as co2)
        if 'co2' in result_df.columns and 'total' in result_df.columns:
            result_df['energy_percent'] = (result_df['co2'] / result_df['total']) * 100
        
        # Clean column names again just to be sure
        result_df.columns = [col.lower().replace(' ', '_').strip() for col in result_df.columns]
        
        # Write to a temporary parquet file
        local_output_file = os.path.join(temp_dir, "data.parquet")
        result_df.to_parquet(local_output_file)
        
        # Check if year-specific output directory already exists
        year_output_path = f"{output_path}/{year}"
        output_file = f"{year_output_path}/data.parquet"
        
        # Upload to GCS
        output_bucket_name = year_output_path.replace("gs://", "").split("/")[0]
        output_blob_path = "/".join(year_output_path.replace(f"gs://{output_bucket_name}/", "").split("/"))
        
        output_bucket = client.bucket(output_bucket_name)
        
        # Delete existing directory if it exists
        for blob in output_bucket.list_blobs(prefix=f"{output_blob_path}/"):
            blob.delete()
            print(f"Deleted existing blob: {blob.name}")
        
        # Upload new file
        output_blob = output_bucket.blob(f"{output_blob_path}/data.parquet")
        output_blob.upload_from_filename(local_output_file)
        print(f"Uploaded processed data to {output_file}")
        
    return year_output_path

def combine_datasets(world_bank_path, climate_trace_path, output_path):
    """Combine World Bank and Climate Trace data for all years"""
    import pandas as pd
    import os
    from google.cloud import storage
    import tempfile
    
    print(f"Combining datasets from World Bank: {world_bank_path} and Climate Trace: {climate_trace_path}")
    
    # Create a temp directory for processing
    with tempfile.TemporaryDirectory() as temp_dir:
        client = storage.Client()
        
        # Get all available years from both datasets
        wb_bucket_name = world_bank_path.replace("gs://", "").split("/")[0]
        wb_prefix = "/".join(world_bank_path.replace(f"gs://{wb_bucket_name}/", "").split("/"))
        wb_bucket = client.bucket(wb_bucket_name)
        
        ct_bucket_name = climate_trace_path.replace("gs://", "").split("/")[0]
        ct_prefix = "/".join(climate_trace_path.replace(f"gs://{ct_bucket_name}/", "").split("/"))
        ct_bucket = client.bucket(ct_bucket_name)
        
        # Find all year directories
        wb_years = set()
        for blob in wb_bucket.list_blobs(prefix=wb_prefix):
            # Extract year from path
            parts = blob.name.replace(wb_prefix, "").strip("/").split("/")
            if len(parts) >= 1 and parts[0].isdigit():
                wb_years.add(parts[0])
        
        ct_years = set()
        for blob in ct_bucket.list_blobs(prefix=ct_prefix):
            # Extract year from path
            parts = blob.name.replace(ct_prefix, "").strip("/").split("/")
            if len(parts) >= 1 and parts[0].isdigit():
                ct_years.add(parts[0])
        
        # Get intersection of years available in both datasets
        all_years = wb_years.union(ct_years)
        print(f"Processing years: {all_years}")
        
        # Initialize dataframes to hold all data
        all_wb_data = []
        all_ct_data = []
        
        # Process each year
        for year in all_years:
            # Process World Bank data if available
            wb_year_path = f"{world_bank_path}/{year}/data.parquet"
            wb_exists = any(blob.name == f"{wb_prefix}/{year}/data.parquet" for blob in wb_bucket.list_blobs(prefix=f"{wb_prefix}/{year}"))
            
            if wb_exists:
                print(f"Processing World Bank data for year {year}")
                wb_local_path = os.path.join(temp_dir, f"wb_{year}.parquet")
                wb_blob = wb_bucket.blob(f"{wb_prefix}/{year}/data.parquet")
                wb_blob.download_to_filename(wb_local_path)
                
                try:
                    wb_df = pd.read_parquet(wb_local_path)
                    # Ensure year column exists
                    if 'year' not in wb_df.columns:
                        wb_df['year'] = int(year)
                    all_wb_data.append(wb_df)
                except Exception as e:
                    print(f"Error reading World Bank data for year {year}: {e}")
            
            # Process Climate Trace data if available
            ct_year_path = f"{climate_trace_path}/{year}/data.parquet"
            ct_exists = any(blob.name == f"{ct_prefix}/{year}/data.parquet" for blob in ct_bucket.list_blobs(prefix=f"{ct_prefix}/{year}"))
            
            if ct_exists:
                print(f"Processing Climate Trace data for year {year}")
                ct_local_path = os.path.join(temp_dir, f"ct_{year}.parquet")
                ct_blob = ct_bucket.blob(f"{ct_prefix}/{year}/data.parquet")
                ct_blob.download_to_filename(ct_local_path)
                
                try:
                    ct_df = pd.read_parquet(ct_local_path)
                    # Ensure year column exists
                    if 'year' not in ct_df.columns:
                        ct_df['year'] = int(year)
                    all_ct_data.append(ct_df)
                except Exception as e:
                    print(f"Error reading Climate Trace data for year {year}: {e}")
        
        # Combine all years' data
        if all_wb_data:
            combined_wb_df = pd.concat(all_wb_data, ignore_index=True)
            print(f"Combined World Bank data has {len(combined_wb_df)} rows")
        else:
            combined_wb_df = pd.DataFrame(columns=['country', 'year'])
            print("No World Bank data available")
        
        if all_ct_data:
            combined_ct_df = pd.concat(all_ct_data, ignore_index=True)
            print(f"Combined Climate Trace data has {len(combined_ct_df)} rows")
        else:
            combined_ct_df = pd.DataFrame(columns=['country', 'year'])
            print("No Climate Trace data available")
        
        # Merge datasets on country and year
        if not combined_wb_df.empty and not combined_ct_df.empty:
            # Clean column names to ensure they're lowercase
            combined_wb_df.columns = [col.lower().replace(' ', '_').strip() for col in combined_wb_df.columns]
            combined_ct_df.columns = [col.lower().replace(' ', '_').strip() for col in combined_ct_df.columns]
            
            print(f"World Bank columns: {combined_wb_df.columns.tolist()}")
            print(f"Climate Trace columns: {combined_ct_df.columns.tolist()}")
            
            # Merge on country and year
            combined_df = pd.merge(combined_wb_df, combined_ct_df, on=['country', 'year'], how='outer')
            
            # Handle any column name collisions
            # If we have total_emissions from both datasets, rename one
            for col in combined_df.columns:
                if col.endswith('_x') or col.endswith('_y'):
                    base_col = col[:-2]
                    if f"{base_col}_x" in combined_df.columns and f"{base_col}_y" in combined_df.columns:
                        combined_df = combined_df.rename(columns={
                            f"{base_col}_x": f"{base_col}_wb",
                            f"{base_col}_y": f"{base_col}_ct"
                        })
            
            print(f"Combined data has {len(combined_df)} rows and columns: {combined_df.columns.tolist()}")
        elif not combined_wb_df.empty:
            combined_df = combined_wb_df
            print(f"Only World Bank data available, {len(combined_df)} rows")
        elif not combined_ct_df.empty:
            combined_df = combined_ct_df
            print(f"Only Climate Trace data available, {len(combined_df)} rows")
        else:
            # Create an empty dataframe with expected columns
            combined_df = pd.DataFrame(columns=[
                'country', 'year', 'sp_pop_totl', 'ny_gdp_pcap_cd', 'en_atm_co2e_pc', 
                'total_emissions', 'co2', 'ch4', 'n2o', 'total', 'co2e_20yr', 'energy_percent'
            ])
            print("No data available for either dataset")
        
        # Clean column names one last time
        combined_df.columns = [col.lower().replace(' ', '_').strip() for col in combined_df.columns]
        
        # Write combined data to temporary file
        local_combined_file = os.path.join(temp_dir, "combined_data.parquet")
        combined_df.to_parquet(local_combined_file)
        
        # Upload to GCS
        output_bucket_name = output_path.replace("gs://", "").split("/")[0]
        output_blob_path = "/".join(output_path.replace(f"gs://{output_bucket_name}/", "").split("/"))
        
        output_bucket = client.bucket(output_bucket_name)
        
        # Delete existing files if they exist
        for blob in output_bucket.list_blobs(prefix=output_blob_path):
            blob.delete()
            print(f"Deleted existing blob: {blob.name}")
        
        # Upload new file
        output_blob = output_bucket.blob(f"{output_blob_path}/combined_data.parquet")
        output_blob.upload_from_filename(local_combined_file)
        print(f"Uploaded combined data to {output_path}/combined_data.parquet")
        
    return output_path