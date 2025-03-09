# Add these functions to a utils.py file in your project
# debugging techniques for data pipelines

import os
import pandas as pd
from google.cloud import storage
import tempfile

def verify_gcs_file_exists(gcs_path):
    """Check if a file exists in GCS and print details about it."""
    if not gcs_path.startswith('gs://'):
        print(f"Not a GCS path: {gcs_path}")
        return False
        
    bucket_name = gcs_path.replace('gs://', '').split('/')[0]
    blob_name = '/'.join(gcs_path.replace(f'gs://{bucket_name}/', '').split('/'))
    
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        exists = blob.exists()
        
        print(f"Checking file: {gcs_path}")
        print(f"  - Exists: {exists}")
        
        if exists:
            print(f"  - Size: {blob.size} bytes")
            print(f"  - Updated: {blob.updated}")
            print(f"  - Content type: {blob.content_type}")
        
        return exists
    except Exception as e:
        print(f"Error checking GCS file: {e}")
        return False

def preview_parquet_file(gcs_path, num_rows=5):
    """Download and preview a Parquet file from GCS."""
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download the Parquet file
            bucket_name = gcs_path.replace('gs://', '').split('/')[0]
            blob_name = '/'.join(gcs_path.replace(f'gs://{bucket_name}/', '').split('/'))
            local_path = os.path.join(temp_dir, "data.parquet")
            
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.download_to_filename(local_path)
            
            # Read and display Parquet file info
            df = pd.read_parquet(local_path)
            print(f"Parquet file shape: {df.shape}")
            print(f"Columns: {df.columns.tolist()}")
            print(f"\nFirst {num_rows} rows:")
            print(df.head(num_rows))
            
            # Check for problematic column names
            problematic_cols = [col for col in df.columns if '.' in col]
            if problematic_cols:
                print(f"\nWARNING: Found {len(problematic_cols)} columns with dots:")
                print(problematic_cols)
            
            return df
    except Exception as e:
        print(f"Error previewing Parquet file: {e}")
        return None

# Example usage in your DAG or Python code:
# verify_gcs_file_exists("gs://zoomcamp-climate-trace/processed/combined/combined_data.parquet")
# preview_parquet_file("gs://zoomcamp-climate-trace/processed/combined/combined_data.parquet")