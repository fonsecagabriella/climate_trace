from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import os

def process_world_bank_data(input_path, output_path):
    """
    Process World Bank data using Spark

    Args:
        input_path: GCS path to the World Bank data (e.g., 'gs://bucket-name/world_bank/file.csv')
        output_path: GCS path where to save the processed data
    """
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("World Bank Data Processing") \
        .config("spark.jars.packages", "com.google.cloud:google-cloud-storage:2.2.0") \
        .getOrCreate()

    print(f"Reading data from: {input_path}")

    # Read the CSV data
    df = spark.read.option("header", "true").csv(input_path)

    # Data transformations
    # Convert string columns to appropriate types
    numeric_columns = [
        'SP_POP_TOTL', 'NY_GDP_PCAP_CD', 'EN_ATM_CO2E_PC',
        'SP_DYN_LE00_IN', 'SE_SEC_ENRR', 'SI_POV_GINI', 'SL_UEM_TOTL_ZS'
    ]

    for col in numeric_columns:
        if col in df.columns:
            df = df.withColumn(col, df[col].cast(DoubleType()))

    # Add calculated columns
    if 'SP_POP_TOTL' in df.columns and 'EN_ATM_CO2E_PC' in df.columns:
        df = df.withColumn('total_emissions', 
                          F.col('SP_POP_TOTL') * F.col('EN_ATM_CO2E_PC'))
    
    # Write the processed data
    df.write.format("parquet").mode("overwrite").save(output_path)
    
    print(f"Processed data saved to: {output_path}")
    return output_path

def process_climate_trace_data(input_path, output_path):
    """
    Process Climate Trace data using Spark
    
    Args:
        input_path: GCS path to the Climate Trace data
        output_path: GCS path where to save the processed data
    """
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Climate Trace Data Processing") \
        .config("spark.jars.packages", "com.google.cloud:google-cloud-storage:2.2.0") \
        .getOrCreate()
    
    print(f"Reading data from: {input_path}")
    
    # Read the CSV data
    df = spark.read.option("header", "true").csv(input_path)
    
    # Convert relevant columns to double
    for col in df.columns:
        if col != 'country' and col != 'year':
            df = df.withColumn(col, df[col].cast(DoubleType()))
    
    # Add calculated columns like percentage of energy emissions to total
    if 'total' in df.columns and 'energy' in df.columns:
        df = df.withColumn('energy_percent', 
                          (F.col('energy') / F.col('total')) * 100)
    
    # Group by country and calculate aggregates
    summary_df = df.groupBy('country').agg(
        F.sum('total').alias('total_emissions'),
        F.avg('energy_percent').alias('avg_energy_percent')
    )
    
    # Write the processed data
    df.write.format("parquet").mode("overwrite").save(output_path)
    summary_df.write.format("parquet").mode("overwrite").save(f"{output_path}_summary")
    
    print(f"Processed data saved to: {output_path}")
    return output_path

def combine_datasets(world_bank_path, climate_trace_path, output_path):
    """
    Combine World Bank and Climate Trace data for joint analysis
    
    Args:
        world_bank_path: GCS path to processed World Bank data
        climate_trace_path: GCS path to processed Climate Trace data
        output_path: GCS path where to save the combined dataset
    """
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Combine Climate and Economic Data") \
        .config("spark.jars.packages", "com.google.cloud:google-cloud-storage:2.2.0") \
        .getOrCreate()
    
    # Read the processed data
    wb_df = spark.read.parquet(world_bank_path)
    ct_df = spark.read.parquet(climate_trace_path)
    
    # Join datasets on country code
    combined_df = wb_df.join(
        ct_df,
        wb_df.country == ct_df.country,
        "inner"
    )
    
    # Cleanup duplicate columns
    columns_to_drop = ["country", "year"]
    for col in columns_to_drop:
        if f"{col}_1" in combined_df.columns:
            combined_df = combined_df.drop(f"{col}_1")
    
    # Write the combined data
    combined_df.write.format("parquet").mode("overwrite").save(output_path)
    
    print(f"Combined dataset saved to: {output_path}")
    return output_path