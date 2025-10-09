"""
Data Pipeline Airflow DAG
Orchestrates the entire data pipeline workflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from data_pipeline.utils.config_loader import ConfigLoader
from data_pipeline.utils.s3_utils import S3Manager
from data_pipeline.ingestion.base import DataIngestionPipeline
from data_pipeline.ingestion.database_source import DatabaseSource
from data_pipeline.ingestion.api_source import APISource
from data_pipeline.ingestion.file_source import FileSource
from data_pipeline.transformation.spark_transformer import DataStandardizer
from data_pipeline.transformation.star_schema import StarSchemaBuilder
from data_pipeline.masking.data_masker import DataMasker


# Load configuration
config_loader = ConfigLoader()

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def ingest_data_sources(**context):
    """Ingest data from all configured sources"""
    import boto3
    
    aws_config = config_loader.get_aws_config()
    s3_client = boto3.client('s3', 
                            region_name=aws_config['region'])
    
    bucket_name = aws_config['bucket_name']
    pipeline = DataIngestionPipeline(s3_client, bucket_name)
    
    data_sources = config_loader.get_data_sources()
    
    for source_config in data_sources:
        source_type = source_config.get('type')
        
        if source_type == 'postgresql' or source_type == 'mysql':
            source = DatabaseSource(source_config)
        elif source_type == 'api':
            source = APISource(source_config)
        elif source_type == 'file':
            source = FileSource(source_config)
        else:
            print(f"Unknown source type: {source_type}")
            continue
        
        # Ingest data
        success = pipeline.ingest(source, 'raw/')
        
        if success:
            print(f"Successfully ingested data from {source.name}")
        else:
            print(f"Failed to ingest data from {source.name}")


def transform_and_standardize(**context):
    """Transform and standardize raw data"""
    spark_config = config_loader.get_spark_config()
    aws_config = config_loader.get_aws_config()
    
    standardizer = DataStandardizer(spark_config)
    
    # Read raw data from S3
    raw_path = f"s3a://{aws_config['bucket_name']}/raw/"
    
    try:
        # Read all parquet files from raw layer
        df = standardizer.read_from_s3(raw_path + "*/*.parquet")
        
        # Standardize data
        standardized_df = standardizer.standardize(df)
        
        # Write to processed layer
        processed_path = f"s3a://{aws_config['bucket_name']}/processed/"
        standardizer.write_to_s3(standardized_df, processed_path)
        
        print(f"Standardized {standardized_df.count()} records")
        
    finally:
        standardizer.stop()


def apply_data_masking(**context):
    """Apply data masking to sensitive fields"""
    spark_config = config_loader.get_spark_config()
    aws_config = config_loader.get_aws_config()
    masking_config = config_loader.get_masking_config()
    
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName(spark_config.get('app_name')) \
        .getOrCreate()
    
    masker = DataMasker(masking_config)
    
    try:
        # Read processed data
        processed_path = f"s3a://{aws_config['bucket_name']}/processed/"
        df = spark.read.parquet(processed_path)
        
        # Apply masking
        masked_df = masker.apply_masking_policy(df)
        
        # Write back to processed layer
        masked_df.write.mode('overwrite').parquet(processed_path + "masked/")
        
        print(f"Applied masking to {masked_df.count()} records")
        
    finally:
        spark.stop()


def build_star_schema(**context):
    """Build star schema for data warehouse"""
    spark_config = config_loader.get_spark_config()
    aws_config = config_loader.get_aws_config()
    
    builder = StarSchemaBuilder(spark_config)
    
    try:
        # Read processed data
        processed_path = f"s3a://{aws_config['bucket_name']}/processed/masked/"
        df = builder.read_from_s3(processed_path)
        
        # Assuming the dataframe has the necessary columns
        # Split into different entities (this is simplified)
        
        # For demonstration, create sample dataframes
        # In production, you would split/transform the actual data
        sales_df = df.filter(df._table_name == 'sales_transactions')
        customer_df = df.filter(df._table_name == 'customers')
        product_df = df.filter(df._table_name == 'products')
        
        # Build star schema
        star_schema = builder.build_star_schema(
            sales_df=sales_df,
            customer_df=customer_df,
            product_df=product_df
        )
        
        # Write to curated layer
        curated_path = f"s3a://{aws_config['bucket_name']}/curated/"
        
        for table_name, table_df in star_schema.items():
            table_path = f"{curated_path}{table_name}/"
            builder.write_to_s3(table_df, table_path)
            print(f"Created {table_name} with {table_df.count()} records")
        
    finally:
        builder.stop()


def data_quality_checks(**context):
    """Perform data quality checks"""
    print("Running data quality checks...")
    # Implement data quality checks using Great Expectations or similar
    # For now, just log
    print("Data quality checks completed")


# Define the DAG
dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    description='End-to-end data pipeline for data lake and warehouse',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['data_engineering', 'etl', 'data_lake'],
)

# Define tasks
start = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

ingest = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data_sources,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_and_standardize',
    python_callable=transform_and_standardize,
    dag=dag,
)

mask_data = PythonOperator(
    task_id='apply_data_masking',
    python_callable=apply_data_masking,
    dag=dag,
)

build_schema = PythonOperator(
    task_id='build_star_schema',
    python_callable=build_star_schema,
    dag=dag,
)

quality_checks = PythonOperator(
    task_id='data_quality_checks',
    python_callable=data_quality_checks,
    dag=dag,
)

end = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Define task dependencies
start >> ingest >> transform >> mask_data >> build_schema >> quality_checks >> end
