#!/usr/bin/env python3
"""
Setup script for the data pipeline
Initializes the S3 data lake structure and configuration
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_pipeline.utils.config_loader import ConfigLoader
from data_pipeline.utils.s3_utils import S3Manager


def setup_data_lake():
    """Setup the S3 data lake structure"""
    print("Setting up data lake...")
    
    # Load configuration
    config = ConfigLoader()
    aws_config = config.get_aws_config()
    
    # Initialize S3 Manager
    s3_manager = S3Manager(
        region_name=aws_config['region'],
        bucket_name=aws_config['bucket_name']
    )
    
    # Create bucket if it doesn't exist
    s3_manager.create_bucket()
    
    # Setup data lake folder structure
    prefixes = [
        'raw/',
        'raw/database/',
        'raw/api/',
        'raw/files/',
        'processed/',
        'processed/masked/',
        'curated/',
        'curated/dim_customer/',
        'curated/dim_product/',
        'curated/dim_date/',
        'curated/dim_location/',
        'curated/fact_sales/',
    ]
    
    s3_manager.setup_data_lake_structure(prefixes)
    
    print("Data lake setup completed successfully!")
    print(f"Bucket: {aws_config['bucket_name']}")
    print(f"Region: {aws_config['region']}")


def verify_configuration():
    """Verify that all required configuration is present"""
    print("\nVerifying configuration...")
    
    config = ConfigLoader()
    
    # Check AWS config
    aws_config = config.get_aws_config()
    required_aws_keys = ['region', 'bucket_name']
    
    for key in required_aws_keys:
        if not aws_config.get(key):
            print(f"Warning: AWS configuration missing: {key}")
    
    # Check Spark config
    spark_config = config.get_spark_config()
    if not spark_config:
        print("Warning: Spark configuration is missing")
    
    # Check data sources
    data_sources = config.get_data_sources()
    if not data_sources:
        print("Warning: No data sources configured")
    else:
        print(f"Found {len(data_sources)} data sources configured")
    
    print("Configuration verification completed!")


if __name__ == '__main__':
    print("Data Pipeline Setup")
    print("=" * 50)
    
    verify_configuration()
    
    response = input("\nDo you want to setup the data lake on S3? (y/n): ")
    if response.lower() == 'y':
        setup_data_lake()
    else:
        print("Setup cancelled")
