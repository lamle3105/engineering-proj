#!/usr/bin/env python3
"""
Simple script to run the complete pipeline locally for testing
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from data_pipeline.utils.config_loader import ConfigLoader
from data_pipeline.ingestion.file_source import FileSource
from data_pipeline.transformation.spark_transformer import DataStandardizer
from data_pipeline.masking.data_masker import DataMasker


def run_pipeline():
    """Run the complete data pipeline locally"""
    print("=" * 60)
    print("Starting Data Pipeline")
    print("=" * 60)
    
    # Load configuration
    config = ConfigLoader()
    
    # Step 1: Ingest data
    print("\n[1/4] Ingesting data from CSV files...")
    file_config = {
        'name': 'sales_transactions',
        'location': 'data/raw/sales_transactions.csv',
        'format': 'csv'
    }
    source = FileSource(file_config)
    data = source.extract()
    print(f"✓ Ingested {len(data)} records")
    
    # Step 2: Transform with Spark
    print("\n[2/4] Transforming data with Spark...")
    spark_config = config.get_spark_config()
    standardizer = DataStandardizer(spark_config)
    
    spark_df = standardizer.spark.createDataFrame(data)
    standardized_df = standardizer.standardize(spark_df)
    print(f"✓ Standardized {standardized_df.count()} records")
    
    # Step 3: Apply data masking
    print("\n[3/4] Applying data masking...")
    masking_config = config.get_masking_config()
    masker = DataMasker(masking_config)
    
    masked_df = masker.apply_masking_policy(standardized_df)
    print(f"✓ Masked sensitive data")
    
    # Step 4: Save results
    print("\n[4/4] Saving results...")
    output_path = f"data/processed/pipeline_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    masked_df.write.mode('overwrite').parquet(output_path)
    print(f"✓ Saved to {output_path}")
    
    standardizer.stop()
    
    print("\n" + "=" * 60)
    print("Pipeline completed successfully!")
    print("=" * 60)


if __name__ == '__main__':
    try:
        run_pipeline()
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {str(e)}")
        sys.exit(1)
