"""
Data Ingestion Base Module
Provides base classes and utilities for data ingestion from various sources
"""

import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataSource(ABC):
    """Abstract base class for data sources"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = config.get('name', 'unknown')
        
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract data from the source"""
        pass
    
    @abstractmethod
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate extracted data"""
        pass
    
    def add_metadata(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add metadata columns to the dataframe"""
        data['_ingestion_timestamp'] = datetime.now()
        data['_source'] = self.name
        return data


class DataIngestionPipeline:
    """Main data ingestion pipeline orchestrator"""
    
    def __init__(self, s3_client, bucket_name: str):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.logger = logger
        
    def ingest(self, source: DataSource, destination_prefix: str) -> bool:
        """
        Ingest data from a source and store in S3
        
        Args:
            source: DataSource instance
            destination_prefix: S3 prefix for storing data
            
        Returns:
            bool: Success status
        """
        try:
            self.logger.info(f"Starting ingestion from {source.name}")
            
            # Extract data
            data = source.extract()
            self.logger.info(f"Extracted {len(data)} records")
            
            # Validate data
            if not source.validate(data):
                self.logger.error(f"Validation failed for {source.name}")
                return False
            
            # Add metadata
            data = source.add_metadata(data)
            
            # Store in S3
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            s3_key = f"{destination_prefix}{source.name}_{timestamp}.parquet"
            
            self.save_to_s3(data, s3_key)
            self.logger.info(f"Data saved to s3://{self.bucket_name}/{s3_key}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Ingestion failed: {str(e)}")
            return False
    
    def save_to_s3(self, data: pd.DataFrame, s3_key: str):
        """Save dataframe to S3 as parquet"""
        import io
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        buffer = io.BytesIO()
        table = pa.Table.from_pandas(data)
        pq.write_table(table, buffer)
        
        buffer.seek(0)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=buffer.getvalue()
        )
