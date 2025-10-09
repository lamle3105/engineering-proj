"""
Spark Transformation Base Module
Provides base classes for PySpark transformations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkTransformer:
    """Base class for Spark transformations"""
    
    def __init__(self, spark_config: Dict[str, Any]):
        self.spark_config = spark_config
        self.spark = self._create_spark_session()
        self.logger = logger
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        builder = SparkSession.builder \
            .appName(self.spark_config.get('app_name', 'DataPipeline'))
        
        # Apply additional configurations
        configs = self.spark_config.get('configs', {})
        for key, value in configs.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    def read_from_s3(self, s3_path: str, format: str = 'parquet') -> DataFrame:
        """Read data from S3"""
        return self.spark.read.format(format).load(s3_path)
    
    def write_to_s3(self, df: DataFrame, s3_path: str, 
                    mode: str = 'overwrite', 
                    partition_by: Optional[list] = None):
        """Write data to S3"""
        writer = df.write.mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.parquet(s3_path)
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()


class DataStandardizer(SparkTransformer):
    """Standardize and clean data"""
    
    def standardize_column_names(self, df: DataFrame) -> DataFrame:
        """Standardize column names to lowercase with underscores"""
        for col_name in df.columns:
            new_name = col_name.lower().replace(' ', '_').replace('-', '_')
            df = df.withColumnRenamed(col_name, new_name)
        return df
    
    def remove_duplicates(self, df: DataFrame, subset: Optional[list] = None) -> DataFrame:
        """Remove duplicate rows"""
        return df.dropDuplicates(subset=subset)
    
    def handle_nulls(self, df: DataFrame, strategy: str = 'drop') -> DataFrame:
        """Handle null values"""
        if strategy == 'drop':
            return df.dropna()
        elif strategy == 'fill':
            return df.fillna({'numeric': 0, 'string': ''})
        return df
    
    def add_audit_columns(self, df: DataFrame) -> DataFrame:
        """Add audit columns"""
        return df \
            .withColumn('processed_timestamp', F.current_timestamp()) \
            .withColumn('record_hash', F.md5(F.concat_ws('|', *df.columns)))
    
    def standardize(self, df: DataFrame) -> DataFrame:
        """Apply all standardization steps"""
        df = self.standardize_column_names(df)
        df = self.remove_duplicates(df)
        df = self.add_audit_columns(df)
        return df
