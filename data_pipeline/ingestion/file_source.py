"""
File Data Source
Ingests data from files (CSV, JSON, Parquet) stored locally or in S3
"""

import pandas as pd
from typing import Dict, Any
from .base import DataSource


class FileSource(DataSource):
    """Data source for file-based data"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.location = config.get('location')
        self.format = config.get('format', 'csv')
        self.delimiter = config.get('delimiter', ',')
    
    def extract(self) -> pd.DataFrame:
        """Extract data from files"""
        if self.location.startswith('s3://'):
            return self._read_from_s3()
        else:
            return self._read_from_local()
    
    def _read_from_local(self) -> pd.DataFrame:
        """Read from local filesystem"""
        if self.format == 'csv':
            return pd.read_csv(self.location, delimiter=self.delimiter)
        elif self.format == 'json':
            return pd.read_json(self.location)
        elif self.format == 'parquet':
            return pd.read_parquet(self.location)
        else:
            raise ValueError(f"Unsupported format: {self.format}")
    
    def _read_from_s3(self) -> pd.DataFrame:
        """Read from S3"""
        if self.format == 'csv':
            return pd.read_csv(self.location, delimiter=self.delimiter)
        elif self.format == 'json':
            return pd.read_json(self.location)
        elif self.format == 'parquet':
            return pd.read_parquet(self.location)
        else:
            raise ValueError(f"Unsupported format: {self.format}")
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate extracted data"""
        return not data.empty
