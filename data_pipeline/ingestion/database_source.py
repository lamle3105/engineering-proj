"""
Database Data Source
Ingests data from relational databases (PostgreSQL, MySQL, etc.)
"""

import pandas as pd
from sqlalchemy import create_engine
from typing import Dict, Any, List
from .base import DataSource


class DatabaseSource(DataSource):
    """Data source for relational databases"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection = config.get('connection', {})
        self.tables = config.get('tables', [])
        self.engine = self._create_engine()
    
    def _create_engine(self):
        """Create SQLAlchemy engine based on database type"""
        db_type = self.config.get('type', 'postgresql')
        host = self.connection.get('host')
        port = self.connection.get('port')
        database = self.connection.get('database')
        user = self.connection.get('user')
        password = self.connection.get('password')
        
        if db_type == 'postgresql':
            connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        elif db_type == 'mysql':
            connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        return create_engine(connection_string)
    
    def extract(self) -> pd.DataFrame:
        """Extract data from database tables"""
        all_data = []
        
        for table in self.tables:
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, self.engine)
            df['_table_name'] = table
            all_data.append(df)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate extracted data"""
        if data.empty:
            return False
        
        # Check for required columns
        if '_table_name' not in data.columns:
            return False
        
        return True
    
    def extract_table(self, table_name: str, query: str = None) -> pd.DataFrame:
        """Extract data from a specific table"""
        if query is None:
            query = f"SELECT * FROM {table_name}"
        
        return pd.read_sql(query, self.engine)
