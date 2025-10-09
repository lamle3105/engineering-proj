"""
API Data Source
Ingests data from REST APIs
"""

import pandas as pd
import requests
from typing import Dict, Any, Optional
from .base import DataSource


class APISource(DataSource):
    """Data source for REST APIs"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.endpoint = config.get('endpoint')
        self.auth_type = config.get('auth_type', 'none')
        self.api_key = config.get('api_key')
        self.headers = self._setup_headers()
    
    def _setup_headers(self) -> Dict[str, str]:
        """Setup request headers based on auth type"""
        headers = {'Content-Type': 'application/json'}
        
        if self.auth_type == 'bearer' and self.api_key:
            headers['Authorization'] = f"Bearer {self.api_key}"
        elif self.auth_type == 'api_key' and self.api_key:
            headers['X-API-Key'] = self.api_key
        
        return headers
    
    def extract(self) -> pd.DataFrame:
        """Extract data from API endpoint"""
        try:
            response = requests.get(self.endpoint, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Convert to DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # If response is dict with a data key
                if 'data' in data:
                    df = pd.DataFrame(data['data'])
                else:
                    df = pd.DataFrame([data])
            else:
                df = pd.DataFrame()
            
            return df
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"API request failed: {str(e)}")
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate extracted data"""
        return not data.empty
