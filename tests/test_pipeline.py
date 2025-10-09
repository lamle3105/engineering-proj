"""
Unit tests for data pipeline components
"""

import unittest
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from data_pipeline.masking.data_masker import DataMasker
from data_pipeline.ingestion.file_source import FileSource


class TestDataMasker(unittest.TestCase):
    """Test data masking functionality"""
    
    def setUp(self):
        self.masker = DataMasker({'enabled': True, 'fields': {}})
    
    def test_email_masking(self):
        """Test email masking"""
        email = "john.doe@example.com"
        masked = self.masker.mask_email(email)
        
        # Should mask username but keep domain
        self.assertIn('@example.com', masked)
        self.assertNotEqual(email, masked)
        self.assertIn('*', masked)
    
    def test_phone_masking(self):
        """Test phone number masking"""
        phone = "555-123-4567"
        masked = self.masker.mask_phone(phone)
        
        # Should show only last 4 digits
        self.assertIn('4567', masked)
        self.assertIn('XXX', masked)
    
    def test_ssn_masking(self):
        """Test SSN masking"""
        ssn = "123-45-6789"
        masked = self.masker.mask_ssn(ssn)
        
        # Should show only last 4 digits
        self.assertIn('6789', masked)
        self.assertEqual(masked, 'XXX-XX-6789')
    
    def test_credit_card_masking(self):
        """Test credit card masking"""
        card = "1234-5678-9012-3456"
        masked = self.masker.mask_credit_card(card)
        
        # Should show only last 4 digits
        self.assertIn('3456', masked)
        self.assertIn('XXXX', masked)
    
    def test_hash_value(self):
        """Test value hashing"""
        value = "sensitive_data"
        hashed = self.masker.hash_value(value, 'sha256')
        
        # Should return hexadecimal hash
        self.assertIsInstance(hashed, str)
        self.assertEqual(len(hashed), 64)  # SHA-256 produces 64-char hex
        self.assertNotEqual(value, hashed)
    
    def test_tokenize(self):
        """Test tokenization"""
        value = "secret_value"
        token = self.masker.tokenize(value)
        
        # Should return token with prefix
        self.assertTrue(token.startswith('TOK_'))
        self.assertNotEqual(value, token)


class TestFileSource(unittest.TestCase):
    """Test file data source"""
    
    def test_file_source_config(self):
        """Test file source initialization"""
        config = {
            'name': 'test_csv',
            'location': 'test.csv',
            'format': 'csv'
        }
        source = FileSource(config)
        
        self.assertEqual(source.name, 'test_csv')
        self.assertEqual(source.format, 'csv')
        self.assertEqual(source.location, 'test.csv')


class TestDataValidation(unittest.TestCase):
    """Test data validation"""
    
    def test_dataframe_not_empty(self):
        """Test that dataframe is not empty"""
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 3)
    
    def test_dataframe_columns(self):
        """Test dataframe column validation"""
        df = pd.DataFrame({
            'customer_id': ['C001', 'C002'],
            'product_id': ['P001', 'P002'],
            'amount': [100.0, 200.0]
        })
        
        required_columns = ['customer_id', 'product_id', 'amount']
        for col in required_columns:
            self.assertIn(col, df.columns)


def run_tests():
    """Run all tests"""
    print("Running data pipeline tests...\n")
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestDataMasker))
    suite.addTests(loader.loadTestsFromTestCase(TestFileSource))
    suite.addTests(loader.loadTestsFromTestCase(TestDataValidation))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Return exit code
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    exit_code = run_tests()
    sys.exit(exit_code)
