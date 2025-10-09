#!/usr/bin/env python3
"""
Sample data generator for testing the pipeline
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os


def generate_sample_customers(n=1000):
    """Generate sample customer data"""
    customers = []
    
    for i in range(n):
        customer = {
            'customer_id': f'CUST{str(i+1).zfill(6)}',
            'first_name': random.choice(['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily']),
            'last_name': random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia']),
            'email': f'customer{i+1}@example.com',
            'phone': f'{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}',
            'address': f'{random.randint(100, 9999)} Main St',
            'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
            'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ']),
            'zip_code': str(random.randint(10000, 99999)),
            'country': 'USA'
        }
        customers.append(customer)
    
    return pd.DataFrame(customers)


def generate_sample_products(n=200):
    """Generate sample product data"""
    products = []
    
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']
    subcategories = {
        'Electronics': ['Laptops', 'Phones', 'Tablets', 'Accessories'],
        'Clothing': ['Men', 'Women', 'Kids', 'Shoes'],
        'Home & Garden': ['Furniture', 'Kitchen', 'Decor', 'Garden'],
        'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports'],
        'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Children']
    }
    
    for i in range(n):
        category = random.choice(categories)
        subcategory = random.choice(subcategories[category])
        
        product = {
            'product_id': f'PROD{str(i+1).zfill(6)}',
            'product_name': f'{subcategory} Product {i+1}',
            'category': category,
            'subcategory': subcategory,
            'brand': random.choice(['BrandA', 'BrandB', 'BrandC', 'BrandD']),
            'unit_cost': round(random.uniform(10, 500), 2),
            'unit_price': 0  # Will calculate based on cost
        }
        product['unit_price'] = round(product['unit_cost'] * random.uniform(1.3, 2.5), 2)
        products.append(product)
    
    return pd.DataFrame(products)


def generate_sample_locations(n=50):
    """Generate sample location data"""
    locations = []
    
    for i in range(n):
        location = {
            'location_id': f'LOC{str(i+1).zfill(4)}',
            'store_name': f'Store {i+1}',
            'address': f'{random.randint(100, 9999)} Commerce Blvd',
            'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
            'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ']),
            'zip_code': str(random.randint(10000, 99999)),
            'country': 'USA',
            'region': random.choice(['Northeast', 'West', 'Midwest', 'South'])
        }
        locations.append(location)
    
    return pd.DataFrame(locations)


def generate_sample_transactions(customers_df, products_df, locations_df, n=10000):
    """Generate sample transaction data"""
    transactions = []
    
    start_date = datetime.now() - timedelta(days=365)
    
    for i in range(n):
        customer = customers_df.sample(1).iloc[0]
        product = products_df.sample(1).iloc[0]
        location = locations_df.sample(1).iloc[0]
        
        quantity = random.randint(1, 5)
        discount = round(random.uniform(0, 0.3), 4)
        unit_price = product['unit_price']
        
        transaction = {
            'transaction_id': f'TXN{str(i+1).zfill(8)}',
            'customer_id': customer['customer_id'],
            'product_id': product['product_id'],
            'location_id': location['location_id'],
            'quantity': quantity,
            'unit_price': unit_price,
            'discount': discount,
            'tax_rate': 0.08,
            'transaction_date': start_date + timedelta(days=random.randint(0, 365))
        }
        
        subtotal = quantity * unit_price * (1 - discount)
        transaction['tax_amount'] = round(subtotal * transaction['tax_rate'], 2)
        transaction['total_amount'] = round(subtotal + transaction['tax_amount'], 2)
        transaction['transaction_timestamp'] = transaction['transaction_date']
        
        transactions.append(transaction)
    
    return pd.DataFrame(transactions)


def save_sample_data(output_dir='../data/raw'):
    """Generate and save all sample data"""
    print("Generating sample data...")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate data
    customers = generate_sample_customers(1000)
    products = generate_sample_products(200)
    locations = generate_sample_locations(50)
    transactions = generate_sample_transactions(customers, products, locations, 10000)
    
    # Save to CSV
    customers.to_csv(f'{output_dir}/customers.csv', index=False)
    products.to_csv(f'{output_dir}/products.csv', index=False)
    locations.to_csv(f'{output_dir}/locations.csv', index=False)
    transactions.to_csv(f'{output_dir}/sales_transactions.csv', index=False)
    
    print(f"Sample data saved to {output_dir}")
    print(f"  - customers.csv: {len(customers)} records")
    print(f"  - products.csv: {len(products)} records")
    print(f"  - locations.csv: {len(locations)} records")
    print(f"  - sales_transactions.csv: {len(transactions)} records")


if __name__ == '__main__':
    save_sample_data()
