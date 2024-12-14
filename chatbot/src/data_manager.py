import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv

class ProcurementDataManager:
    def __init__(self):
        load_dotenv()
        
        mongo_uri = os.getenv('MONGO_URI')
        self.client = MongoClient(mongo_uri)
        print("mongo database: ", os.getenv('MONGO_DATABASE'))
        self.db = self.client[os.getenv('MONGO_DATABASE')]
        
        # let's have standard names
        self.column_mapping = {
            'Creation Date': 'creation_date',
            'Purchase Date': 'purchase_date',
            'Fiscal Year': 'fiscal_year',
            'LPA Number': 'lpa_number',
            'Purchase Order Number': 'purchase_order_number',
            'Requisition Number': 'requisition_number',
            'Acquisition Type': 'acquisition_type',
            'Sub-Acquisition Type': 'sub_acquisition_type',
            'Acquisition Method': 'acquisition_method',
            'Sub-Acquisition Method': 'sub_acquisition_method',
            'Department Name': 'department_name',
            'Supplier Name': 'supplier_name',
            'Supplier Qualifications': 'supplier_qualifications',
            'Item Name': 'item_name',
            'Item Description': 'item_description',
            'Quantity': 'quantity',
            'Unit Price': 'unit_price',
            'Total Price': 'total_price',
            'Normalized UNSPSC': 'normalized_unspsc',
            'Commodity Title': 'commodity_title',
            'Classification Codes': 'classification_codes'
        }
    
    def load_dataset(self, csv_path):
        """
        Load dataset from CSV to MongoDB with proper data type handling
        
        Args:
            csv_path (str): Path to the procurement dataset CSV file
        
        Returns:
            int: Number of records loaded
        """
        try:
            print(f"Reading CSV file from: {csv_path}")
            df = pd.read_csv(csv_path)
            print(f"Successfully read {len(df)} rows")

            # Rename columns using mapping
            df = df.rename(columns=self.column_mapping)
            
            # Convert date columns
            date_columns = ['Creation Date', 'Purchase Date']
            for col in date_columns:
                if col in df.columns:
                    print(f"Converting {col} to datetime")
                    try:
                        # Try mm/dd/yyyy format first
                        df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce')
                    except Exception as e:
                        print(f"Error converting {col}: {str(e)}")
                        # If that fails, let pandas infer the format
                        df[col] = pd.to_datetime(df[col], errors='coerce')

            # Handle numeric columns
            numeric_columns = ['Quantity', 'Unit Price', 'Total Price']
            for col in numeric_columns:
                if col in df.columns:
                    print(f"Converting {col} to numeric")
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Replace NaN values with None for MongoDB compatibility
            df = df.replace({pd.NaT: None, np.nan: None})
            
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            # Get collection and clear existing data
            collection = self.db['procurement_data']
            collection.delete_many({})
            
            # Insert new records
            print(f"Inserting {len(records)} records into MongoDB")
            collection.insert_many(records)
            
            print("Data loading completed successfully")
            return len(records)
            
        except Exception as e:
            print(f"Error loading dataset: {str(e)}")
            raise
    
    def get_spending_by_fiscal_year(self):
        """
        Analyze total spending by fiscal year
        
        Returns:
            List of spending per fiscal year
        """
        collection = self.db['procurement_data']
        
        pipeline = [
            {
                '$group': {
                    '_id': '$fiscal_year',
                    'total_spending': {'$sum': '$total_price'},
                    'total_orders': {'$sum': 1}
                }
            },
            {
                '$sort': {'total_spending': -1}
            }
        ]
        
        return list(collection.aggregate(pipeline))
    
    def get_top_suppliers(self, top_n=10):
        """
        Get top suppliers by total spending
        
        Args:
            top_n (int): Number of top suppliers to return
        
        Returns:
            List of top suppliers by spending
        """
        collection = self.db['procurement_data']
        
        pipeline = [
            {
                '$group': {
                    '_id': '$supplier_name',
                    'total_spending': {'$sum': '$total_price'},
                    'total_orders': {'$sum': 1},
                    'supplier_qualifications': {'$first': '$supplier_qualifications'}
                }
            },
            {
                '$sort': {'total_spending': -1}
            },
            {
                '$limit': top_n
            }
        ]
        
        return list(collection.aggregate(pipeline))
    
    def get_acquisition_type_analysis(self):
        """
        Analyze spending and orders by acquisition type
        
        Returns:
            List of acquisition type breakdown
        """
        collection = self.db['procurement_data']
        
        pipeline = [
            {
                '$group': {
                    '_id': '$acquisition_type',
                    'total_spending': {'$sum': '$total_price'},
                    'total_orders': {'$sum': 1},
                    'avg_order_value': {'$avg': '$total_price'}
                }
            },
            {
                '$sort': {'total_spending': -1}
            }
        ]
        
        return list(collection.aggregate(pipeline))
    
    def get_commodity_spending(self, top_n=10):
        """
        Get top commodities by spending
        
        Args:
            top_n (int): Number of top commodities to return
        
        Returns:
            List of top commodities
        """
        collection = self.db['procurement_data']
        
        pipeline = [
            {
                '$match': {
                    'commodity_title': {'$ne': None}
                }
            },
            {
                '$group': {
                    '_id': '$commodity_title',
                    'total_spending': {'$sum': '$total_price'},
                    'total_orders': {'$sum': 1},
                    'avg_order_value': {'$avg': '$total_price'}
                }
            },
            {
                '$sort': {'total_spending': -1}
            },
            {
                '$limit': top_n
            }
        ]
        
        return list(collection.aggregate(pipeline))
    
    def get_spending_trend(self):
        """
        Analyze spending trend over time
        
        Returns:
            Monthly spending trend
        """
        collection = self.db['procurement_data']
        
        pipeline = [
            {
                '$group': {
                    '_id': {
                        'year': {'$year': '$creation_date'},
                        'month': {'$month': '$creation_date'}
                    },
                    'total_spending': {'$sum': '$total_price'},
                    'total_orders': {'$sum': 1}
                }
            },
            {
                '$sort': {
                    '_id.year': 1,
                    '_id.month': 1
                }
            }
        ]
        
        return list(collection.aggregate(pipeline))
    
    def close_connection(self):
        """Close MongoDB connection"""
        self.client.close()