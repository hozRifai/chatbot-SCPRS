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
        self.db = self.client[os.getenv('MONGO_DATABASE')]
        
    
    def standardize_column_names(self, col):
        return col.lower().replace(' ', '_')
    
    def load_dataset(self, csv_path):
        try:
            df = pd.read_csv(csv_path)
            df = df.rename(columns= lambda x: self.standardize_column_names(x))
            
            # date_columns = ['creation_date', 'purchase_date']
            # for col in date_columns:
            #     if col in df.columns:
            #         print(f"Converting {col} to datetime")
            #         try:
            #             # Try mm/dd/yyyy format first
            #             df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce')
            #         except Exception as e:
            #             print(f"Error converting {col}: {str(e)}")
            #             # If that fails, let pandas infer the format
            #             df[col] = pd.to_datetime(df[col], errors='coerce')

            # Handle numeric columns
            numeric_columns = ['quantity', 'unit_price', 'total_price', 'supplier_zip_code', 'classification_codes']
            for col in numeric_columns:
                if col in df.columns:
                    print(f"Converting {col} to numeric")
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # replace Nan with None for Mongodb compatibility
            df = df.replace({pd.NaT: None, np.nan: None})
            records = df.to_dict('records')
            collection = self.db['procurement_data']
            collection.delete_many({})
            
            print(f"Inserting {len(records)} records into MongoDB")
            collection.insert_many(records)
            
            print("Data loading completed successfully")
            return len(records)
            
        except Exception as e:
            print(f"Error loading dataset: {str(e)}")
            raise
    
    def close_connection(self):
        """Close MongoDB connection"""
        self.client.close()