import pandas as pd
from sqlalchemy import inspect
import database_utils
import tabula
import requests
import boto3
from io import StringIO
import time
import os

class DataExtractor:
    @staticmethod
    def list_db_tables(engine): 
        inspector = inspect(engine) 
        tables = inspector.get_table_names()
        print(tables)
        return tables
    @staticmethod    
    def read_rds_table(engine, table_name): 
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query,engine)     
        return df
    @staticmethod
    def retrieve_pdf_data(link):
        print(type(link))
        extracted_data = tabula.read_pdf(link,pages='all',multiple_tables=True)
        # Combine all tables into a single DataFrame to check if extracted_data is a list
        if isinstance(extracted_data, list):
            return pd.concat(extracted_data, ignore_index=True)
        # then use Use pd.concat() to merge all the DataFrames in the list into a single DataFrame.
        elif isinstance(extracted_data, pd.DataFrame):
            return extracted_data
        else:
            raise ValueError("Error: extracted_data is neither a list nor a valid DataFrame.")
  
    @staticmethod
    def list_number_of_stores(store_endpoint,headers):
        try:
            response = requests.get(store_endpoint, headers=headers)
            if response.status_code == 200:
                stores_data = response.json()
                #print ("stores data:", stores_data)
                return stores_data
            else:
                return f"Error 1:{response.status_code}:{response.text}"
        except requests.exceptions.RequestException as e:
            print(f"Error2:{e}")

#extracts all the stores from the API saving them in a pandas DataFrame.
    @staticmethod
    def retrieve_stores_data(return_stores_endpoint, headers, num_stores, csv_file = "stores_data.csv"):

        if os.path.exists(csv_file):
            print(f"Loading data from existing file: {os.path.abspath(csv_file)}")
            return pd.read_csv(csv_file)
        print("Fetching data from API...")
       
        extracted_stores = []

        for stores_num in range(0, num_stores):
            store_url = f"{return_stores_endpoint}/{stores_num}"       
            response = requests.get(store_url, headers=headers)

            if response.status_code == 200:
                store_data = response.json()
                #print(f"Store Data for {stores_num}: {store_data}") 

                if isinstance (store_data, dict):
                    extracted_stores.append(store_data)
                else:
                    print(response.text)
                    raise Exception( f"Error3: {response.status_code}, {response.text}")
            else:
                print(f"Failed to fetch store data for {stores_num}: {response.status_code}")

        stores_df = pd.DataFrame(extracted_stores)
        if stores_df.empty:
            raise ValueError("Retrieved data is empty. Please check the API response.")
        
        return stores_df
        

    @staticmethod
    def extract_from_s3(s3_key):
        s3 = boto3.client('s3', region_name = 'eu-west-1')
        s3_bucket = s3_key.split('/')[2]
        file_key = '/'.join(s3_key.split('/')[3:])
        print(f"Bucket: {s3_bucket}, File: {file_key}")

        load_s3_data = s3.get_object(Bucket = 'data-handling-public', Key = 'products.csv')
        raw_s3_data = load_s3_data['Body'].read().decode('utf-8')

        #check if file extension is csv
        file_ext = file_key.split('.')[-1]
        if file_ext == "csv":
            extracted_s3_data = pd.read_csv(StringIO(raw_s3_data))
            return extracted_s3_data
        elif file_ext == "json":
            extracted_s3_data = pd.read_json(StringIO(raw_s3_data))
            return extracted_s3_data
        else:
            return None
    
