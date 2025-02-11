import pandas as pd
from sqlalchemy import inspect
import database_utils
import tabula
import requests
import boto3
from io import StringIO
import time

class DataExtractor:
    @staticmethod
    def list_db_tables(engine): 
        inspector = inspect(engine) 
        tables = inspector.get_table_names()
        print(tables)
        return tables
    @staticmethod    
    def read_rds_table(engine, table_name): 
        query = (f"SELECT * FROM {table_name}")
        df = pd.read_sql_query(query,engine)     
        return df
    @staticmethod
    def retrieve_pdf_data(link):
        extracted_data = tabula.read_pdf(link, pages = 'all')
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
                return f"Error:{response.status_code}:{response.text}"
        except requests.exceptions.RequestException as e:
            print(f"Error:{e}")

#extracts all the stores from the API saving them in a pandas DataFrame.
    @staticmethod
    def retrieve_stores_data(return_stores_endpoint, headers, num_stores):
        extracted_stores = []

        for stores_num in range(0, num_stores):
            store_url = f"{return_stores_endpoint}/{stores_num}"     
            start_time = time.time()    
            response = requests.get(store_url, headers=headers, timeout=15)
            end_time = time.time()

            if response.status_code == 200:
                store_data = response.json()  
                extracted_stores.extend(store_data)
            else:
                print(response.text)
                raise Exception( f"Error: {response.status_code}, {response.text}")

        stores_df = pd.DataFrame(extracted_stores)
        return stores_df
        

    @staticmethod
    def extract_from_s3(s3_key):
        s3 = boto3.client('s3')
        s3_bucket = s3_key.split('/')[2]
        file_key = '/'.join(s3_key.split('/')[3:])

        load_s3_data = s3.get_object(Bucket = s3_bucket, Key = file_key)
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
            print(f"Error: {file_ext} is not recognised")
            return None
        #s3_key = "s3://data-handling-public/products.csv"
        #dataframe = DataExtractor.extract_from_s3(s3_key)
        #if dataframe is not None:
        #    print(dataframe.head())
        
    
    
    


            

if __name__ == "__main__":
    utils = database_utils.DatabaseConnector()
    engine = utils.init_db_engine("C:\\Users\\abshi\\OneDrive\\Documents\\IT\\Multination Retail Data Centralisation Project\\db_creds.yaml")
    #get_db_tables = utils.list_db_tables this was a mistake as i called it using an instance which isn't nessecarry beacuse i used a static method, meaning i can jus call it directly. done below:
    get_db_tables = DataExtractor.list_db_tables(engine)
    table_name = 'legacy_users'
    read_rds = DataExtractor.read_rds_table(engine, table_name)
    #print(read_rds)
    link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    retrieve_pdf = DataExtractor.retrieve_pdf_data(link)
    #print(retrieve_pdf)
    headers = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
    return_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    DataExtractor.list_number_of_stores(return_stores_endpoint, headers)
    num_stores = 451
    DataExtractor.retrieve_stores_data(store_endpoint,headers,num_stores)
    s3_key = 'products.csv'
    orders_df = DataExtractor.read_rds_table(engine,'orders_table')
    data_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json."
