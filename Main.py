from DataExtractor import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector


#cleaning class

db_connector = DatabaseConnector()
extractor = DataExtractor()
engine = db_connector.init_db_engine("C:\\Users\\abshi\\OneDrive\\Documents\\IT\\Multination Retail Data Centralisation Project\\db_creds.yaml")

#clean user data
cleaned_user_data = DataCleaning.clean_user_data(engine)
#clean card data
link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
cleaned_card_data = DataCleaning.clean_card_data(link)
#clean store data
headers = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
return_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
num_stores = 451
cleaned_store_data = DataCleaning.clean_store_data(store_endpoint, headers, num_stores)
#clean
s3_key = 's3://data-handling-public/products.csv'
extracted_s3_data = DataExtractor.extract_from_s3(s3_key)
cleaned_products_data = DataCleaning.clean_products_data(extracted_s3_data)


#date_data
data_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json."
date_data = extractor.extract_from_s3(data_address)

#upload clean data to respective tables
# db_connector.upload_to_db(cleaned_user_data, 'dim_users', 'sales_data_db.yaml') Done
db_connector.upload_to_db(cleaned_card_data, 'dim_card_details','sales_data_db.yaml')
db_connector.upload_to_db(cleaned_store_data, 'dim_store_details','sales_data_db.yaml')
db_connector.upload_to_db(cleaned_products_data, 'dim_products', 'sales_data_db.yaml')
db_connector.upload_to_db(date_data, 'dim_date_times', 'sales_data_db.yaml')


