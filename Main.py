from DataExtractor import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

if __name__ == "__main__": 
#connector
    utils = DatabaseConnector()
    #engine
    engine = utils.init_db_engine("C:\\Users\\abshi\\OneDrive\\Documents\\IT\\Multination Retail Data Centralisation Project\\db_creds.yaml")

    #####
    #engine,connector, credentials, extractor
    db_connector = DatabaseConnector()
    engine = db_connector.init_db_engine("C:\\Users\\abshi\\OneDrive\\Documents\\IT\\Multination Retail Data Centralisation Project\\db_creds.yaml")
    extractor = DataExtractor()
    sales_creds_file = "C:\\Users\\abshi\\OneDrive\\Documents\\IT\\Multination Retail Data Centralisation Project\\sales_data_db.yaml"

    #clean user data
    get_db_tables = DataExtractor.list_db_tables(engine)
    table_name = 'legacy_users'
    read_rds = DataExtractor.read_rds_table(engine, table_name)
    retrieve_pdf = DataExtractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    cleaned_user_data = DataCleaning.clean_user_data(engine)

    #clean card data
    link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    cleaned_card_data = DataCleaning.clean_card_data(link)

    #store data
    headers = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
    return_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    num_stores = 451
    DataExtractor.list_number_of_stores(return_stores_endpoint, headers)
    DataExtractor.retrieve_stores_data(store_endpoint,headers,num_stores)
    cleaned_store_data = DataCleaning.clean_store_data(store_endpoint, headers, num_stores)

    #products data
    s3_key = 's3://data-handling-public/products.csv'
    extracted_s3_data = DataExtractor.extract_from_s3(s3_key)
    cleaned_products_data = DataCleaning.clean_products_data(extracted_s3_data)

    #orders_data
    orders_table = 'orders_table'
    orders_data = DataExtractor.read_rds_table(engine,orders_table)
    cleaned_orders_data = DataCleaning.clean_orders_data(orders_data)

    #date_data
    data_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json."
    date_data = DataExtractor.extract_from_s3(data_address)

    #upload clean data to respective tables
    #   db_connector.upload_to_db(cleaned_user_data, 'dim_users', sales_creds_file)
    #   db_connector.upload_to_db(cleaned_card_data, 'dim_card_details',sales_creds_file)
    #   db_connector.upload_to_db(cleaned_store_data, 'dim_store_details',sales_creds_file)
    #   db_connector.upload_to_db(cleaned_products_data, 'dim_products', sales_creds_file)
    #   db_connector.upload_to_db(date_data, 'dim_date_times', sales_creds_file)
    #   db_connector.upload_to_db(orders_data, 'orders_table', sales_creds_file)

    ##########


