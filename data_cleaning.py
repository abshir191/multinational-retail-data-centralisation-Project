import pandas as pd
from sqlalchemy import inspect
from DataExtractor import DataExtractor
import numpy as np
import re
from database_utils import DatabaseConnector

class DataCleaning():
    @staticmethod
    def clean_user_data(engine):
      user_data_to_clean = DataExtractor.read_rds_table(engine,'legacy_users')
      user_data_to_clean.replace("NULL",np.nan, inplace = True)
      user_data_to_clean.dropna(inplace = True)
      user_data_to_clean["join_date"] = pd.to_datetime(user_data_to_clean["join_date"], errors="coerce", format="mixed")
      user_data_to_clean.dropna(inplace=True)
      cleaned_user_data = user_data_to_clean 
      print(f"Number of user rows after cleaning: {cleaned_user_data.shape[0]}")
      #user_data_to_clean.to_csv("raw_user_data.csv", index=False)
      return cleaned_user_data

# Exporting raw card data

    @staticmethod
    def clean_card_data(link):
        card_data_to_clean = DataExtractor.retrieve_pdf_data(link)
        card_data_to_clean.replace("NULL", np.nan, inplace = True)
        card_data_to_clean.dropna(inplace = True)
        card_data_to_clean.drop_duplicates(subset = "card_number", inplace = True)
        card_data_to_clean['card_number'] = card_data_to_clean['card_number'].apply(lambda x: re.sub(r'\D', '', str(x)))
        card_data_to_clean.dropna(subset = ["card_number"], inplace = True)
        card_data_to_clean["date_payment_confirmed"] = pd.to_datetime(card_data_to_clean["date_payment_confirmed"], errors="coerce", format="mixed")
        card_data_to_clean.dropna(subset=["date_payment_confirmed"], inplace=True)
        cleaned_card_data = card_data_to_clean
        print(f"Number of card rows after cleaning: {cleaned_card_data.shape[0]}")
        return cleaned_card_data
    
    @staticmethod 
    def clean_store_data(return_stores_endpoint, headers, num_stores):
       raw_store_data = DataExtractor.retrieve_stores_data(return_stores_endpoint, headers, num_stores)
       raw_store_data.replace("NULL", np.nan)
       raw_store_data.dropna()
       raw_store_data["opening_date"] = pd.to_datetime(raw_store_data["opening_date"],format="%Y-%m-%d", errors="coerce")
       raw_store_data.dropna(subset=["opening_date"])
       raw_store_data["staff_numbers"] = raw_store_data["staff_numbers"].str.replace(r'[^0-9]', '', regex = True)
       raw_store_data.dropna(subset=["staff_numbers"])
       cleaned_store_data = raw_store_data
       return cleaned_store_data

    @staticmethod
    def convert_product_weights(extracted_s3_data):
       weights_data = extracted_s3_data["weight"]
       weights_kg = []
       for i in weights_data:
          #if it's already a float/int add to new list
          if isinstance(i,(float,int)):
             weights_kg.append(i)
             #cleaning of string values
          elif isinstance(i,(str)):
             searched_values = re.findall(r'\d+\.?\d*', i)
             if searched_values:
                extracted_values = float(searched_values[0])
                if "kg" in i:
                   weights_kg.append(f"{extracted_values}kg")
                elif 'g' in i:
                   weights_kg.append(f"{extracted_values/1000}kg")
                elif "l" in i:
                   weights_kg.append(f"{extracted_values}kg")
                elif 'ml' in i:
                   weights_kg.append(f"{extracted_values/1000}kg")
                elif 'lb' in i:
                   weights_kg.append(f"{extracted_values * 0.45359237}kg")
                elif 'oz' in i:
                   weights_kg.append(f"{extracted_values *  0.0283495}kg")
                else:
                   weights_kg.append(np.nan)
             else:
                weights_kg.append(np.nan)
          else:
             weights_kg.append(np.nan)

       extracted_s3_data['weight'] = weights_kg
       extracted_s3_data.dropna(subset = ['weight'], inplace = True)
       cleaned_product_weights = extracted_s3_data
       return cleaned_product_weights

    @staticmethod
    def clean_products_data(extracted_s3_data):
       raw_store_weights_data = DataCleaning.convert_product_weights(extracted_s3_data)
       #print(type(raw_store_weights_data))
       raw_store_weights_data = raw_store_weights_data.replace("NULL", np.nan)
       raw_store_weights_data.dropna(inplace = True)
       cleaned_products_data = raw_store_weights_data
       return cleaned_products_data
    
    @staticmethod
    def clean_orders_data(orders_data):
      orders_data.drop(["first_name", "last_name", "1", "level_0"], axis=1, inplace = True)
      cleaned_orders_data = orders_data
      return cleaned_orders_data
    
    @staticmethod
    def clean_date_times(date_data):
       date_data.replace("NULL", np.nan, inplace = True)
       date_data.dropna(inplace = True)
       date_data["day"] = pd.to_numeric(date_data["day"], errors = "coerce")
       date_data["month"] = pd.to_numeric(date_data["month"], errors = "coerce")
       date_data["year"] = pd.to_numeric(date_data["year"], errors = "coerce")
       cleaned_date_data = date_data
       return cleaned_date_data
