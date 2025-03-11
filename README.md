## Multinational Retail Data Centralisation Project

## Project Aim
In this project we work for a multinational company that sells various goods across the globe. The aim of the project is to centralise retail data from multiple sources into a structured PostgreSQL database making the company data easily accessible and analysable. 
One of the goals of this project was to produce a system that will store the companies data in a database, so that it may be accessed from one centralised location and acts as a single source of truth for sales data. We will then query the database to get up-to-date metrics for the business.


## Key Aspects
- Extracting data from cloud databases and local sources.
- Cleaning data to remove inconsistencies and errors.
- Structuring the data for efficient querying and analysis.
- Storing processed data in a PostgreSQL database

## Installation
1- Clone repository. git clone https://github.com/Abshir099/multinational-retail-data-centralisation878.git

2- Navigate to project directory. cd Multination Retail Data Centralisation Project

3- Install required Python packages and dependencies.Use pip install command to install.
Example:pip install yaml

- Python 3
- Pandas
- yaml
- boto3
- numpy
- SQLAlchemy
- requests
- psycopg2
- tabula

4- install PostgreSQL and create a server and a Database called "sales_data"

5- record database credentials, do so by right clicking on your server and then select properties to view credentials

6- Create YAML files: db_creds.yaml and sales_db_creds.yaml and enter database credentials. 


## Usage
### Python Classes used in this project:
DataExtractor: found in DataExtractor.py, 
 DataCleaning: found in data_cleaning.py, 
 DatabaseConnector: found in database_utils.py
#
- Run the main.py script to begin data centralisation
- To Extract Data, we used the DataExtractor in order to pull Data from required sources.
- Once Data has been Extracted, clean data using the DataCleaning class methods according to specified cleaning requirements. 
- Uploading Data. DatabaseConnector Class contains methods needed for connecting and uploading to SQL Database
- Processed Data is then uploaded to a Postgres SQL Database.


## What I've Learned
- How to build and manage ETL pipelines
- Efficient Database management and SQL Querying
- Data cleaning using Pandas to improve data quality.
- Managing cloud and local database interactions using SQLAlchemy.

## File Structure
Multination-Retail-Data-Centralisation-Project/
├── main.py
│   └── Run this script to create a local database from extracted data.
├── DataExtractor.py
│   └── Contains the DataExtractor class and methods used to extract data from required sources.
├── data_cleaning.py
│   └── Defines the DataCleaning class and methods to clean and process data.
├── database_utils.py
│   └── Implements the DatabaseConnector class and methods for interacting with the database.
├── Milestone3_Database_schema.sql
│   └── SQL queries used to develop the database schema.
├── Milestone4_Business_Data.sql
│   └── SQL queries used to retrieve up-to-date metrics from data.
├── README.md
│   └── Provides an overview of the project and its contents.
└── .gitignore
    └── Specifies files and directories to be ignored by Git.


## License
See LICENSE for more information