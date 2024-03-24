import requests
from datetime import date
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import os
from dotenv import load_dotenv
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.coincap.io/v2"

headers = {"accept": "application/json", "Authorization": "Bearer "+API_KEY}

def get_assets_from_api():
    try:
        url = f"{BASE_URL}/assets"
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        data = response.json()
        return data
        
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"Other error occurred: {err}")
    

def manipulate_assets():
        raw_data = get_assets_from_api()
        data = raw_data["data"]
        
        currency_rank = []
        currency_symbol = []
        currency_name = []
        currency_price = []
        
        for asset in data:
            asset_id = asset["id"]
            asset_rank = asset["rank"]
            asset_name = asset["name"]
            asset_symbol = asset["symbol"]
            asset_price = asset["priceUsd"]
            
            currency_rank.append(asset_rank)
            currency_symbol.append(asset_symbol)
            currency_name.append(asset_name)
            currency_price.append(asset_price)
            
    
        coincap_price_tracker = {
            "Rank": currency_rank,
            "Symbol": currency_symbol,
            "Name": currency_name,
            "Price": currency_price
        }
        
        table_df = pd.DataFrame(coincap_price_tracker)
        today = date.today()
        inserted_date = pd.Timestamp.now().isoformat()
        table_df["INSERTED_DATE"] = inserted_date
        
        return table_df



def crypto_to_snowflake(dataframe):
    # Sends DataFrames to Snowflake
    conn = snowflake.connector.connect(
        user = os.getenv("SNOWFLAKE_USER"),
        password= os.getenv("SNOWFLAKE_PASSWORD"),
        account= os.getenv("SNOWFLAKE_ACCOUNT_ID"),
        warehouse= os.getenv("SNOWFLAKE_WAREHOUSE"),
        role= os.getenv("SNOWFLAKE_ROLE")
    )
    
    # ToDO: extract to config

    schema_name = "COINCAP"
    table_name = "COINCAP_EXCHANGE_RATES"
    conn.cursor().execute("CREATE DATABASE IF NOT EXISTS CRYPTO_DB")
    conn.cursor().execute("USE DATABASE CRYPTO_DB")
    conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS Coincap")
    conn.cursor().execute("USE SCHEMA Coincap")
    conn.cursor().execute("CREATE OR REPLACE TABLE " "Coincap_Exchange_Rates(Rank integer, Symbol string, Name string, Price float)" )
    
    status, num_chunks, num_rows, output = write_pandas(
    conn,
    dataframe,
    schema=schema_name,
    table_name=table_name,
    database="CRYPTO_DB",
    auto_create_table=True,
    overwrite=True
    )
    
get_assets_from_api()
table_df = manipulate_assets()
crypto_to_snowflake(table_df)