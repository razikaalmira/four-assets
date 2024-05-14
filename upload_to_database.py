import pandas as pd
import numpy as np
import datetime as dt
import json
import psycopg2
from config import config
from sqlalchemy import create_engine

param_database = config('postgresql')
csv_files = config('csv_directory')

# btc = pd.read_csv('Data Historis BTC_IDR BTC Indonesia 2017-2024.csv')
# 1. import csv
# 2. lower_columnnames
# 3. insert to table

def lower_columnnames(dataframe_list):
    for df in dataframe_list:
        df.columns = [x.lower() for x in df.columns]

def init_connection():
    # create connection
    conn = psycopg2.connect(**param_database,options='-csearch_path=dbo,dev')
    # create cursor
    cur = conn.cursor()
    conn_string = f"postgresql://{param_database['user']}:{param_database['password']}@{param_database['host']}/{param_database['dbname']}?options=-csearch_path%3Ddbo,dev"
    engine = create_engine(conn_string).connect()
    return conn, cur, engine

def create_table(conn,cur,sql_query):
    try:
        cur.execute(sql_query)
    except Exception as e:
        print(f"Error: {e}")
        print(f"Query: {sql_query}")
        conn.rollback()
    else:
        conn.commit()
        print(f"Table has been created")
        
def insert_to_table(conn,dataframe,table_name):
    try:
        dataframe.to_sql(name=table_name,con=engine,if_exists='replace',index=False)
        # conn.autocommit = True
    except Exception as e:
        print(f"Error: {e}")
    else:
        conn.commit()
        print(f"{table_name} has been inserted")

def close_connections():
    cur.close()
    conn.close()
    print("Database cursor and connections have been closed")

btc_temp = """
    CREATE TABLE IF NOT EXISTS btc (
        tanggal         VARCHAR(20),
        terakhir        VARCHAR(20),
        pembukaan       VARCHAR(20),
        tertinggi       VARCHAR(20),
        terendah        VARCHAR(20),
        volume          VARCHAR(20),
        perubahan       VARCHAR(20)
    );
    """

ihsg_temp = """
    CREATE TABLE IF NOT EXISTS ihsg (
        tanggal         VARCHAR(20),
        terakhir        VARCHAR(20),
        pembukaan       VARCHAR(20),
        tertinggi       VARCHAR(20),
        terendah        VARCHAR(20),
        volume          VARCHAR(20),
        perubahan       VARCHAR(20)
    );
    """

usd_temp = """
    CREATE TABLE IF NOT EXISTS usd (
        tanggal         VARCHAR(20),
        terakhir        VARCHAR(20),
        pembukaan       VARCHAR(20),
        tertinggi       VARCHAR(20),
        terendah        VARCHAR(20),
        volume          VARCHAR(20),
        perubahan       VARCHAR(20)
    );
    """

gold_temp = """
    CREATE TABLE IF NOT EXISTS gold (
        tanggal         VARCHAR(20),
        terakhir        VARCHAR(20),
        pembukaan       VARCHAR(20),
        tertinggi       VARCHAR(20),
        terendah        VARCHAR(20),
        volume          VARCHAR(20),
        perubahan       VARCHAR(20)
    );
    """

if __name__ == "__main__":
    # conn, cur, engine = init_connection()
    
    # create_table(conn,cur,btc_temp)
    # create_table(conn,cur,ihsg_temp)
    # create_table(conn,cur,usd_temp)
    # create_table(conn,cur,gold_temp)

    for name,directory in csv_files.items():
        df = pd.read_csv(directory)
        df.columns = [x.lower() for x in df.columns]
        # insert_to_table(conn,df,name)
        
    # close_connections()
    
    