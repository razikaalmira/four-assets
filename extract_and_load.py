import pandas as pd
import numpy as np
import datetime as dt
import requests
import json
import psycopg2
from config import config
from sqlalchemy import create_engine

API_key = config('BPS_API')['api_key']
param_database = config('postgresql')
csv_files = config('csv_directory')


def get_monthly_cpi(url):
    try:
        api_key = API_key
        url = f'{url}/{api_key}'
        response = requests.get(url)
        response.raise_for_status()
        fetched = response.json()
        fixed = json.dumps(fetched)
        data = json.loads(fixed)
    except Exception as e:
        print(f"Error: {e}")
    
    vervar = next(item['val'] for item in data['vervar'] if item['label'].upper() == 'INDONESIA')
    var = next(item['val'] for item in data['var'])
    turvar = next(item['val'] for item in data['turvar'])
    code = str(vervar)+str(var)+str(turvar)
    
    datacontent = data['datacontent']
    year_mapping = {item['val']:item['label'] for item in data['tahun']}
    month_mapping = {item['val']:item['label'] for item in data['turtahun']}
    last_four_years = [key for key in year_mapping.keys() if key >= max(year_mapping)-3]   
    filter_keys = [i+str(num) for num in range(1,13) for i in [code + str(num) for num in last_four_years]]
    filtered = {key: value for key, value in datacontent.items() if key in filter_keys}
    df = pd.DataFrame.from_dict(filtered,orient='index').reset_index().rename(columns={'index':'year_month',0:'CPI'})
    
    df['year_month'] = df['year_month'].apply(lambda x: x[len(code):])
    df['year_code'] = df['year_month'].str[:3].astype('int32')
    df['month_code'] = df['year_month'].str[3:].astype('int32')
    df['Year'] = df['year_code'].map(year_mapping)
    df['Month'] = df['month_code'].map(month_mapping)
    return df


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

cpi_temp = """
    CREATE TABLE IF NOT EXISTS cpi (
        year_month    INTEGER,
        CPI           DECIMAL,
        year_code     INTEGER,
        month_code    INTEGER,
        year          INTEGER,
        month         VARCHAR(20)
    );
    """

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
    # df_2019 = get_monthly_cpi('https://webapi.bps.go.id/v1/api/list/model/data/lang/ind/domain/0000/var/2/key')
    # df_2023 = get_monthly_cpi('https://webapi.bps.go.id/v1/api/list/model/data/lang/ind/domain/0000/var/1709/key')
    # df_2024 = get_monthly_cpi('https://webapi.bps.go.id/v1/api/list/model/data/lang/ind/domain/0000/var/2261/key')
    # cpi = pd.concat([df_2019,df_2023,df_2024],ignore_index=True)
    
    conn, cur, engine = init_connection()

    create_table(conn,cur,cpi_temp)
    create_table(conn,cur,btc_temp)
    create_table(conn,cur,ihsg_temp)
    create_table(conn,cur,usd_temp)
    create_table(conn,cur,gold_temp)

    # cpi_links = ['https://webapi.bps.go.id/v1/api/list/model/data/lang/ind/domain/0000/var/2/key',
    #         'https://webapi.bps.go.id/v1/api/list/model/data/lang/ind/domain/0000/var/1709/key',
    #         'https://webapi.bps.go.id/v1/api/list/model/data/lang/ind/domain/0000/var/2261/key']

    # cpi_list = [get_monthly_cpi(link) for link in cpi_links]
    # cpi = pd.concat(cpi_list,ignore_index=True)

    # insert_to_table(conn,cpi,'cpi')

    for name,directory in csv_files.items():
        df = pd.read_csv(directory)
        df.columns = [x.lower() for x in df.columns]
        insert_to_table(conn,df,name)
        
    close_connections()