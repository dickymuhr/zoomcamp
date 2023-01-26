#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine
import argparse
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    # Download the csv
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()


    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


    df.head(0).to_sql(name=table_name,con=engine,if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')


    for df in df_iter:
        t_start = time()
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name,con=engine,if_exists='append')
        
        t_end = time()
        print(f"Inserted.. took {t_end-t_start} second")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # User, pass, host, port, db name, table name
    # url of the csv

    parser.add_argument('--user', help="username for pg")
    parser.add_argument('--password', help="pass for pg")
    parser.add_argument('--host', help="host for pg")
    parser.add_argument('--port', help="port for pg")
    parser.add_argument('--db', help="db name for pg")
    parser.add_argument('--table_name', help="name of the table where will write the result to")
    parser.add_argument('--url', help="url to the csv file")

    args = parser.parse_args()
    main(args)