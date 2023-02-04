#!/usr/bin/env python
# coding: utf-8

import os
import sys
import pandas as pd
from sqlalchemy import create_engine
import argparse
from time import time
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    # Download the csv
    os.system(f"wget {url} -O {csv_name}")
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, parse_dates=["tpep_pickup_datetime","tpep_dropoff_datetime"])
    
    df_list = []
    for df in df_iter:
        df_list.append(df)
    
    df_combined = pd.concat(df_list)
    return df_combined

@task(log_prints=True)
def transform_data(df):
    missing_count = 0
    missing_count_transformed= 0
    df_transformed_list = []

    # Create generator function
    def chunks(df, chunk_size):
        for i in range(0, len(df), chunk_size):
            yield df[i: i + chunk_size]
    # Transform each chunk
    for chunk in chunks(df, 100000):
        print(f"Transforming {len(chunk)} data ...")
        missing_count+=chunk['passenger_count'].isin([0]).sum()
        df_transformed = chunk[chunk["passenger_count"] != 0]
        missing_count_transformed+=df_transformed['passenger_count'].isin([0]).sum()
        df_transformed_list.append(df_transformed)

    # Merge all chunk
    df_combined = pd.concat(df_transformed_list)
    print(f"pre: missing passenger count: {missing_count}")
    print(f"post: missing passenger count: {missing_count_transformed}")
    return df_combined

@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # engine.connect()

        df.head(0).to_sql(name=table_name,con=engine,if_exists='replace')

        # Create generator function
        def chunks(df, chunk_size):
            for i in range(0, len(df), chunk_size):
                yield df[i: i + chunk_size]
        # Ingest each chunk
        for chunk in chunks(df, 100000):
            t_start = time()
            chunk.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print(f"Inserted.. took {t_end-t_start} second")

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name:str):
    print(f"Logging Subflow for: {table_name}")


@flow(name="Ingest Flow")
def main_flow(table_name, url):
    

    log_subflow(table_name)
    raw_data = extract_data(url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)

if __name__ == '__main__':
    table_name = "yellow_taxi_trips"
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    main_flow(table_name,url)