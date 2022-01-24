#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    load_csv('https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv', 
             engine, 
             ingest_func=ingest_yellow_tripdata)

    load_csv('https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv', 
             engine, 
             ingest_func=ingest_taxi_zones)


def load_csv(url, engine, ingest_func=None):

    # -nc (--no-clobber) doesn't download file, if it exists already
    # -P (--directory-prefix) saves file to directory
    os.system(f'wget {url} -nc -P csv')

    csv_file_path = f'csv/{os.path.basename(url)}'

    if ingest_func != None:
        ingest_func(csv_file_path, engine)


def ingest_taxi_zones(csv, engine, table_name='taxi_zones'):

    df = pd.read_csv('csv/taxi+_zone_lookup.csv')
    df.to_sql(name=table_name, con=engine, if_exists='replace')


def ingest_yellow_tripdata(csv, engine, table_name='yellow_taxi_data'):
    
    df_iter = pd.read_csv(csv, 
                          iterator=True, 
                          chunksize=100000,
                          parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
                          dtype={'store_and_fwd_flag': "string"})

    t_start = time()

    df = next(df_iter)
    # create table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    # insert data
    df.to_sql(name=table_name, con=engine, if_exists='append')

    t_end = time()
    print('inserted {:d} rows, took {:.3f} seconds'.format(df.shape[0], t_end - t_start))

    t_start = time()

    for df in df_iter:
        
        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another {:d} rows, took {:.3f} seconds'.format(df.shape[0], t_end - t_start))

        t_start = time()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest NYC taxi data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')

    args = parser.parse_args()

    main(args)