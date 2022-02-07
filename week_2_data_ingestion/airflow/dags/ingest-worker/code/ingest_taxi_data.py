#!/usr/bin/env python
# coding: utf-8

import os
import subprocess
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

def ingest_taxi_data_to_postgres(csv_file, table_name):
    
    engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}')

    num_rows = 0
    num_rows_total = int(subprocess.run(['wc', '-l', csv_file], stdout=subprocess.PIPE, encoding='utf-8').stdout.split()[0])-1

    df_iter = pd.read_csv(csv_file, 
                          iterator=True, 
                          chunksize=100000,
                          parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
                          dtype={'store_and_fwd_flag': "string"})

    t_start = time()

    df = next(df_iter)
    num_rows += df.shape[0]

    # create table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    # insert data
    df.to_sql(name=table_name, con=engine, if_exists='append')

    print('Loading {:d}/{:d} rows, {:.3f} seconds elapsed'.format(num_rows, num_rows_total, time() - t_start))

    for df in df_iter:
        
        num_rows += df.shape[0]

        df.to_sql(name=table_name, con=engine, if_exists='append')

        print('Loading {:d}/{:d} rows, {:.3f} seconds elapsed'.format(num_rows, num_rows_total, time() - t_start))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest NYC taxi data to Postgres')
    parser.add_argument('--file', required=True, help='CSV-file which contains NY taxi data')
    parser.add_argument('--table', required=True, help='Name of db table, where file contents is saved')

    args = parser.parse_args()

    ingest_taxi_data_to_postgres(args.file, args.table)