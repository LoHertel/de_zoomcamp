import subprocess

from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest_to_postgres(user, password, host, port, db, table_name, csv_file, execution_date):
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

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