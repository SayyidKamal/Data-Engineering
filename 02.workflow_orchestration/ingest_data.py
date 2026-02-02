import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--pg-user', required=True)
@click.option('--pg-pass', required=True)
@click.option('--pg-host', required=True)
@click.option('--pg-port', required=True)
@click.option('--pg-db', required=True)
@click.option('--target-table', required=True)
@click.option('--year', type=int, required=True)
@click.option('--month', type=int, required=True)
@click.option('--chunksize', type=int, default=100000)
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month, chunksize):
    
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace'
            )
            first = False
            print(f'Created table {target_table} in the database.')

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append'
        )
        print(f'Inserted chunk of size {len(df_chunk)} into {target_table}.')

if __name__ == '__main__':
    run()