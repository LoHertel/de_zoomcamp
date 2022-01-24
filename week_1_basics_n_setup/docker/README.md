## Create network

Create a network to ensure, that ingestion container is in the same network as postgres database.
```bash
docker network create pg-network
```

## Run Postgres in Docker

Make sure, the data folder is full acessible for the postgres instance:
```bash
sudo chmod a+rwx ny_taxi_postgres_data
```

Start a postgres instance in docker:

```bash
docker run --rm -it \
  --name pg-database \
  --network=pg-network  \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

## Load Data from CSV to Postgres

Build docker container:
```bash
docker build -t taxi_ingest:v001 .
```

Start container an run ingestion automatically:
```bash
docker run --rm -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi
```

Now all CSV data is imported into the database and could be used for analyses.
