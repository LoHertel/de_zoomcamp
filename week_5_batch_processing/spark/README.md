## Create Docker container

```
docker build -t spark_taxi_ingest:v002 .
```

## Run container


Start a spark instance in docker:

```bash
docker run --rm -it \
  --name spark \
  spark_taxi_ingest:v002 /bin/bash
```
