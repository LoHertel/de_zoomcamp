FROM python:3.9

RUN apt-get install -y wget
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_taxi_data.py ingest_taxi_data.py 
COPY csv csv/

ENTRYPOINT [ "python", "ingest_taxi_data.py" ]

#docker build -t taxi_ingest:v001 .