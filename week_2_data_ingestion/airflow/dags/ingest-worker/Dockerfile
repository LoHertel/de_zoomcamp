FROM python:3.9.10-slim

#RUN apt-get install -y wget
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary

WORKDIR /app
RUN mkdir code
RUN mkdir csv

#COPY ingest_taxi_data.py ingest_taxi_data.py 
#COPY csv csv/

ENTRYPOINT [ "bash" ]

# docker build -t ingest_pg:v001 .
# docker run --rm -it ingest_pg:v001