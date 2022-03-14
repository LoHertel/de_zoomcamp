from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep
from datetime import datetime


def load_avro_schema_from_file():
    key_schema = avro.load('data/employee_key.avsc')
    value_schema = avro.load('data/employee_value.avsc')

    return key_schema, value_schema


def send_record():
    key_schema, value_schema = load_avro_schema_from_file()

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(producer_config, 
                            default_key_schema=key_schema, 
                            default_value_schema=value_schema)

    file = open('data/employee.csv')

    csvreader = csv.reader(file)
    header = next(csvreader)
    for row in csvreader:
        key = {'employee_id': int(row[0])}
        value = {'employee_id': int(row[0]), 
                 'first_name': row[1], 
                 'last_name': row[2], 
                 'address': row[3], 
                 'date': int(datetime.strptime(row[4], '%Y-%m-%d').timestamp()),
                 'action': row[5]}

        try:
            producer.produce(topic='hw6.employees', key=key, value=value)
        except Exception as e:
            print(f"Exception while producing record value - {value}: {e}")
        else:
            print(f"Successfully producing record value - {value}")

        producer.flush()
        sleep(1)

if __name__ == "__main__":
    send_record()