# Installation

Create a conda environment ``kafka`` with Python 3.7, because ``faust`` needs 3.6 or 3.7.

```
conda env create -f conda_environment.yml
conda activate kafka
```

Start Kafka environment
```
docker-compose up -d
```

Create a new topics 
```
docker compose exec broker \
  kafka-topics --create \
    --topic hw6.employees \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1

docker compose exec broker \
  kafka-topics --create \
    --topic hw6.salary \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1
```

# Data

Data Source is an exampla employee database ([source](https://github.com/cristiscu/employees-test-database)).