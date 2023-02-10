### To run postgre with environment variable, mount volume, and specify port to access

# Create local volume 
```bash
docker volume create --name dtc_postgres_volume_local -d local
```
```bash
docker run -it \
    --name postgresql \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGREES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13
```

# To view folder, first change permisson
```bash
sudo chmod a+rwx ny_taxi_postgres_data
```

# To login
```bash
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

# NY Taxi Dataset
* https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz

# Use pgAdmin
```bash
docker run -it \
    --name pgAdmin \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8081:80 \
    dpage/pgadmin4
```

# Use network to integrate container
```bash
docker network create pg-network
```
```bash
docker run -it \
    --name pg-database \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGREES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    postgres:13
```

```bash
docker run -it \
    --name pgAdmin \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8081:80 \
    --network=pg-network \
    dpage/pgadmin4
```

# Data Ingestion
```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```

To ensure data can be accessed before build image
```bash
sudo chmod -R 755 ny_taxi_postgres_data
```

Build the image
```bash
docker build -t taxi_ingest:v001 .
```

Run the image
```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

docker run -it \
  --name ingest_data \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
```

# Docker compose
To run multiple images that specified in docker-compose.yaml. Also add command to pass CLI argument to ingest_data.py (it will be added to ENTRYPOINT)

```bash
services:
  pgdatabase:
    container_name: pg-database
    image: postgres:13
    environment:
    - POSTGRES_USER=root
    - POSTGRES_PASSWORD=root 
    - POSTGREES_DB=ny_taxi
    volumes:
    - "./ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
    ports:
    - "5432:5432"
  pgadmin:
    container_name: pgAdmin
    image: dpage/pgadmin4
    environment:
    - PGADMIN_DEFAULT_EMAIL=admin@admin.com
    - PGADMIN_DEFAULT_PASSWORD=root
    volumes:
    - "./data_pgadmin:/var/lib/pgadmin"
    ports:
    - "8081:80"
  ingestdata:
    container_name: ingestdata
    image: taxi_ingest:v002
    command: 
    - --user=root 
    - --password=root 
    - --host=pg-database 
    - --port=5432 
    - --db=ny_taxi 
    - --table_name=yellow_taxi_trips 
    - --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz 
    - --table_lookup_name=zones 
    - --url_lookup=https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv 
```

```bash
docker-compose up -d # detach terminal
```

To turn it off and delete container
```bash
docker-compose down
```