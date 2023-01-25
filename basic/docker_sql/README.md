# To run postgre with environment variable, mount volume, and specify port to access

#Create local volume 
docker volume create --name dtc_postgres_volume_local -d local

docker run -it \
    --name postgresql \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGREES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13

# To view folder, first change permisson
sudo chmod a+rwx ny_taxi_postgres_data

# To login
pgcli -h localhost -p 5432 -u root -d ny_taxi
