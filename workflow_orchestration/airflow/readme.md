# For official setup please refer to
Please first update docker-compose version at least to 1.29
```bash
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/bin/docker-compose
```

[https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2022/week_2_data_ingestion/airflow/1_setup_official.md](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2022/week_2_data_ingestion/airflow/1_setup_official.md)

or from [Youtube Videos](https://www.youtube.com/watch?v=lqDMzReAtrw&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

# Execution

0. Build the image (only once)
```bash
docker-compose build
```
1. Initialize 
```bash
docker-compose up airflow-init
``` 

2. Run all Airflow container
```bash
docker-compose up -d
```