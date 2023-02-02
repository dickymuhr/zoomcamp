# Workflow Orchestration
1. Run postgre and pdAdmin Docker
2. Run ingest_data.py

```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python flows/gcp/etl_web_to_gcs.py \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```

# User Interface
```bash
prefect orion start
```
![My Image](https://github.com/dickymuhr/zoomcamp/blob/main/workflow_orchestration/asset/prefect%20ui.png?raw=true)

# Connect to GCP

Register blocks from prefect GCP modules
```bash
prefect block register -m prefect_gcp
Successfully registered 6 blocks

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Registered Blocks             ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ BigQuery Warehouse            │
│ GCP Cloud Run Job             │
│ GCP Credentials               │
│ GcpSecret                     │
│ GCS Bucket                    │
│ Vertex AI Custom Training Job │
└───────────────────────────────┘

 To configure the newly registered blocks, go to the Blocks page in the Prefect UI: None/blocks/catalog
```

Then create GCS block with credentials using **Prefect Orion**
See more documentation on : [https://prefecthq.github.io/prefect-gcp/cloud_storage/](https://prefecthq.github.io/prefect-gcp/cloud_storage/)