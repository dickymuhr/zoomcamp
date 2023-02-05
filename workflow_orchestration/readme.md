# Workflow Orchestration
1. Run postgre and pdAdmin Docker
2. Run ingest_data.py

```bash
python flows/gcp/etl_web_to_gcs.py 
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

# GCP Flows
To read data from web and write to GCS
And to read data from GCS to BigQuery

# Deployment
Deployment in Prefect is a server-side concept that encapsulates a flow, allowing it to be scheduled, and triggered via the API. [https://docs.prefect.io/concepts/deployments/#deployments-overview](https://docs.prefect.io/concepts/deployments/#deployments-overview)

## Using CLI
Build, build the metadata of the flow
```bash
prefect deployment build flows/deployment/parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
```
Apply (trigger flow runs), send metadata to prefect API:
```bash
prefect deployment apply etl_parent_flow-deployment.yaml
```
Start Agent (executing runs), is a process running on executing env, *pulling from a work queue*:
```bash
prefect agent start  --work-queue "default"
```
-> And run the flow through the Orion UI


Build and apply together, with scheduling
```bash
prefect deployment build flows/deployment/parameterized_flow.py:etl_parent_flow -n etl2 --cron "0 0 * * *" -a
```
Help comannd
```bash
prefect deployment build --help
```

### Running on Docker Container
We'll build and image and upload it to dockerhub to save or flow code
```bash
docker image build -t dickymuhr/prefect:zoom .
```
```bash
docker image push dickymuhr/prefect:zoom
```
Dont forget to add Docker blocks in orion

## Using Python

```bash
python flows/deployment/docker_deploy.py
```

Set updated config
```bash
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
```

```bash
prefect agent start  -q default
```

Run from CLI
```bash
prefect deployment run etl-parent-flow/docker-flow -p "months=[1,2]"
```