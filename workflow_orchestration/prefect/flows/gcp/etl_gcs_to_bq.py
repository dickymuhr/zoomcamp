from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(year: int,month: int,color :str) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoomcamp-gcs") # Name of block in orion, not bucket
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(f"./{gcs_path}")

task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"ROW PROCESSED: {len(df)}")
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df['passenger_count'].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoomp-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="axial-coyote-376114",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(name="To BigQuery Flow")
def etl_gcs_to_bq(year,month,color):
    """Main ETL flow to load data into Big Query"""


    path = extract_from_gcs(year,month,color)
    df = transform(path)
    write_bq(df)

@flow(name="To BigQuery Parent")
def etl_gcs_to_bq_parent(
    year,
    months,
    color,
):
    for month in months:
        etl_gcs_to_bq(year,month,color)

if __name__ == "__main__":
    color = "yellow"
    months = [2,3]
    year = 2019
    etl_gcs_to_bq_parent(year,months,color)