from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3)
def fetch(dataset_url:str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df:pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df:pd.DataFrame, color:str, dataset_file:str) -> Path:
    """Write DataFrame out as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)
    df.to_parquet(path, compression="gzip")
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoomcamp-gcs") # Name of block in orion, not bucket
    gcs_block.upload_from_path(from_path = path, to_path = path, timeout=180)
    return 

@flow(name="Ingest Flow")
def etl_web_to_gcs(year,month,color) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow(name="Parent Ingest Flow")
def etl_web_to_gcs_parent(
    year: int = 2021,
    months: list[int] = [1,2],
    color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year,month,color)

if __name__ == '__main__':
    color = "yellow"
    months = [2,3]
    year = 2019
    etl_web_to_gcs_parent(year,months,color)