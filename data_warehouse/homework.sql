-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp.rides_fhv`
OPTIONS (
  format = 'CSV',
  compression='gzip',
  uris = ['gs://de_zoomcamp_prefect/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- CREATE BQ Table
CREATE OR REPLACE TABLE `hw.rides_fhv`
AS (
  SELECT * FROM `dezoomcamp.rides_fhv`
)

---------------------------------------- NUMBER 1 ----------------------------------------
--- What is the count for fhv vehicle records for year 2019?
SELECT COUNT(*) FROM `axial-coyote-376114.dezoomcamp.rides_fhv` 
--- 43_244_696

---------------------------------------- NUMBER 2 ----------------------------------------
--- Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
SELECT COUNT(DISTINCT affiliated_base_number) 
FROM `axial-coyote-376114.dezoomcamp.rides_fhv`  -- change to hw if want BQ Table
--- 3163

---What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
--- External: This query will process 0 B when run
--- Internal: This query will process 317.94 MB when run.

---------------------------------------- NUMBER 3 ----------------------------------------
--- How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
SELECT COUNT(*) 
FROM `axial-coyote-376114.hw.rides_fhv` 
WHERE PUlocationID IS NULL AND DOlocationID IS NULL
--- 717,748

---------------------------------------- NUMBER 4 ----------------------------------------
--- What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
--- Partition by pickup_datetime Cluster on affiliated_base_number
CREATE OR REPLACE TABLE `hw.rides_fhv_partition`
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY 
  affiliated_base_number   AS
SELECT * FROM `dezoomcamp.rides_fhv`

---------------------------------------- NUMBER 5 ----------------------------------------
SELECT DISTINCT Affiliated_base_number
FROM `hw.rides_fhv_partition`
WHERE DATE(pickup_datetime) BETWEEN DATE('2019-03-01') AND DATE('2019-03-31')
--- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

---------------------------------------- NUMBER 6 ----------------------------------------
--- Where is the data stored in the External Table you created?
--- GCP Bucket


---------------------------------------- NUMBER 7 ----------------------------------------
--- It is best practice in Big Query to always cluster your data:
-- if less than 1 Gb, not