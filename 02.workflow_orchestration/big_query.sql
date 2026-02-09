--Creating an External Table from gcs path
create or replace External Table `de-time-486008.zoomcamp.external_yellow_tripdata_2024`
options (
  format = 'PARQUET',
  uris = ['gs://kestra-detime-ahmad-486008/yellow_tripdata_2024-*.parquet', 'gs://kestra-detime-ahmad-486008/yellow_tripdata_2024-*.parquet']
);

-- Check yellow trip data
Select count(*) from de-time-486008.zoomcamp.external_yellow_tripdata_2024;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE de-time-486008.zoomcamp.yellow_tripdata_non_partitioned AS
SELECT * FROM de-time-486008.zoomcamp.external_yellow_tripdata_2024;

-- Select * from materialized table
SELECT * FROM de-time-486008.zoomcamp.yellow_tripdata_non_partitioned;

-- Querying from external table has no cost.
-- not part. Count distinct PULocationIDs
-- estimate 155.12 mb
Select count(distinct PULocationID) 
from de-time-486008.zoomcamp.yellow_tripdata_non_partitioned;


-- part. Count distinct PULocationIDs
-- estimate 0b
Select count(distinct PULocationID) 
from de-time-486008.zoomcamp.external_yellow_tripdata_2024;


-- How many records have a fare_amount of 0?
select count(*)
from de-time-486008.zoomcamp.yellow_tripdata_non_partitioned
where fare_amount = 0;


--Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID). Seee below
-- retreive PULocationID 
-- 155 mb
Select PULocationID
from de-time-486008.zoomcamp.yellow_tripdata_non_partitioned;

-- retreive PULocationID, DOLocationID
-- 310 mb
Select PULocationID, DOLocationID
from de-time-486008.zoomcamp.yellow_tripdata_non_partitioned;



--Do we have any Partitions?
SELECT table_name, partition_id, total_rows
FROM `zoomcamp.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;


--Lets Partition and cluster our table to reduce cost ;)
CREATE OR REPLACE TABLE de-time-486008.zoomcamp.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM de-time-486008.zoomcamp.external_yellow_tripdata_2024;


-- Materialized distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15
-- 310.24 Mb
SELECT distinct(VendorID)
from de-time-486008.zoomcamp.yellow_tripdata_non_partitioned
where tpep_dropoff_datetime 
between '2024-03-01' and '2024-03-16';

-- Part+clus distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15
-- 28.83 Mb
SELECT distinct(VendorID)
from de-time-486008.zoomcamp.yellow_tripdata_partitioned_clustered
where tpep_dropoff_datetime 
between '2024-03-01' and '2024-03-16';




