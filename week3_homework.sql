CREATE OR REPLACE EXTERNAL TABLE `aqueous-nebula-375121.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp_bigquery/green2022_taxi_data.parquet']
);

SELECT COUNT(1) FROM aqueous-nebula-375121.ny_taxi.external_green_tripdata;

CREATE OR REPLACE TABLE aqueous-nebula-375121.ny_taxi.external_green_tripdata_non_partitoned AS
SELECT * FROM aqueous-nebula-375121.ny_taxi.external_green_tripdata;


ALTER TABLE `aqueous-nebula-375121.ny_taxi.external_green_tripdata_non_partitoned`
RENAME TO green_tripdata_non_partitoned;

SELECT COUNT(1) FROM aqueous-nebula-375121.ny_taxi.green_tripdata_non_partitoned;

SELECT COUNT(DISTINCT PULocationID ) FROM aqueous-nebula-375121.ny_taxi.green_tripdata_non_partitoned;

SELECT COUNT(DISTINCT PULocationID ) FROM aqueous-nebula-375121.ny_taxi.external_green_tripdata;

SELECT COUNT(fare_amount) FROM aqueous-nebula-375121.ny_taxi.external_green_tripdata WHERE fare_amount = 0;

CREATE OR REPLACE TABLE aqueous-nebula-375121.ny_taxi.green_tripdata_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM aqueous-nebula-375121.ny_taxi.external_green_tripdata;

SELECT DISTINCT PULocationID 
FROM aqueous-nebula-375121.ny_taxi.green_tripdata_clustered
WHERE lpep_pickup_datetime BETWEEN '2022-06-01'AND '2022-06-30';

SELECT DISTINCT PULocationID 
FROM aqueous-nebula-375121.ny_taxi.green_tripdata_non_partitoned
WHERE lpep_pickup_datetime BETWEEN '2022-06-01'AND '2022-06-30';
