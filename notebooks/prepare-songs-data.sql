-- Databricks notebook source
CREATE OR REPLACE TABLE
prepared_songs_data(
  artist_id STRING,
  artist_name STRING,
  duration DOUBLE,
  release STRING,
  tempo DOUBLE,
  time_signature DOUBLE,
  title STRING,
  year DOUBLE,
  processed_time TIMESTAMP
);


-- COMMAND ----------

INSERT INTO
  prepared_songs_data
SELECT
  artist_id,
  artist_name,
  duration,
  release,
  tempo,
  time_signature,
  title,
  year,
  current_timestamp()
FROM
  songs_datapipeline

-- COMMAND ----------

