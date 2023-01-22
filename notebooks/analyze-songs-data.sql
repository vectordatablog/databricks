-- Databricks notebook source
CREATE OR REPLACE VIEW 
  artists_by_year
AS SELECT
  artist_name,
  year
FROM prepared_songs_data
WHERE year > 0

-- COMMAND ----------

SELECT 
  artist_name,
  COUNT(artist_name) AS num_songs, year
FROM artists_by_year
GROUP BY artist_name, year
ORDER BY num_songs DESC, year DESC

-- COMMAND ----------

CREATE OR REPLACE VIEW danceable_songs
AS SELECT
  artist_name,
  title,
  tempo
FROM prepared_songs_data
WHERE 
  time_signature = 4
  AND
  tempo between 100 and 140;
  
SELECT * FROM danceable_songs ORDER BY tempo DESC limit 10;

-- COMMAND ----------

