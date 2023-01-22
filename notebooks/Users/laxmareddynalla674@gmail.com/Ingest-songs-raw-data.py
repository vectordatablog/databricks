# Databricks notebook source
from pyspark.sql.types import DoubleType, StringType, StructType, StructField, IntegerType

# COMMAND ----------

file_path = "/databricks-datasets/songs/data-001/"
table_name = "songs_datapipeline"
checkpoint_path = "/tmp/ingest-songs-raw-data/_checkpoint/songs_data"

# COMMAND ----------

with open("/dbfs/databricks-datasets/songs/data-001/part-00000","r") as f:
    x = "".join(f.readline())
print(x)

# COMMAND ----------

with open("/dbfs/databricks-datasets/songs/README.md","r") as f:
    x = "".join(f.readlines())
print(x)

# COMMAND ----------

schema = StructType(
    [
        StructField("artist_id", StringType(), True),
        StructField("artist_lati", DoubleType(), True),
        StructField("artist_long", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("end_of_fade_in", DoubleType(), True),
        StructField("key", IntegerType(), True),
        StructField("key_confidence", DoubleType(), True),
        StructField("loudness", DoubleType(), True),
        StructField("release", StringType(), True),
        StructField("song_hotness", DoubleType(), True),
        StructField("song_id", StringType(), True),
        StructField("start_of_fade_out", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("time_signature", DoubleType(), True),
        StructField("time_signature_cofidence", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("year", DoubleType(), True),
        StructField("partial_sequence", IntegerType(), True)      
    ]
    )
(spark.readStream
 .format("cloudFiles")
 .schema(schema)
 .option("cloudFiles.format", "csv")
 .option("sep","\t")
 .load(file_path)
 .writeStream
 .option("checkpointLocation", checkpoint_path)
 .trigger(availableNow=True)
 .toTable(table_name)
)

# COMMAND ----------

