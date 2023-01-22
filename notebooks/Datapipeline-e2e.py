# Databricks notebook source
display(dbutils.fs.ls("databricks-datasets/songs/"))

# COMMAND ----------

with open("/dbfs/databricks-datasets/songs/README.md",'r') as f:
    x = "".join(f.readlines())
    
print(x)

# COMMAND ----------

display(dbutils.fs.ls("databricks-datasets/songs/data-001"))

# COMMAND ----------

with open("/dbfs/databricks-datasets/songs/data-001/part-00000", "r") as f:
    x = "".join(f.readlines())
    
print(x)

# COMMAND ----------

