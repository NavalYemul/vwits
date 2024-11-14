# Databricks notebook source
# MAGIC %md
# MAGIC Stream Data:
# MAGIC Data which grow over time

# COMMAND ----------

batch:
  df=spark.read.csv("path")
  df.write.saveAsTable("tablname")

Stream:
  df=spark.readStream.csv("path")
  df.writeStream.option("checkpointLocation","path").table("tblname")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/nyadls/raw/

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/nyadls/raw/stream_in/",header=True,inferSchema=True)

# COMMAND ----------

schema="id int, name string, gender string, salary integer, country string, date string"

# COMMAND ----------

df=spark.readStream.schema(schema).csv("dbfs:/mnt/nyadls/raw/stream_in/",header=True)

# COMMAND ----------

(spark
 .readStream
 .schema(schema)
 .csv("dbfs:/mnt/nyadls/raw/stream_in/",header=True)
 .writeStream
 .option("checkpointLocation","dbfs:/mnt/nyadls/raw/checkpoint/streamvwits")
 .trigger(once=True)
 .table("vwits.bronze.stream"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vwits.bronze.stream
