# Databricks notebook source
Excatly Once

# COMMAND ----------

#structured streaming
(spark
 .readStream
 .schema(schema)
 .csv("dbfs:/mnt/nyadls/raw/stream_in/",header=True)
 .writeStream
 .option("checkpointLocation","dbfs:/mnt/nyadls/raw/checkpoint/streamvwits")
 .trigger(once=True)
 .table("vwits.bronze.stream"))

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .load("dbfs:/mnt/nyadls/raw/stream_in/")
 .writeStream
 .option("checkpointLocation","dbfs:/mnt/nyadls/raw/checkpoint/autoloader")
 .table("vwits.bronze.autoloader"))

# COMMAND ----------

metadata="dbfs:/mnt/nyadls/raw/checkpoint/autoloader"

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .option("cloudFiles.schemaLocation",f"{metadata}/schema")
 .load("dbfs:/mnt/nyadls/raw/stream_in/")
 .writeStream
 .option("checkpointLocation",f"{metadata}/checkpoint")
 .table("vwits.bronze.autoloader"))

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .option("cloudFiles.schemaLocation",f"{metadata}/schema")
 .load("dbfs:/mnt/nyadls/raw/stream_in/")
 .writeStream
 .option("checkpointLocation",f"{metadata}/checkpoint")
 .table("vwits.bronze.autoloader"))

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .option("cloudFiles.schemaLocation",f"{metadata}/schema")
 .load("dbfs:/mnt/nyadls/raw/stream_in/")
 .writeStream
 .option("checkpointLocation",f"{metadata}/checkpoint")
 .option("mergeSchema","true")
 .table("vwits.bronze.autoloader"))

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .option("cloudFiles.schemaLocation",f"{metadata}/schema")
 .option("cloudFiles.schemaEvolutionMode","rescue")
 .load("dbfs:/mnt/nyadls/raw/stream_in/")
 .writeStream
 .option("checkpointLocation",f"{metadata}/checkpoint")
 .option("mergeSchema","true")
 .table("vwits.bronze.autoloader"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vwits.bronze.autoloader
