# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv",header=True,inferSchema=True)

# COMMAND ----------

df.createOrReplaceTempView("bikeview")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, current_timestamp() as ingestion_date from bikeview
