# Databricks notebook source
df=spark.read.csv("/Volumes/datamaster/test/raw/user_data1.csv",header=True,inferSchema=True)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("test.user_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamaster.test.user_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`/Volumes/datamaster/test/raw/user_data1.csv`
