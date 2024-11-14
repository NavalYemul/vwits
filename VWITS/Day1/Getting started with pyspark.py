# Databricks notebook source
Data structures of spark
1. rdd (Resilient distributed dataset) (old core API)
2. DataFrame (Structured data)
3. Datasets (Scala )

# COMMAND ----------

Extraction/ Ingestion/ Reading
Reader API
df=spark.read.format("path")

csv
df=spark.read.csv("path")

json
df=spark.read.json("path")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/vw_raw/

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/vw_raw/user_data1.csv")

# COMMAND ----------

Spark is lazy evaluted

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/FileStore/tables/vw_raw/user_data1.csv")

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/vw_raw/user_data1.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/vw_raw/user_data1.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------


