# Databricks notebook source
# DBTITLE 1,Read
df=spark.read.json("/Volumes/datamaster/test/raw/customers.json")

# COMMAND ----------

# DBTITLE 1,Transformations
dataframe functions
.select
.alias
.withColumnRenamed
.withColumn


functions
col
current_date
upper

# COMMAND ----------

df.select("*")
df.select("customer_city","customer_email").display()

# COMMAND ----------

df.select("customer_city","customer_email").display()

# COMMAND ----------

df.select("customer_city".alias("city"),"customer_email").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("customer_city").alias("city"),"customer_email").display()

# COMMAND ----------

df.withColumnRenamed("customer_city","city").display()

# COMMAND ----------

df

# COMMAND ----------

df1=df.withColumnsRenamed({"customer_city":"city","customer_email":"email"})

# COMMAND ----------

df1.display()

# COMMAND ----------

# DBTITLE 1,New column
df1.withColumn("ingestion_date",current_date()).display()

# COMMAND ----------

# DBTITLE 1,Replace existing col
df1.withColumn("customer_name",upper("customer_name")).display()

# COMMAND ----------


