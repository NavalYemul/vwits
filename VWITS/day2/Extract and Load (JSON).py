# Databricks notebook source
# MAGIC %md
# MAGIC ### EL
# MAGIC Three ways 
# MAGIC
# MAGIC 1. PySpark (DataFrame)
# MAGIC 2. Spark SQL
# MAGIC 3. UI

# COMMAND ----------

# MAGIC %md
# MAGIC #### PySpark

# COMMAND ----------

# DBTITLE 1,reading
df=spark.read.json("/Volumes/datamaster/test/raw/customers.json")

# COMMAND ----------

# DBTITLE 1,writing
df.write.mode("overwrite").saveAsTable("test.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.customers

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from file_format.`path`

# COMMAND ----------

# DBTITLE 1,CTAS
# MAGIC %sql
# MAGIC Create or replace table test.customers_sql as 
# MAGIC select * from json.`/Volumes/datamaster/test/raw/customers.json`
