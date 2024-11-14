# Databricks notebook source
1.Databricks
2.Spark (big data frameworkd)
3.Delta Lake

# COMMAND ----------

Dbutilities 
Databricks utitlites (linux like command) manipulate dbfs
Interactive with rest of databricks 
DBFS: databricks files system

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/vw_raw")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/tables/vw/user_data1.csv","dbfs:/FileStore/tables/vw_raw/")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/vw/",True)
