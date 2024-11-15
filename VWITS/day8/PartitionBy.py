# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/2007.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display().limit(500)

# COMMAND ----------

df.groupBy("Dest").count()

# COMMAND ----------

df.groupBy("Dest").count().display()

# COMMAND ----------

df.write.option("path","dbfs:/mnt/nyadls/raw/vwits_delta/airlines").saveAsTable("airlines")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail hive_metastore.default.airlines

# COMMAND ----------

df.write.option("path","dbfs:/mnt/nyadls/raw/vwits_delta/airlines_dest").partitionBy("Dest").saveAsTable("airlines_dest")

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe detail hive_metastore.default.airlines_dest

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.airlines where Dest='BFF'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.airlines where FlightNum = 3101

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.airlines_dest where FlightNum = 3101

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.airlines_dest where FlightNum = 3101

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.airlines_dest where Dest='BFF'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize hive_metastore.default.airlines_dest
# MAGIC zorder by (FlightNum)
