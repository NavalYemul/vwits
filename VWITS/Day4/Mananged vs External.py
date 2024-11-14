# Databricks notebook source
delta table 
parquet +
_delta_log

# COMMAND ----------

# MAGIC %sql
# MAGIC Delta table (Managed)
# MAGIC 1.create table emp (id int, name string)
# MAGIC
# MAGIC 2.create table sales as select * from tablanme
# MAGIC
# MAGIC
# MAGIC 3. df=spark.read.csv("path")
# MAGIC df.write.saveAsTable("tbalanme")

# COMMAND ----------

# MAGIC %sql
# MAGIC Delta table (External)
# MAGIC 1.create table emp (id int, name string) location 'path'
# MAGIC
# MAGIC 2.create table sales location 'path ' as select * from tablanme
# MAGIC
# MAGIC
# MAGIC 3. df=spark.read.csv("path")
# MAGIC df.write.option("path","mnt/").saveAsTable("tbalanme")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp (id int, name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into emp values(1,'Naval')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history emp

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended emp 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp_ext (id int, name string) location 'dbfs:/FileStore/vwits/emp_ext'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into emp_ext values(1,'Naval')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended emp_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table emp_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/FileStore/vwits/emp_ext`
