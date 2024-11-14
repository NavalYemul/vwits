-- Databricks notebook source
use catalog vwits;

-- COMMAND ----------

create schema if not exists vwits.bronze;
create schema if not exists vwits.silver;
create schema if not exists vwits.gold;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC input_path="dbfs:/mnt/nyadls/raw/csv/"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def add_ingestion_col(df):
-- MAGIC   df1=df.withColumn("ingestion_date",current_timestamp())
-- MAGIC   return df1
