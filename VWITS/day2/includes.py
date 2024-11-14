# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path="/Volumes/datamaster/test/raw/"

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df=input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists formula1;
# MAGIC use formula1
