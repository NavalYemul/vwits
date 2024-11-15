# Databricks notebook source
# MAGIC %run /Workspace/Users/naval.datamaster@gmail.com/vwits/VWITS/day5/Project/includes

# COMMAND ----------

dbutils.widgets.text("source","")
source_file=dbutils.widgets.get("source")

# COMMAND ----------

df=spark.read.csv(f"{input_path}{source_file}.csv",header=True,inferSchema=True)
df1=add_ingestion_col(df)
df1.write.mode("overwrite").saveAsTable(f"bronze.{source_file}")
