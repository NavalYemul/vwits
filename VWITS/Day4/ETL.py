# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/nyadls/raw/csv/

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists vwits

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/nyadls/raw/csv/linkedin_job_postings.csv",header=True)

# COMMAND ----------

df.write.saveAsTable("vwits.linkedin_job_posting")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail vwits.linkedin_job_posting

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from linkedin_job_posting 

# COMMAND ----------

# MAGIC %sql
# MAGIC desc
