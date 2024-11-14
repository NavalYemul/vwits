# Databricks notebook source
# MAGIC %run /Workspace/Users/naval.datamaster@gmail.com/VWITS/day2/includes

# COMMAND ----------

# DBTITLE 1,Extract
df=spark.read.csv(f"{input_path}circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

# DBTITLE 1,Transformation
# MAGIC %md
# MAGIC ##### Task
# MAGIC 1. rename col name circuitId to circuit_id
# MAGIC 2. renaem col name circuitRef to ciruit_ref
# MAGIC 3. drop the url col
# MAGIC 4. add new col ingestion_date with current_timestamp

# COMMAND ----------

# DBTITLE 1,Load
df1=df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.drop("url")

# COMMAND ----------

df2=add_ingestion_date(df1)

# COMMAND ----------

df2.write.mode("overwrite").saveAsTable("circuits")
