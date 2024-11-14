# Databricks notebook source
# MAGIC %run /Workspace/Users/naval.datamaster@gmail.com/VWITS/day5/Project/includes

# COMMAND ----------

# DBTITLE 1,Sales
df=spark.read.csv(f"{input_path}sales.csv",header=True,inferSchema=True)
df_sales=add_ingestion_col(df)
df_sales.write.mode("overwrite").saveAsTable("bronze.sales")

# COMMAND ----------

# DBTITLE 1,Products
dfp=spark.read.csv(f"{input_path}products.csv",header=True,inferSchema=True)
df_products=add_ingestion_col(dfp)
df_products.write.mode("overwrite").saveAsTable("bronze.products")

# COMMAND ----------

# DBTITLE 1,Customers
dfc=spark.read.csv(f"{input_path}customers.csv",header=True,inferSchema=True)
df_customers=add_ingestion_col(dfc)
df_customers.write.mode("overwrite").saveAsTable("bronze.customers")
