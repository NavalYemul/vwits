# Databricks notebook source
# MAGIC %run /Workspace/Users/naval.datamaster@gmail.com/VWITS/day5/Project/includes

# COMMAND ----------

df=spark.table("vwits.bronze.sales")
df1=df.dropDuplicates().dropna().drop("ingestion_date")
df1.write.mode("overwrite").saveAsTable("silver.sales")

# COMMAND ----------

df=spark.table("vwits.bronze.customers")
df1=df.dropDuplicates(["customer_id"]).select("customer_id","customer_name","customer_email","customer_city","customer_state")
df1.write.mode("overwrite").saveAsTable("silver.customers")

# COMMAND ----------

df=spark.table("vwits.bronze.products")
df1=df.dropDuplicates(["product_id"]).select("product_id","product_name","product_category","product_price")
df1.write.mode("overwrite").saveAsTable("silver.products")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silver.sales_customer as 
# MAGIC (select s.customer_id,s.transaction_id,s.product_id,s.discount_amount,s.total_amount,c.customer_name,c.customer_city,c.customer_state
# MAGIC from 
# MAGIC vwits.silver.sales s
# MAGIC inner join vwits.silver.customers c
# MAGIC on s.customer_id=c.customer_id)
# MAGIC
