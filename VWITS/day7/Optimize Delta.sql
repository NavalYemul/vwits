-- Databricks notebook source
-- MAGIC %md
-- MAGIC Delta lake
-- MAGIC
-- MAGIC optimize 
-- MAGIC z order
-- MAGIC
-- MAGIC liquid clusting
-- MAGIC

-- COMMAND ----------

optimize tablename
zorder by (order_id,date)

-- COMMAND ----------

select * from tbalnaem wher order_id=

-- COMMAND ----------

adversely effect
select * from tblname where pincode=

-- COMMAND ----------

--make changes and read bronze table
select * from sales


select * from bronze.sales


df=spark.read.csv("path")
